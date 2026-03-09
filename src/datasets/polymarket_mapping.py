from __future__ import annotations

from ast import literal_eval
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import json
from pathlib import Path
from typing import Any

import duckdb


REQUIRED_COLUMNS_BY_DATASET = {
    "markets": (
        "id",
        "condition_id",
        "question",
        "slug",
        "outcomes",
        "outcome_prices",
        "clob_token_ids",
        "closed",
        "end_date",
        "_fetched_at",
    ),
    "trades": (
        "block_number",
        "transaction_hash",
        "log_index",
        "order_hash",
        "maker",
        "taker",
        "maker_asset_id",
        "taker_asset_id",
        "maker_amount",
        "taker_amount",
        "fee",
        "timestamp",
        "_fetched_at",
        "_contract",
    ),
    "legacy_trades": (
        "block_number",
        "transaction_hash",
        "log_index",
        "fpmm_address",
        "trader",
        "amount",
        "fee_amount",
        "outcome_index",
        "outcome_tokens",
        "is_buy",
        "timestamp",
        "_fetched_at",
    ),
}

STRICT_YES_LABELS = {"yes"}
STRICT_NO_LABELS = {"no"}


@dataclass(frozen=True, slots=True)
class SchemaColumn:
    name: str
    duckdb_type: str

    def to_dict(self) -> dict[str, str]:
        return {"name": self.name, "duckdb_type": self.duckdb_type}


@dataclass(frozen=True, slots=True)
class DatasetSchemaSnapshot:
    dataset: str
    sample_path: str
    columns: tuple[SchemaColumn, ...]
    required_columns: tuple[str, ...]
    missing_columns: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "dataset": self.dataset,
            "sample_path": self.sample_path,
            "columns": [column.to_dict() for column in self.columns],
            "required_columns": list(self.required_columns),
            "missing_columns": list(self.missing_columns),
        }


@dataclass(frozen=True, slots=True)
class CanonicalFieldMapping:
    canonical_field: str
    source_field: str
    note: str

    def to_dict(self) -> dict[str, str]:
        return {
            "canonical_field": self.canonical_field,
            "source_field": self.source_field,
            "note": self.note,
        }


@dataclass(frozen=True, slots=True)
class ProbabilitySelectionResult:
    included: bool
    exclusion_reason: str | None
    probability: Decimal | None
    probability_source: str | None
    yes_index: int | None
    no_index: int | None
    yes_label: str | None
    no_label: str | None
    yes_token_id: str | None
    no_token_id: str | None


@dataclass(frozen=True, slots=True)
class ResolutionSelectionResult:
    resolved_outcome: str | None
    reason: str


CANONICAL_FIELD_MAPPINGS = (
    CanonicalFieldMapping(
        canonical_field="market_id",
        source_field="markets.id",
        note="Primary market snapshot identifier for Polymarket.",
    ),
    CanonicalFieldMapping(
        canonical_field="condition_id",
        source_field="markets.condition_id",
        note="Cross-record market condition identifier for later joins.",
    ),
    CanonicalFieldMapping(
        canonical_field="question",
        source_field="markets.question",
        note="Human-readable market prompt.",
    ),
    CanonicalFieldMapping(
        canonical_field="slug",
        source_field="markets.slug",
        note="Stable URL-like market slug.",
    ),
    CanonicalFieldMapping(
        canonical_field="observation_time_utc",
        source_field="markets._fetched_at",
        note="Snapshot collection time for priced-probability observations.",
    ),
    CanonicalFieldMapping(
        canonical_field="market_end_time_utc",
        source_field="markets.end_date",
        note="Scheduled market end timestamp.",
    ),
    CanonicalFieldMapping(
        canonical_field="yes_outcome_label",
        source_field="markets.outcomes[yes_index]",
        note="YES label after strict binary label normalization.",
    ),
    CanonicalFieldMapping(
        canonical_field="no_outcome_label",
        source_field="markets.outcomes[no_index]",
        note="NO label after strict binary label normalization.",
    ),
    CanonicalFieldMapping(
        canonical_field="yes_contract_id",
        source_field="markets.clob_token_ids[yes_index]",
        note="CLOB token aligned to the YES outcome.",
    ),
    CanonicalFieldMapping(
        canonical_field="no_contract_id",
        source_field="markets.clob_token_ids[no_index]",
        note="CLOB token aligned to the NO outcome.",
    ),
)


def inspect_parquet_schema(parquet_path: Path) -> tuple[SchemaColumn, ...]:
    query_path = parquet_path.as_posix().replace("'", "''")
    with duckdb.connect(database=":memory:") as connection:
        rows = connection.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{query_path}')"
        ).fetchall()
    return tuple(SchemaColumn(name=row[0], duckdb_type=row[1]) for row in rows)


def build_dataset_schema_snapshot(
    dataset: str,
    parquet_path: Path,
    *,
    base_dir: Path | None = None,
) -> DatasetSchemaSnapshot:
    columns = inspect_parquet_schema(parquet_path)
    required_columns = REQUIRED_COLUMNS_BY_DATASET[dataset]
    observed_names = {column.name for column in columns}
    sample_path = parquet_path.relative_to(base_dir).as_posix() if base_dir else parquet_path.as_posix()
    return DatasetSchemaSnapshot(
        dataset=dataset,
        sample_path=sample_path,
        columns=columns,
        required_columns=required_columns,
        missing_columns=tuple(column for column in required_columns if column not in observed_names),
    )


def select_market_priced_probability(
    *,
    outcomes: Any,
    outcome_prices: Any,
    clob_token_ids: Any,
) -> ProbabilitySelectionResult:
    labels = _parse_sequence(outcomes)
    prices = _parse_decimal_sequence(outcome_prices)
    token_ids = _parse_sequence(clob_token_ids)

    if len(labels) != 2:
        return _excluded_probability("non_binary_outcomes")
    if len(prices) != 2:
        return _excluded_probability("invalid_outcome_prices")
    if len(token_ids) != 2:
        return _excluded_probability("missing_token_alignment")

    normalized_labels = tuple(_normalize_binary_label(label) for label in labels)
    if any(label is None for label in normalized_labels) or set(normalized_labels) != {"NO", "YES"}:
        return _excluded_probability("ambiguous_outcome_labels")

    yes_index = normalized_labels.index("YES")
    no_index = normalized_labels.index("NO")
    yes_price = prices[yes_index]
    no_price = prices[no_index]

    if yes_price is not None and _is_probability(yes_price):
        return ProbabilitySelectionResult(
            included=True,
            exclusion_reason=None,
            probability=yes_price,
            probability_source="yes_outcome_price",
            yes_index=yes_index,
            no_index=no_index,
            yes_label=str(labels[yes_index]),
            no_label=str(labels[no_index]),
            yes_token_id=str(token_ids[yes_index]),
            no_token_id=str(token_ids[no_index]),
        )

    if no_price is not None and _is_probability(no_price):
        return ProbabilitySelectionResult(
            included=True,
            exclusion_reason=None,
            probability=Decimal("1") - no_price,
            probability_source="1_minus_no_outcome_price",
            yes_index=yes_index,
            no_index=no_index,
            yes_label=str(labels[yes_index]),
            no_label=str(labels[no_index]),
            yes_token_id=str(token_ids[yes_index]),
            no_token_id=str(token_ids[no_index]),
        )

    return _excluded_probability("probability_out_of_range")


def resolve_closed_market_outcome(
    *,
    outcomes: Any,
    outcome_prices: Any,
    clob_token_ids: Any,
    closed: bool | None,
) -> ResolutionSelectionResult:
    pricing_result = select_market_priced_probability(
        outcomes=outcomes,
        outcome_prices=outcome_prices,
        clob_token_ids=clob_token_ids,
    )
    if not pricing_result.included:
        return ResolutionSelectionResult(
            resolved_outcome=None,
            reason=pricing_result.exclusion_reason or "unmappable_market_snapshot",
        )

    if closed is not True:
        return ResolutionSelectionResult(resolved_outcome=None, reason="market_not_closed")

    prices = _parse_decimal_sequence(outcome_prices)
    assert pricing_result.yes_index is not None
    assert pricing_result.no_index is not None
    yes_price = prices[pricing_result.yes_index]
    no_price = prices[pricing_result.no_index]

    if yes_price == Decimal("1") and no_price == Decimal("0"):
        return ResolutionSelectionResult(resolved_outcome="YES", reason="closed_market_terminal_prices")
    if yes_price == Decimal("0") and no_price == Decimal("1"):
        return ResolutionSelectionResult(resolved_outcome="NO", reason="closed_market_terminal_prices")
    return ResolutionSelectionResult(
        resolved_outcome=None,
        reason="terminal_prices_not_collapsed",
    )


def _excluded_probability(reason: str) -> ProbabilitySelectionResult:
    return ProbabilitySelectionResult(
        included=False,
        exclusion_reason=reason,
        probability=None,
        probability_source=None,
        yes_index=None,
        no_index=None,
        yes_label=None,
        no_label=None,
        yes_token_id=None,
        no_token_id=None,
    )


def _parse_sequence(value: Any) -> tuple[Any, ...]:
    if value is None:
        return ()
    if isinstance(value, tuple):
        return value
    if isinstance(value, list):
        return tuple(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return ()
        for parser in (json.loads, literal_eval):
            try:
                parsed = parser(stripped)
            except (ValueError, SyntaxError, json.JSONDecodeError):
                continue
            if isinstance(parsed, (list, tuple)):
                return tuple(parsed)
        return (stripped,)
    return (value,)


def _parse_decimal_sequence(value: Any) -> tuple[Decimal | None, ...]:
    return tuple(_parse_decimal_item(item) for item in _parse_sequence(value))


def _parse_decimal_item(value: Any) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, bool):
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _normalize_binary_label(value: Any) -> str | None:
    normalized = str(value).strip().strip('"').strip("'").casefold()
    if normalized in STRICT_YES_LABELS:
        return "YES"
    if normalized in STRICT_NO_LABELS:
        return "NO"
    return None


def _is_probability(value: Decimal) -> bool:
    return Decimal("0") <= value <= Decimal("1")
