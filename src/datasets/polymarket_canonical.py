from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
import re

import duckdb

from src.datasets.archive_inventory import ArchiveInventory, InventoryFile, build_archive_inventory
from src.datasets.polymarket_mapping import resolve_closed_market_outcome, select_market_priced_probability


VENUE = "polymarket"
SOURCE_DATASET = "markets"
DEFAULT_CANONICAL_WAREHOUSE_PATH = Path("data/warehouse/extreme_probability.duckdb")
DECIMAL_SQL_TYPE = "DECIMAL(38, 18)"
LOW_PROBABILITY_THRESHOLD = Decimal("0.10")
HIGH_PROBABILITY_THRESHOLD = Decimal("0.90")
_TRAILING_HOUR_OFFSET_PATTERN = re.compile(r"([+-]\d{2})$")


@dataclass(frozen=True, slots=True)
class CanonicalTableCounts:
    archive_inventory: int
    market_catalog: int
    contract_catalog: int
    tick_observations: int
    resolution_outcomes: int
    threshold_entry_events: int

    def to_dict(self) -> dict[str, int]:
        return {
            "archive_inventory": self.archive_inventory,
            "market_catalog": self.market_catalog,
            "contract_catalog": self.contract_catalog,
            "tick_observations": self.tick_observations,
            "resolution_outcomes": self.resolution_outcomes,
            "threshold_entry_events": self.threshold_entry_events,
        }


@dataclass(frozen=True, slots=True)
class PolymarketCanonicalBuildResult:
    raw_dir: str
    warehouse_path: str
    counts: CanonicalTableCounts
    excluded_unmappable_observation_count: int
    unresolved_market_count: int
    ambiguous_resolution_market_count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "raw_dir": self.raw_dir,
            "warehouse_path": self.warehouse_path,
            "counts": self.counts.to_dict(),
            "excluded_unmappable_observation_count": self.excluded_unmappable_observation_count,
            "unresolved_market_count": self.unresolved_market_count,
            "ambiguous_resolution_market_count": self.ambiguous_resolution_market_count,
        }


@dataclass(frozen=True, slots=True)
class _ResolutionCandidate:
    market_id: str
    resolved_outcome: str
    resolution_time_utc: datetime
    source_file: str


def build_polymarket_canonical_dataset(
    raw_dir: Path,
    *,
    warehouse_path: Path = DEFAULT_CANONICAL_WAREHOUSE_PATH,
) -> PolymarketCanonicalBuildResult:
    inventory = build_archive_inventory(raw_dir)
    warehouse_path = Path(warehouse_path)
    warehouse_path.parent.mkdir(parents=True, exist_ok=True)

    excluded_unmappable_observation_count = 0
    priced_market_ids: set[str] = set()
    resolution_candidates_by_market: dict[str, list[_ResolutionCandidate]] = {}

    with duckdb.connect(str(warehouse_path)) as connection:
        for file in _iter_market_inventory_files(inventory):
            for row in _read_market_rows(connection, base_dir=Path(inventory.raw_dir), file=file):
                selection = select_market_priced_probability(
                    outcomes=row["outcomes"],
                    outcome_prices=row["outcome_prices"],
                    clob_token_ids=row["clob_token_ids"],
                )
                if not selection.included or selection.probability is None:
                    excluded_unmappable_observation_count += 1
                    continue

                market_id = row["market_id"]
                priced_market_ids.add(market_id)

                resolution = resolve_closed_market_outcome(
                    outcomes=row["outcomes"],
                    outcome_prices=row["outcome_prices"],
                    clob_token_ids=row["clob_token_ids"],
                    closed=row["closed"],
                )
                if resolution.resolved_outcome is None:
                    continue

                resolution_candidates_by_market.setdefault(market_id, []).append(
                    _ResolutionCandidate(
                        market_id=market_id,
                        resolved_outcome=resolution.resolved_outcome,
                        resolution_time_utc=row["observation_time_utc"],
                        source_file=file.relative_path,
                    )
                )

        resolved_markets, ambiguous_market_ids = _resolve_market_outcomes(
            resolution_candidates_by_market
        )
        unresolved_market_count = len(priced_market_ids - set(resolved_markets) - ambiguous_market_ids)
        ambiguous_resolution_market_count = len(ambiguous_market_ids)

        archive_inventory_rows = [
            (
                VENUE,
                file.relative_path,
                file.dataset,
                file.file_format,
                file.size_bytes,
                file.partition.start if file.partition else None,
                file.partition.end if file.partition else None,
            )
            for file in inventory.files
        ]

        market_states: dict[str, dict[str, object]] = {}
        contract_states: dict[tuple[str, str], dict[str, object]] = {}
        tick_rows: list[tuple[object, ...]] = []

        for file in _iter_market_inventory_files(inventory):
            for row in _read_market_rows(connection, base_dir=Path(inventory.raw_dir), file=file):
                market_id = row["market_id"]
                if market_id not in resolved_markets:
                    continue

                selection = select_market_priced_probability(
                    outcomes=row["outcomes"],
                    outcome_prices=row["outcome_prices"],
                    clob_token_ids=row["clob_token_ids"],
                )
                if not selection.included or selection.probability is None:
                    continue

                assert selection.yes_token_id is not None
                assert selection.no_token_id is not None
                assert selection.yes_label is not None
                assert selection.no_label is not None
                assert selection.probability_source is not None

                _upsert_market_state(
                    market_states=market_states,
                    market_id=market_id,
                    condition_id=row["condition_id"],
                    question=row["question"],
                    slug=row["slug"],
                    market_end_time_utc=row["market_end_time_utc"],
                    observation_time_utc=row["observation_time_utc"],
                    market_resolved_outcome=resolved_markets[market_id].resolved_outcome,
                )
                _upsert_contract_state(
                    contract_states=contract_states,
                    market_id=market_id,
                    condition_id=row["condition_id"],
                    contract_id=selection.yes_token_id,
                    contract_side="YES",
                    outcome_label=selection.yes_label,
                    paired_contract_id=selection.no_token_id,
                    observation_time_utc=row["observation_time_utc"],
                )
                _upsert_contract_state(
                    contract_states=contract_states,
                    market_id=market_id,
                    condition_id=row["condition_id"],
                    contract_id=selection.no_token_id,
                    contract_side="NO",
                    outcome_label=selection.no_label,
                    paired_contract_id=selection.yes_token_id,
                    observation_time_utc=row["observation_time_utc"],
                )
                tick_rows.append(
                    (
                        VENUE,
                        market_id,
                        row["condition_id"],
                        selection.yes_token_id,
                        "YES",
                        row["observation_time_utc"],
                        row["market_end_time_utc"],
                        selection.probability,
                        selection.probability_source,
                        SOURCE_DATASET,
                        file.relative_path,
                    )
                )

        market_rows = _build_market_rows(market_states)
        contract_rows = _build_contract_rows(contract_states)
        resolution_rows = _build_resolution_rows(contract_rows, resolved_markets)
        threshold_rows = _build_threshold_entry_rows(tick_rows, resolved_markets)

        connection.execute("BEGIN")
        try:
            _replace_table(connection, "archive_inventory", _create_archive_inventory_table_sql())
            _replace_table(connection, "market_catalog", _create_market_catalog_table_sql())
            _replace_table(connection, "contract_catalog", _create_contract_catalog_table_sql())
            _replace_table(connection, "tick_observations", _create_tick_observations_table_sql())
            _replace_table(connection, "resolution_outcomes", _create_resolution_outcomes_table_sql())
            _replace_table(connection, "threshold_entry_events", _create_threshold_entry_events_table_sql())

            connection.executemany(
                """
                INSERT INTO archive_inventory (
                    venue,
                    relative_path,
                    dataset,
                    file_format,
                    size_bytes,
                    partition_start,
                    partition_end
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                archive_inventory_rows,
            )
            connection.executemany(
                """
                INSERT INTO market_catalog (
                    venue,
                    market_id,
                    condition_id,
                    question,
                    slug,
                    market_end_time_utc,
                    first_observation_time_utc,
                    last_observation_time_utc,
                    tick_observation_count,
                    market_resolved_outcome
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                market_rows,
            )
            connection.executemany(
                """
                INSERT INTO contract_catalog (
                    venue,
                    market_id,
                    condition_id,
                    contract_id,
                    contract_side,
                    outcome_label,
                    paired_contract_id,
                    first_observation_time_utc,
                    last_observation_time_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                contract_rows,
            )
            connection.executemany(
                """
                INSERT INTO tick_observations (
                    venue,
                    market_id,
                    condition_id,
                    contract_id,
                    contract_side,
                    observation_time_utc,
                    market_end_time_utc,
                    probability,
                    price_source,
                    source_dataset,
                    source_file
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                sorted(tick_rows, key=lambda row: (row[3], row[5], row[10])),
            )
            connection.executemany(
                """
                INSERT INTO resolution_outcomes (
                    venue,
                    market_id,
                    contract_id,
                    contract_side,
                    resolved_outcome,
                    market_resolved_outcome,
                    resolution_time_utc,
                    source
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                resolution_rows,
            )
            connection.executemany(
                """
                INSERT INTO threshold_entry_events (
                    venue,
                    market_id,
                    contract_id,
                    contract_side,
                    threshold_bucket,
                    entry_time_utc,
                    probability,
                    price_source,
                    event_index,
                    resolved_outcome,
                    market_resolved_outcome
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                threshold_rows,
            )
        except Exception:
            connection.execute("ROLLBACK")
            raise
        else:
            connection.execute("COMMIT")

    counts = CanonicalTableCounts(
        archive_inventory=len(archive_inventory_rows),
        market_catalog=len(market_rows),
        contract_catalog=len(contract_rows),
        tick_observations=len(tick_rows),
        resolution_outcomes=len(resolution_rows),
        threshold_entry_events=len(threshold_rows),
    )
    return PolymarketCanonicalBuildResult(
        raw_dir=Path(inventory.raw_dir).as_posix(),
        warehouse_path=warehouse_path.as_posix(),
        counts=counts,
        excluded_unmappable_observation_count=excluded_unmappable_observation_count,
        unresolved_market_count=unresolved_market_count,
        ambiguous_resolution_market_count=ambiguous_resolution_market_count,
    )


def _iter_market_inventory_files(inventory: ArchiveInventory) -> tuple[InventoryFile, ...]:
    return tuple(file for file in inventory.files if file.dataset == SOURCE_DATASET)


def _read_market_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    base_dir: Path,
    file: InventoryFile,
) -> list[dict[str, object]]:
    parquet_path = (base_dir / file.relative_path).as_posix().replace("'", "''")
    rows = connection.execute(
        f"""
        SELECT
            id,
            condition_id,
            question,
            slug,
            outcomes,
            outcome_prices,
            clob_token_ids,
            closed,
            CAST(end_date AS VARCHAR) AS market_end_time_utc,
            CAST(_fetched_at AS VARCHAR) AS observation_time_utc
        FROM read_parquet('{parquet_path}')
        ORDER BY CAST(_fetched_at AS VARCHAR), id, slug
        """
    ).fetchall()
    return [
        {
            "market_id": row[0],
            "condition_id": row[1],
            "question": row[2],
            "slug": row[3],
            "outcomes": row[4],
            "outcome_prices": row[5],
            "clob_token_ids": row[6],
            "closed": row[7],
            "market_end_time_utc": _parse_optional_timestamp(row[8]),
            "observation_time_utc": _parse_timestamp(row[9]),
        }
        for row in rows
    ]


def _resolve_market_outcomes(
    resolution_candidates_by_market: dict[str, list[_ResolutionCandidate]]
) -> tuple[dict[str, _ResolutionCandidate], set[str]]:
    resolved_markets: dict[str, _ResolutionCandidate] = {}
    ambiguous_market_ids: set[str] = set()

    for market_id, candidates in resolution_candidates_by_market.items():
        outcomes = {candidate.resolved_outcome for candidate in candidates}
        if len(outcomes) != 1:
            ambiguous_market_ids.add(market_id)
            continue

        resolved_markets[market_id] = max(
            candidates,
            key=lambda candidate: (
                candidate.resolution_time_utc,
                candidate.source_file,
            ),
        )

    return resolved_markets, ambiguous_market_ids


def _upsert_market_state(
    *,
    market_states: dict[str, dict[str, object]],
    market_id: str,
    condition_id: str | None,
    question: str | None,
    slug: str | None,
    market_end_time_utc: datetime | None,
    observation_time_utc: datetime,
    market_resolved_outcome: str,
) -> None:
    state = market_states.get(market_id)
    if state is None:
        market_states[market_id] = {
            "condition_id": condition_id,
            "question": question,
            "slug": slug,
            "market_end_time_utc": market_end_time_utc,
            "first_observation_time_utc": observation_time_utc,
            "last_observation_time_utc": observation_time_utc,
            "tick_observation_count": 1,
            "market_resolved_outcome": market_resolved_outcome,
        }
        return

    state["tick_observation_count"] = int(state["tick_observation_count"]) + 1
    if observation_time_utc < state["first_observation_time_utc"]:
        state["first_observation_time_utc"] = observation_time_utc
    if observation_time_utc >= state["last_observation_time_utc"]:
        state["condition_id"] = condition_id
        state["question"] = question
        state["slug"] = slug
        state["market_end_time_utc"] = market_end_time_utc
        state["last_observation_time_utc"] = observation_time_utc


def _upsert_contract_state(
    *,
    contract_states: dict[tuple[str, str], dict[str, object]],
    market_id: str,
    condition_id: str | None,
    contract_id: str,
    contract_side: str,
    outcome_label: str,
    paired_contract_id: str,
    observation_time_utc: datetime,
) -> None:
    key = (market_id, contract_id)
    state = contract_states.get(key)
    if state is None:
        contract_states[key] = {
            "condition_id": condition_id,
            "contract_side": contract_side,
            "outcome_label": outcome_label,
            "paired_contract_id": paired_contract_id,
            "first_observation_time_utc": observation_time_utc,
            "last_observation_time_utc": observation_time_utc,
        }
        return

    if observation_time_utc < state["first_observation_time_utc"]:
        state["first_observation_time_utc"] = observation_time_utc
    if observation_time_utc >= state["last_observation_time_utc"]:
        state["condition_id"] = condition_id
        state["contract_side"] = contract_side
        state["outcome_label"] = outcome_label
        state["paired_contract_id"] = paired_contract_id
        state["last_observation_time_utc"] = observation_time_utc


def _build_market_rows(
    market_states: dict[str, dict[str, object]]
) -> list[tuple[object, ...]]:
    rows = []
    for market_id in sorted(market_states):
        state = market_states[market_id]
        rows.append(
            (
                VENUE,
                market_id,
                state["condition_id"],
                state["question"],
                state["slug"],
                state["market_end_time_utc"],
                state["first_observation_time_utc"],
                state["last_observation_time_utc"],
                state["tick_observation_count"],
                state["market_resolved_outcome"],
            )
        )
    return rows


def _build_contract_rows(
    contract_states: dict[tuple[str, str], dict[str, object]]
) -> list[tuple[object, ...]]:
    rows = []
    for market_id, contract_id in sorted(contract_states):
        state = contract_states[(market_id, contract_id)]
        rows.append(
            (
                VENUE,
                market_id,
                state["condition_id"],
                contract_id,
                state["contract_side"],
                state["outcome_label"],
                state["paired_contract_id"],
                state["first_observation_time_utc"],
                state["last_observation_time_utc"],
            )
        )
    return rows


def _build_resolution_rows(
    contract_rows: list[tuple[object, ...]],
    resolved_markets: dict[str, _ResolutionCandidate],
) -> list[tuple[object, ...]]:
    rows = []
    for row in contract_rows:
        market_resolution = resolved_markets[row[1]]
        contract_side = row[4]
        rows.append(
            (
                row[0],
                row[1],
                row[3],
                contract_side,
                _contract_resolved_outcome(
                    contract_side=contract_side,
                    market_resolved_outcome=market_resolution.resolved_outcome,
                ),
                market_resolution.resolved_outcome,
                market_resolution.resolution_time_utc,
                "markets_terminal_price",
            )
        )
    return rows


def _build_threshold_entry_rows(
    tick_rows: list[tuple[object, ...]],
    resolved_markets: dict[str, _ResolutionCandidate],
) -> list[tuple[object, ...]]:
    rows = []
    previous_bucket_by_contract: dict[str, str | None] = {}
    event_index_by_contract: dict[str, int] = {}

    for row in sorted(tick_rows, key=lambda item: (item[3], item[5], item[10])):
        bucket = _classify_probability(row[7])
        contract_id = row[3]
        if bucket is None:
            previous_bucket_by_contract[contract_id] = None
            continue

        previous_bucket = previous_bucket_by_contract.get(contract_id)
        if previous_bucket != bucket:
            event_index = event_index_by_contract.get(contract_id, 0) + 1
            event_index_by_contract[contract_id] = event_index
            market_resolution = resolved_markets[row[1]]
            rows.append(
                (
                    row[0],
                    row[1],
                    contract_id,
                    row[4],
                    bucket,
                    row[5],
                    row[7],
                    row[8],
                    event_index,
                    _contract_resolved_outcome(
                        contract_side=row[4],
                        market_resolved_outcome=market_resolution.resolved_outcome,
                    ),
                    market_resolution.resolved_outcome,
                )
            )
        previous_bucket_by_contract[contract_id] = bucket

    return rows


def _classify_probability(value: object) -> str | None:
    probability = Decimal(str(value))
    if probability < LOW_PROBABILITY_THRESHOLD:
        return "low_probability"
    if probability > HIGH_PROBABILITY_THRESHOLD:
        return "high_probability"
    return None


def _contract_resolved_outcome(*, contract_side: object, market_resolved_outcome: str) -> str:
    if contract_side == "YES":
        return market_resolved_outcome
    return "NO" if market_resolved_outcome == "YES" else "YES"


def _parse_timestamp(value: object) -> datetime:
    if value is None:
        raise ValueError("Expected a timestamp string, received None.")

    normalized = str(value).strip()
    if not normalized:
        raise ValueError("Expected a timestamp string, received an empty value.")
    normalized = normalized.replace("Z", "+00:00")
    if _TRAILING_HOUR_OFFSET_PATTERN.search(normalized):
        normalized = f"{normalized}:00"
    normalized = normalized.replace(" ", "T", 1)

    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone(UTC).replace(tzinfo=None)


def _parse_optional_timestamp(value: object) -> datetime | None:
    if value is None:
        return None
    if str(value).strip() == "":
        return None
    return _parse_timestamp(value)


def _replace_table(connection: duckdb.DuckDBPyConnection, name: str, create_sql: str) -> None:
    connection.execute(f"DROP TABLE IF EXISTS {name}")
    connection.execute(create_sql)


def _create_archive_inventory_table_sql() -> str:
    return """
        CREATE TABLE archive_inventory (
            venue VARCHAR NOT NULL,
            relative_path VARCHAR NOT NULL,
            dataset VARCHAR NOT NULL,
            file_format VARCHAR NOT NULL,
            size_bytes BIGINT NOT NULL,
            partition_start BIGINT,
            partition_end BIGINT
        )
    """


def _create_market_catalog_table_sql() -> str:
    return """
        CREATE TABLE market_catalog (
            venue VARCHAR NOT NULL,
            market_id VARCHAR NOT NULL,
            condition_id VARCHAR,
            question VARCHAR,
            slug VARCHAR,
            market_end_time_utc TIMESTAMP,
            first_observation_time_utc TIMESTAMP NOT NULL,
            last_observation_time_utc TIMESTAMP NOT NULL,
            tick_observation_count BIGINT NOT NULL,
            market_resolved_outcome VARCHAR NOT NULL
        )
    """


def _create_contract_catalog_table_sql() -> str:
    return """
        CREATE TABLE contract_catalog (
            venue VARCHAR NOT NULL,
            market_id VARCHAR NOT NULL,
            condition_id VARCHAR,
            contract_id VARCHAR NOT NULL,
            contract_side VARCHAR NOT NULL,
            outcome_label VARCHAR NOT NULL,
            paired_contract_id VARCHAR NOT NULL,
            first_observation_time_utc TIMESTAMP NOT NULL,
            last_observation_time_utc TIMESTAMP NOT NULL
        )
    """


def _create_tick_observations_table_sql() -> str:
    return f"""
        CREATE TABLE tick_observations (
            venue VARCHAR NOT NULL,
            market_id VARCHAR NOT NULL,
            condition_id VARCHAR,
            contract_id VARCHAR NOT NULL,
            contract_side VARCHAR NOT NULL,
            observation_time_utc TIMESTAMP NOT NULL,
            market_end_time_utc TIMESTAMP,
            probability {DECIMAL_SQL_TYPE} NOT NULL,
            price_source VARCHAR NOT NULL,
            source_dataset VARCHAR NOT NULL,
            source_file VARCHAR NOT NULL
        )
    """


def _create_resolution_outcomes_table_sql() -> str:
    return """
        CREATE TABLE resolution_outcomes (
            venue VARCHAR NOT NULL,
            market_id VARCHAR NOT NULL,
            contract_id VARCHAR NOT NULL,
            contract_side VARCHAR NOT NULL,
            resolved_outcome VARCHAR NOT NULL,
            market_resolved_outcome VARCHAR NOT NULL,
            resolution_time_utc TIMESTAMP NOT NULL,
            source VARCHAR NOT NULL
        )
    """


def _create_threshold_entry_events_table_sql() -> str:
    return f"""
        CREATE TABLE threshold_entry_events (
            venue VARCHAR NOT NULL,
            market_id VARCHAR NOT NULL,
            contract_id VARCHAR NOT NULL,
            contract_side VARCHAR NOT NULL,
            threshold_bucket VARCHAR NOT NULL,
            entry_time_utc TIMESTAMP NOT NULL,
            probability {DECIMAL_SQL_TYPE} NOT NULL,
            price_source VARCHAR NOT NULL,
            event_index BIGINT NOT NULL,
            resolved_outcome VARCHAR NOT NULL,
            market_resolved_outcome VARCHAR NOT NULL
        )
    """
