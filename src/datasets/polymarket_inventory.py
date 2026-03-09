from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.datasets.archive_inventory import ArchiveInventory, DatasetInventorySummary, build_archive_inventory
from src.datasets.polymarket_mapping import (
    CANONICAL_FIELD_MAPPINGS,
    CanonicalFieldMapping,
    DatasetSchemaSnapshot,
    build_dataset_schema_snapshot,
)


LOW_PROBABILITY_THRESHOLD = 0.10
HIGH_PROBABILITY_THRESHOLD = 0.90

INCLUSION_RULES = (
    "Include only Polymarket market snapshot rows with exactly two outcomes.",
    "Normalize outcomes strictly to YES and NO labels before building canonical contract fields.",
    "Require `clob_token_ids` to contain exactly two token ids aligned with the binary outcomes.",
    "Use the YES-side element of `outcome_prices` as the priced probability at `_fetched_at`.",
    "Use `1 - no_price` only when the YES-side value is not mappable and the NO-side value is valid.",
)

EXCLUSION_RULES = (
    "Exclude non-binary markets and rows with more than two outcomes.",
    "Exclude rows with ambiguous outcome labels that do not normalize cleanly to YES/NO.",
    "Exclude rows with missing token alignment in `clob_token_ids`.",
    "Exclude rows whose candidate prices cannot be normalized to the `[0, 1]` interval.",
    "Exclude unresolved markets or closed snapshots whose YES/NO prices do not collapse to `1/0` or `0/1`.",
)

PRICED_PROBABILITY_RULE = {
    "primary_source": "markets.outcome_prices[yes_index]",
    "fallback_source": "1 - markets.outcome_prices[no_index]",
    "normalization": "Probabilities must be in the inclusive `[0, 1]` interval.",
    "extreme_buckets": {
        "low_probability_lt": LOW_PROBABILITY_THRESHOLD,
        "high_probability_gt": HIGH_PROBABILITY_THRESHOLD,
    },
}

RESOLUTION_RULE = {
    "source": "markets.outcome_prices on terminal closed snapshots",
    "rule": "Use the terminal `closed=true` snapshot where YES/NO prices collapse to `1/0` or `0/1`.",
    "excluded_when": "The market is still open or the terminal prices are not unambiguous binary outcomes.",
}


@dataclass(frozen=True, slots=True)
class PolymarketInventoryReport:
    inventory: ArchiveInventory
    schema_snapshots: tuple[DatasetSchemaSnapshot, ...]
    canonical_field_mappings: tuple[CanonicalFieldMapping, ...]


def build_polymarket_inventory_report(raw_dir: Path) -> PolymarketInventoryReport:
    inventory = build_archive_inventory(raw_dir)
    schema_snapshots = []
    for dataset in ("markets", "trades", "legacy_trades"):
        summary = inventory.dataset_summary_by_name()[dataset]
        if summary.first_path is None:
            continue
        schema_snapshots.append(
            build_dataset_schema_snapshot(
                dataset=dataset,
                parquet_path=Path(inventory.raw_dir) / summary.first_path,
                base_dir=Path(inventory.raw_dir),
            )
        )

    return PolymarketInventoryReport(
        inventory=inventory,
        schema_snapshots=tuple(schema_snapshots),
        canonical_field_mappings=CANONICAL_FIELD_MAPPINGS,
    )


def render_inventory_manifest(
    report: PolymarketInventoryReport,
    *,
    raw_dir_display: str | None = None,
    polymarket_root_display: str | None = None,
) -> dict[str, object]:
    return {
        "venue": "polymarket",
        "scope": "milestone_1_dataset_inventory_and_schema_mapping",
        "raw_dir": raw_dir_display or report.inventory.raw_dir,
        "polymarket_root": polymarket_root_display or report.inventory.polymarket_root,
        "ignored_entries": list(report.inventory.ignored_entries),
        "dataset_groups": [summary.to_dict() for summary in report.inventory.dataset_summaries],
        "schema_snapshots": [snapshot.to_dict() for snapshot in report.schema_snapshots],
        "included_dataset": "markets",
        "deferred_datasets": ["trades", "legacy_trades"],
        "canonical_field_mappings": [mapping.to_dict() for mapping in report.canonical_field_mappings],
        "inclusion_rules": list(INCLUSION_RULES),
        "exclusion_rules": list(EXCLUSION_RULES),
        "priced_probability_rule": PRICED_PROBABILITY_RULE,
        "resolution_rule": RESOLUTION_RULE,
    }


def render_inventory_summary_markdown(
    report: PolymarketInventoryReport,
    *,
    raw_dir_display: str | None = None,
    polymarket_root_display: str | None = None,
) -> str:
    raw_dir_label = raw_dir_display or report.inventory.raw_dir
    polymarket_root_label = polymarket_root_display or report.inventory.polymarket_root
    lines = [
        "# Polymarket Milestone 1 Inventory and Schema Mapping",
        "",
        "## Scope",
        f"- Venue: `polymarket`",
        f"- Raw root: `{raw_dir_label}`",
        f"- Extracted root: `{polymarket_root_label}`",
        "- First-pass analysis source: `markets` snapshot parquet files",
        "- Deferred record types: `trades`, `legacy_trades`",
        "",
        "## Inventory",
        "| Dataset | Status | Files | Size | Partitions | Access Pattern | Decision |",
        "| --- | --- | ---: | ---: | --- | --- | --- |",
    ]

    for summary in report.inventory.dataset_summaries:
        partition_range = (
            f"{summary.partition_start_min}-{summary.partition_end_max}"
            if summary.partition_start_min is not None and summary.partition_end_max is not None
            else "n/a"
        )
        lines.append(
            "| "
            + " | ".join(
                [
                    summary.dataset,
                    summary.status,
                    str(summary.file_count),
                    _format_bytes(summary.total_size_bytes),
                    partition_range,
                    f"`{summary.glob_pattern}`",
                    summary.note,
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Ignored Noise",
        ]
    )
    if report.inventory.ignored_entries:
        lines.extend(f"- `{entry}`" for entry in report.inventory.ignored_entries)
    else:
        lines.append("- No ignored archive noise was detected.")

    lines.extend(
        [
            "",
            "## Schema Snapshots",
        ]
    )
    for snapshot in report.schema_snapshots:
        lines.extend(
            [
                f"### `{snapshot.dataset}`",
                f"- Sample file: `{snapshot.sample_path}`",
                f"- Missing required columns: {', '.join(f'`{column}`' for column in snapshot.missing_columns) if snapshot.missing_columns else 'none'}",
                "",
                "| Column | DuckDB Type |",
                "| --- | --- |",
            ]
        )
        for column in snapshot.columns:
            lines.append(f"| `{column.name}` | `{column.duckdb_type}` |")
        lines.append("")

    lines.extend(
        [
            "## Canonical Mapping",
            "| Canonical Field | Source Field | Note |",
            "| --- | --- | --- |",
        ]
    )
    for mapping in report.canonical_field_mappings:
        lines.append(f"| `{mapping.canonical_field}` | `{mapping.source_field}` | {mapping.note} |")

    lines.extend(
        [
            "",
            "## Inclusion Rules",
        ]
    )
    lines.extend(f"- {rule}" for rule in INCLUSION_RULES)
    lines.extend(
        [
            "",
            "## Exclusion Rules",
        ]
    )
    lines.extend(f"- {rule}" for rule in EXCLUSION_RULES)
    lines.extend(
        [
            "",
            "## Probability and Resolution Decisions",
            f"- Priced probability: use `{PRICED_PROBABILITY_RULE['primary_source']}` at `_fetched_at`.",
            f"- Fallback: `{PRICED_PROBABILITY_RULE['fallback_source']}`.",
            f"- Normalization: {PRICED_PROBABILITY_RULE['normalization']}",
            (
                "- Extreme buckets: "
                f"low `< {LOW_PROBABILITY_THRESHOLD:.2f}`, "
                f"high `> {HIGH_PROBABILITY_THRESHOLD:.2f}`."
            ),
            f"- Resolution: {RESOLUTION_RULE['rule']}",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"


def _format_bytes(value: int) -> str:
    if value >= 1024**3:
        return f"{value / 1024**3:.2f} GiB"
    if value >= 1024**2:
        return f"{value / 1024**2:.2f} MiB"
    if value >= 1024:
        return f"{value / 1024:.2f} KiB"
    return f"{value} B"
