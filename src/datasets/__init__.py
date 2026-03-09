"""Dataset discovery and normalization for the extreme-probability study."""

from src.datasets.archive_inventory import (
    ArchiveInventory,
    DatasetInventorySummary,
    InventoryFile,
    PartitionRange,
    build_archive_inventory,
    classify_polymarket_path,
    parse_partition_range,
)
from src.datasets.polymarket_inventory import (
    PolymarketInventoryReport,
    build_polymarket_inventory_report,
    render_inventory_manifest,
    render_inventory_summary_markdown,
)
from src.datasets.polymarket_mapping import (
    CanonicalFieldMapping,
    DatasetSchemaSnapshot,
    ProbabilitySelectionResult,
    ResolutionSelectionResult,
    SchemaColumn,
    build_dataset_schema_snapshot,
    inspect_parquet_schema,
    resolve_closed_market_outcome,
    select_market_priced_probability,
)

__all__ = [
    "ArchiveInventory",
    "CanonicalFieldMapping",
    "DatasetInventorySummary",
    "DatasetSchemaSnapshot",
    "InventoryFile",
    "PartitionRange",
    "PolymarketInventoryReport",
    "ProbabilitySelectionResult",
    "ResolutionSelectionResult",
    "SchemaColumn",
    "build_archive_inventory",
    "build_dataset_schema_snapshot",
    "build_polymarket_inventory_report",
    "classify_polymarket_path",
    "inspect_parquet_schema",
    "parse_partition_range",
    "render_inventory_manifest",
    "render_inventory_summary_markdown",
    "resolve_closed_market_outcome",
    "select_market_priced_probability",
]
