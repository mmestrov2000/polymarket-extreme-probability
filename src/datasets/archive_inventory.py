from __future__ import annotations

from src.compat import dataclass
from pathlib import Path, PurePosixPath
import re


PARTITION_FILENAME_PATTERN = re.compile(r"^[^_]+_(\d+)_(\d+)\.parquet$")
POLYMARKET_DATASET_ORDER = ("markets", "trades", "legacy_trades", "blocks", "lookup_json")
IGNORED_ENTRY_NAMES = {".DS_Store"}

DATASET_DECISIONS = {
    "markets": (
        "included",
        "Use market snapshots as the v1 source of priced-probability observations.",
    ),
    "trades": (
        "deferred",
        "Inventory only for Milestone 1; trade price derivation and contract-side mapping are deferred.",
    ),
    "legacy_trades": (
        "deferred",
        "Inventory only for Milestone 1; legacy FPMM trade normalization is deferred.",
    ),
    "blocks": (
        "excluded",
        "Ignore for the first-pass study because no analysis-ready snapshot records are present.",
    ),
    "lookup_json": (
        "auxiliary",
        "Keep as a collateral lookup reference but do not use it as the priced-probability source.",
    ),
}


@dataclass(frozen=True, slots=True)
class PartitionRange:
    start: int
    end: int

    def to_dict(self) -> dict[str, int]:
        return {"start": self.start, "end": self.end}


@dataclass(frozen=True, slots=True)
class InventoryFile:
    relative_path: str
    dataset: str
    file_format: str
    size_bytes: int
    partition: PartitionRange | None

    def to_dict(self) -> dict[str, object]:
        return {
            "relative_path": self.relative_path,
            "dataset": self.dataset,
            "file_format": self.file_format,
            "size_bytes": self.size_bytes,
            "partition": self.partition.to_dict() if self.partition else None,
        }


@dataclass(frozen=True, slots=True)
class DatasetInventorySummary:
    dataset: str
    status: str
    note: str
    file_count: int
    total_size_bytes: int
    glob_pattern: str
    first_path: str | None
    last_path: str | None
    representative_paths: tuple[str, ...]
    partition_start_min: int | None
    partition_end_max: int | None

    def to_dict(self) -> dict[str, object]:
        return {
            "dataset": self.dataset,
            "status": self.status,
            "note": self.note,
            "file_count": self.file_count,
            "total_size_bytes": self.total_size_bytes,
            "glob_pattern": self.glob_pattern,
            "first_path": self.first_path,
            "last_path": self.last_path,
            "representative_paths": list(self.representative_paths),
            "partition_start_min": self.partition_start_min,
            "partition_end_max": self.partition_end_max,
        }


@dataclass(frozen=True, slots=True)
class ArchiveInventory:
    raw_dir: str
    polymarket_root: str
    ignored_entries: tuple[str, ...]
    files: tuple[InventoryFile, ...]
    dataset_summaries: tuple[DatasetInventorySummary, ...]

    def dataset_summary_by_name(self) -> dict[str, DatasetInventorySummary]:
        return {summary.dataset: summary for summary in self.dataset_summaries}


def parse_partition_range(filename: str) -> PartitionRange | None:
    match = PARTITION_FILENAME_PATTERN.match(filename)
    if match is None:
        return None
    return PartitionRange(start=int(match.group(1)), end=int(match.group(2)))


def is_ignored_path(path: Path | PurePosixPath) -> bool:
    return path.name in IGNORED_ENTRY_NAMES or path.name.startswith("._")


def resolve_polymarket_root(raw_dir: Path) -> Path:
    candidates = (
        raw_dir / "data" / "polymarket",
        raw_dir / "polymarket",
    )
    for candidate in candidates:
        if candidate.exists() and candidate.is_dir():
            return candidate
    raise FileNotFoundError(f"Could not locate an extracted Polymarket archive under {raw_dir}.")


def classify_polymarket_path(relative_path: PurePosixPath) -> str:
    if not relative_path.parts:
        raise ValueError("Expected a path relative to the Polymarket root.")

    top_level = relative_path.parts[0]
    if top_level in {"markets", "trades", "legacy_trades", "blocks"}:
        return top_level
    if relative_path.name == "fpmm_collateral_lookup.json":
        return "lookup_json"
    raise ValueError(f"Unsupported Polymarket path: {relative_path}")


def build_archive_inventory(raw_dir: Path) -> ArchiveInventory:
    raw_dir = raw_dir if raw_dir.is_absolute() else Path(raw_dir.as_posix())
    polymarket_root = resolve_polymarket_root(raw_dir)
    discovered_files = _discover_polymarket_files(raw_dir=raw_dir, polymarket_root=polymarket_root)
    ignored_entries = _discover_ignored_entries(raw_dir)
    dataset_summaries = _summarize_datasets(
        raw_dir=raw_dir,
        polymarket_root=polymarket_root,
        discovered_files=discovered_files,
    )
    return ArchiveInventory(
        raw_dir=raw_dir.as_posix(),
        polymarket_root=polymarket_root.as_posix(),
        ignored_entries=ignored_entries,
        files=tuple(discovered_files),
        dataset_summaries=dataset_summaries,
    )


def _discover_polymarket_files(*, raw_dir: Path, polymarket_root: Path) -> list[InventoryFile]:
    files: list[InventoryFile] = []
    for path in sorted(polymarket_root.rglob("*")):
        if not path.is_file() or is_ignored_path(path):
            continue

        dataset = classify_polymarket_path(PurePosixPath(path.relative_to(polymarket_root).as_posix()))
        files.append(
            InventoryFile(
                relative_path=path.relative_to(raw_dir).as_posix(),
                dataset=dataset,
                file_format=path.suffix.removeprefix(".") or "<noext>",
                size_bytes=path.stat().st_size,
                partition=parse_partition_range(path.name),
            )
        )
    return files


def _discover_ignored_entries(raw_dir: Path) -> tuple[str, ...]:
    ignored_entries = []
    for path in sorted(raw_dir.rglob("*")):
        if path.is_file() and is_ignored_path(path):
            ignored_entries.append(path.relative_to(raw_dir).as_posix())
    return tuple(ignored_entries)


def _summarize_datasets(
    *,
    raw_dir: Path,
    polymarket_root: Path,
    discovered_files: list[InventoryFile],
) -> tuple[DatasetInventorySummary, ...]:
    polymarket_relative_root = polymarket_root.relative_to(raw_dir).as_posix()
    grouped_files: dict[str, list[InventoryFile]] = {dataset: [] for dataset in POLYMARKET_DATASET_ORDER}
    for file in discovered_files:
        grouped_files[file.dataset].append(file)

    summaries = []
    for dataset in POLYMARKET_DATASET_ORDER:
        status, note = DATASET_DECISIONS[dataset]
        dataset_files = sorted(grouped_files[dataset], key=_dataset_file_sort_key)
        paths = [file.relative_path for file in dataset_files]
        partitions = [file.partition for file in dataset_files if file.partition is not None]
        summaries.append(
            DatasetInventorySummary(
                dataset=dataset,
                status=status,
                note=note,
                file_count=len(dataset_files),
                total_size_bytes=sum(file.size_bytes for file in dataset_files),
                glob_pattern=_build_dataset_glob_pattern(polymarket_relative_root, dataset),
                first_path=paths[0] if paths else None,
                last_path=paths[-1] if paths else None,
                representative_paths=_select_representative_paths(paths),
                partition_start_min=min((partition.start for partition in partitions), default=None),
                partition_end_max=max((partition.end for partition in partitions), default=None),
            )
        )

    return tuple(summaries)


def _build_dataset_glob_pattern(polymarket_relative_root: str, dataset: str) -> str:
    if dataset == "lookup_json":
        return f"{polymarket_relative_root}/fpmm_collateral_lookup.json"
    return f"{polymarket_relative_root}/{dataset}/*"


def _select_representative_paths(paths: list[str]) -> tuple[str, ...]:
    if not paths:
        return ()
    if len(paths) == 1:
        return (paths[0],)
    if len(paths) == 2:
        return tuple(paths)
    return (paths[0], paths[1], paths[-2], paths[-1])


def _dataset_file_sort_key(file: InventoryFile) -> tuple[int, int, str]:
    if file.partition is None:
        return (10**18, 10**18, file.relative_path)
    return (file.partition.start, file.partition.end, file.relative_path)
