from __future__ import annotations

from src.compat import dataclass
from pathlib import Path

import duckdb

from src.analysis import ExtremeProbabilityAnalysisResult, build_extreme_probability_analysis
from src.datasets import DEFAULT_CANONICAL_WAREHOUSE_PATH, build_polymarket_canonical_dataset
from src.datasets.archive_inventory import resolve_polymarket_root


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW_DIR = Path("data/raw")
CANONICAL_REQUIRED_TABLES = (
    "archive_inventory",
    "contract_catalog",
    "market_catalog",
    "resolution_outcomes",
    "threshold_entry_events",
    "tick_observations",
)
ANALYSIS_REQUIRED_TABLES = (
    "calibration_segments",
    "calibration_sensitivity",
    "calibration_summaries",
)


@dataclass(frozen=True, slots=True)
class NotebookStudyPaths:
    raw_dir: Path
    polymarket_root: Path
    warehouse_path: Path
    canonical_ready: bool
    analysis_ready: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "raw_dir": self.raw_dir.as_posix(),
            "polymarket_root": self.polymarket_root.as_posix(),
            "warehouse_path": self.warehouse_path.as_posix(),
            "canonical_ready": self.canonical_ready,
            "analysis_ready": self.analysis_ready,
        }


@dataclass(frozen=True, slots=True)
class NotebookStudyContext:
    paths: NotebookStudyPaths
    canonical_built: bool
    analysis_built: bool
    analysis_result: ExtremeProbabilityAnalysisResult | None

    def to_dict(self) -> dict[str, object]:
        payload = {
            "paths": self.paths.to_dict(),
            "canonical_built": self.canonical_built,
            "analysis_built": self.analysis_built,
        }
        if self.analysis_result is not None:
            payload["analysis_result"] = self.analysis_result.to_dict()
        return payload


def resolve_notebook_study_paths(
    *,
    raw_dir: Path | str | None = None,
    warehouse_path: Path | str | None = None,
    start_dir: Path | str | None = None,
) -> NotebookStudyPaths:
    anchor = Path(start_dir) if start_dir is not None else Path.cwd()
    resolved_raw_dir = _resolve_existing_path(Path(raw_dir) if raw_dir is not None else DEFAULT_RAW_DIR, anchor)
    resolved_polymarket_root = resolve_polymarket_root(resolved_raw_dir)
    if warehouse_path is None:
        resolved_warehouse_path = resolved_raw_dir.parent / "warehouse" / DEFAULT_CANONICAL_WAREHOUSE_PATH.name
    else:
        resolved_warehouse_path = _resolve_output_path(Path(warehouse_path), anchor)
    canonical_ready = _warehouse_has_tables(resolved_warehouse_path, CANONICAL_REQUIRED_TABLES)
    analysis_ready = _warehouse_has_tables(resolved_warehouse_path, ANALYSIS_REQUIRED_TABLES)
    return NotebookStudyPaths(
        raw_dir=resolved_raw_dir,
        polymarket_root=resolved_polymarket_root,
        warehouse_path=resolved_warehouse_path,
        canonical_ready=canonical_ready,
        analysis_ready=analysis_ready,
    )


def ensure_notebook_study_context(
    *,
    raw_dir: Path | str | None = None,
    warehouse_path: Path | str | None = None,
    start_dir: Path | str | None = None,
    force_rebuild: bool = False,
    bootstrap_samples: int = 400,
    random_seed: int = 17,
) -> NotebookStudyContext:
    initial_paths = resolve_notebook_study_paths(
        raw_dir=raw_dir,
        warehouse_path=warehouse_path,
        start_dir=start_dir,
    )

    canonical_built = False
    analysis_built = False
    analysis_result: ExtremeProbabilityAnalysisResult | None = None

    if force_rebuild or not initial_paths.canonical_ready:
        build_polymarket_canonical_dataset(
            initial_paths.raw_dir,
            warehouse_path=initial_paths.warehouse_path,
        )
        canonical_built = True

    if force_rebuild or canonical_built or not _warehouse_has_tables(
        initial_paths.warehouse_path,
        ANALYSIS_REQUIRED_TABLES,
    ):
        analysis_result = build_extreme_probability_analysis(
            initial_paths.warehouse_path,
            bootstrap_samples=bootstrap_samples,
            random_seed=random_seed,
        )
        analysis_built = True

    resolved_paths = resolve_notebook_study_paths(
        raw_dir=initial_paths.raw_dir,
        warehouse_path=initial_paths.warehouse_path,
        start_dir=Path(start_dir) if start_dir is not None else Path.cwd(),
    )
    return NotebookStudyContext(
        paths=resolved_paths,
        canonical_built=canonical_built,
        analysis_built=analysis_built,
        analysis_result=analysis_result,
    )


def _resolve_existing_path(path: Path, start_dir: Path) -> Path:
    if path.is_absolute():
        if path.exists():
            return path
        raise FileNotFoundError(f"Could not find required path: {path}")

    for anchor in _iter_search_roots(start_dir):
        candidate = anchor / path
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"Could not find required path: {path}")


def _resolve_output_path(path: Path, start_dir: Path) -> Path:
    if path.is_absolute():
        return path

    candidates = [anchor / path for anchor in _iter_search_roots(start_dir)]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    for candidate in candidates:
        if candidate.parent.exists():
            return candidate
    return start_dir / path


def _iter_search_roots(start_dir: Path) -> tuple[Path, ...]:
    roots: list[Path] = []
    seen: set[str] = set()
    for anchor in (start_dir, *start_dir.parents, REPO_ROOT, *REPO_ROOT.parents):
        key = anchor.as_posix()
        if key in seen:
            continue
        seen.add(key)
        roots.append(anchor)
    return tuple(roots)


def _warehouse_has_tables(warehouse_path: Path, required_tables: tuple[str, ...]) -> bool:
    if not warehouse_path.exists():
        return False

    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            """
        ).fetchall()
    return set(required_tables).issubset({row[0] for row in rows})
