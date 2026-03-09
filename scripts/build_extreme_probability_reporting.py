#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.analysis import build_extreme_probability_analysis  # noqa: E402
from src.datasets import DEFAULT_CANONICAL_WAREHOUSE_PATH, build_polymarket_canonical_dataset  # noqa: E402
from src.visualization import write_milestone4_artifacts  # noqa: E402


DEFAULT_RAW_DIR = Path("data/raw")
DEFAULT_FIGURES_DIR = Path("reports/figures")
DEFAULT_MEMO_PATH = Path("reports/summaries/milestone4_decision_memo.md")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build Milestone 4 figures and the decision memo from the canonical extreme-probability warehouse. "
            "If the canonical warehouse is missing, it is rebuilt from the local raw archive first."
        )
    )
    parser.add_argument(
        "--raw-dir",
        default=str(DEFAULT_RAW_DIR),
        help=f"Archive root to inspect when the canonical warehouse needs to be built. Defaults to {DEFAULT_RAW_DIR}.",
    )
    parser.add_argument(
        "--warehouse-path",
        default=str(DEFAULT_CANONICAL_WAREHOUSE_PATH),
        help=f"DuckDB path to read/write. Defaults to {DEFAULT_CANONICAL_WAREHOUSE_PATH}.",
    )
    parser.add_argument(
        "--figures-dir",
        default=str(DEFAULT_FIGURES_DIR),
        help=f"Directory for SVG figures. Defaults to {DEFAULT_FIGURES_DIR}.",
    )
    parser.add_argument(
        "--memo-path",
        default=str(DEFAULT_MEMO_PATH),
        help=f"Markdown decision memo path to write. Defaults to {DEFAULT_MEMO_PATH}.",
    )
    parser.add_argument(
        "--bootstrap-samples",
        type=int,
        default=400,
        help="Number of market-clustered bootstrap samples to draw per summary bucket.",
    )
    parser.add_argument(
        "--random-seed",
        type=int,
        default=17,
        help="Deterministic seed for market-clustered bootstrap estimates.",
    )
    parser.add_argument(
        "--rebuild-canonical",
        action="store_true",
        help="Force a rebuild of the Polymarket canonical warehouse before generating Milestone 4 outputs.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    raw_dir = _resolve_path(Path(args.raw_dir))
    warehouse_path = Path(args.warehouse_path)
    figures_dir = Path(args.figures_dir)
    memo_path = Path(args.memo_path)

    if args.rebuild_canonical or not _warehouse_has_canonical_tables(warehouse_path):
        build_polymarket_canonical_dataset(raw_dir, warehouse_path=warehouse_path)
        print("Canonical dataset built for reporting")

    analysis_result = build_extreme_probability_analysis(
        warehouse_path,
        bootstrap_samples=args.bootstrap_samples,
        random_seed=args.random_seed,
    )
    artifact_result = write_milestone4_artifacts(
        warehouse_path,
        analysis_result,
        figures_dir=figures_dir,
        memo_path=memo_path,
    )

    print("Milestone 4 reporting written")
    print(f"Warehouse: {warehouse_path.as_posix()}")
    print("Figures:")
    for figure_path in artifact_result.figure_paths:
        print(f"- {figure_path}")
    print(f"Memo: {artifact_result.memo_path}")
    print(
        "Low-probability overvaluation: "
        f"{artifact_result.low_probability_assessment.status}"
    )
    print(
        "High-probability undervaluation: "
        f"{artifact_result.high_probability_assessment.status}"
    )
    print(f"Recommendation: {artifact_result.recommendation}")
    if artifact_result.missing_expected_venues:
        print(
            "Missing expected venues: "
            + ", ".join(artifact_result.missing_expected_venues)
        )
    return 0


def _warehouse_has_canonical_tables(warehouse_path: Path) -> bool:
    if not warehouse_path.exists():
        return False

    required_tables = {
        "market_catalog",
        "resolution_outcomes",
        "threshold_entry_events",
        "tick_observations",
    }
    import duckdb

    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            """
        ).fetchall()
    return required_tables.issubset({row[0] for row in rows})


def _resolve_path(path: Path) -> Path:
    if path.is_absolute() and path.exists():
        return path

    candidates = []
    if path.is_absolute():
        candidates.append(path)
    else:
        for anchor in (Path.cwd(), REPO_ROOT):
            candidates.extend((parent / path) for parent in (anchor, *anchor.parents))

    seen = set()
    for candidate in candidates:
        normalized = candidate.resolve() if candidate.exists() else candidate
        key = normalized.as_posix()
        if key in seen:
            continue
        seen.add(key)
        if candidate.exists():
            return candidate
    return path


if __name__ == "__main__":
    raise SystemExit(main())
