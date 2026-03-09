#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.analysis import build_extreme_probability_analysis, render_analysis_report_markdown  # noqa: E402
from src.datasets import DEFAULT_CANONICAL_WAREHOUSE_PATH, build_polymarket_canonical_dataset  # noqa: E402


DEFAULT_RAW_DIR = Path("data/raw")
DEFAULT_REPORT_PATH = Path("reports/summaries/milestone3_statistical_analysis.md")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build Milestone 3 extreme-probability calibration summaries from the canonical warehouse. "
            "If the warehouse is missing, the Polymarket canonical dataset is built first from raw archive files."
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
        "--report-path",
        default=str(DEFAULT_REPORT_PATH),
        help=f"Markdown report path to write. Defaults to {DEFAULT_REPORT_PATH}.",
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
        help="Force a rebuild of the Polymarket canonical warehouse before analysis.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    raw_dir = _resolve_path(Path(args.raw_dir))
    warehouse_path = Path(args.warehouse_path)
    report_path = Path(args.report_path)

    if args.rebuild_canonical or not _warehouse_has_canonical_tables(warehouse_path):
        build_polymarket_canonical_dataset(raw_dir, warehouse_path=warehouse_path)
        print("Canonical dataset built for analysis")

    result = build_extreme_probability_analysis(
        warehouse_path,
        bootstrap_samples=args.bootstrap_samples,
        random_seed=args.random_seed,
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(render_analysis_report_markdown(result), encoding="utf-8")

    print("Milestone 3 statistical analysis written")
    print(f"Warehouse: {warehouse_path.as_posix()}")
    print(f"Report: {report_path.as_posix()}")
    for table_name, row_count in result.counts.to_dict().items():
        print(f"{table_name}: {row_count}")
    print(f"Present venues: {', '.join(result.present_venues) or 'none'}")
    if result.missing_expected_venues:
        print(f"Missing expected venues: {', '.join(result.missing_expected_venues)}")
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
