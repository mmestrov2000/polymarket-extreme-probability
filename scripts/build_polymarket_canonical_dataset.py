#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.datasets import (  # noqa: E402
    DEFAULT_CANONICAL_WAREHOUSE_PATH,
    build_polymarket_canonical_dataset,
)


DEFAULT_RAW_DIR = Path("data/raw")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build the Polymarket canonical Milestone 2 DuckDB dataset from local raw archive files."
    )
    parser.add_argument(
        "--raw-dir",
        default=str(DEFAULT_RAW_DIR),
        help=f"Archive root to inspect. Defaults to {DEFAULT_RAW_DIR}.",
    )
    parser.add_argument(
        "--warehouse-path",
        default=str(DEFAULT_CANONICAL_WAREHOUSE_PATH),
        help=f"DuckDB path to write. Defaults to {DEFAULT_CANONICAL_WAREHOUSE_PATH}.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    raw_dir = _resolve_path(Path(args.raw_dir))
    warehouse_path = Path(args.warehouse_path)

    result = build_polymarket_canonical_dataset(raw_dir, warehouse_path=warehouse_path)

    print("Polymarket canonical dataset written")
    print(f"Raw dir: {Path(args.raw_dir).as_posix()}")
    print(f"Warehouse: {warehouse_path.as_posix()}")
    for table_name, row_count in result.counts.to_dict().items():
        print(f"{table_name}: {row_count}")
    print(f"Excluded unmappable observations: {result.excluded_unmappable_observation_count}")
    print(f"Markets without clear resolution: {result.unresolved_market_count}")
    print(f"Markets with conflicting resolution rows: {result.ambiguous_resolution_market_count}")
    return 0


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
