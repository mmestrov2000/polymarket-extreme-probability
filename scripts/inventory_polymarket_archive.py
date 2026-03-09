#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.datasets import (  # noqa: E402
    build_polymarket_inventory_report,
    render_inventory_manifest,
    render_inventory_summary_markdown,
)


DEFAULT_RAW_DIR = Path("data/raw")
DEFAULT_MANIFEST_PATH = Path("reports/summaries/polymarket_inventory_manifest.json")
DEFAULT_SUMMARY_PATH = Path("reports/summaries/polymarket_inventory_summary.md")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inventory the extracted Polymarket archive and persist a Milestone 1 schema-mapping summary."
    )
    parser.add_argument(
        "--raw-dir",
        default=str(DEFAULT_RAW_DIR),
        help=f"Archive root to inspect. Defaults to {DEFAULT_RAW_DIR}.",
    )
    parser.add_argument(
        "--manifest-path",
        default=str(DEFAULT_MANIFEST_PATH),
        help=f"Path to write the JSON manifest. Defaults to {DEFAULT_MANIFEST_PATH}.",
    )
    parser.add_argument(
        "--summary-path",
        default=str(DEFAULT_SUMMARY_PATH),
        help=f"Path to write the Markdown summary. Defaults to {DEFAULT_SUMMARY_PATH}.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    raw_dir = _resolve_raw_dir(Path(args.raw_dir))
    manifest_path = Path(args.manifest_path)
    summary_path = Path(args.summary_path)

    report = build_polymarket_inventory_report(raw_dir)
    raw_dir_display = Path(args.raw_dir).as_posix()
    polymarket_root_display = _build_display_polymarket_root(report, raw_dir_display=raw_dir_display)
    manifest = render_inventory_manifest(
        report,
        raw_dir_display=raw_dir_display,
        polymarket_root_display=polymarket_root_display,
    )
    summary = render_inventory_summary_markdown(
        report,
        raw_dir_display=raw_dir_display,
        polymarket_root_display=polymarket_root_display,
    )

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    summary_path.write_text(summary)

    inventory = report.inventory.dataset_summary_by_name()
    print("Polymarket inventory report written")
    print(f"Included dataset: {_format_dataset_brief(inventory['markets'])}")
    print(
        "Deferred datasets: "
        + ", ".join(
            _format_dataset_brief(inventory[name])
            for name in ("trades", "legacy_trades")
        )
    )
    print(f"Ignored entries: {len(report.inventory.ignored_entries)}")
    print(f"Manifest: {manifest_path.as_posix()}")
    print(f"Summary: {summary_path.as_posix()}")
    return 0


def _format_dataset_brief(summary) -> str:
    return f"{summary.dataset} ({summary.file_count} files, {summary.total_size_bytes} bytes)"


def _resolve_raw_dir(raw_dir: Path) -> Path:
    if raw_dir.is_absolute() and raw_dir.exists():
        return raw_dir

    candidates = []
    if raw_dir.is_absolute():
        candidates.append(raw_dir)
    else:
        candidates.extend(
            [
                Path.cwd() / raw_dir,
                REPO_ROOT / raw_dir,
                REPO_ROOT.parent.parent / raw_dir,
            ]
        )

    seen = set()
    for candidate in candidates:
        normalized = candidate.resolve() if candidate.exists() else candidate
        key = normalized.as_posix()
        if key in seen:
            continue
        seen.add(key)
        if candidate.exists():
            return candidate
    return raw_dir


def _build_display_polymarket_root(report, *, raw_dir_display: str) -> str:
    actual_raw_dir = Path(report.inventory.raw_dir)
    actual_polymarket_root = Path(report.inventory.polymarket_root)
    suffix = actual_polymarket_root.relative_to(actual_raw_dir).as_posix()
    return (Path(raw_dir_display) / suffix).as_posix()


if __name__ == "__main__":
    raise SystemExit(main())
