from __future__ import annotations

from pathlib import Path

import duckdb

from src.research import ensure_notebook_study_context, resolve_notebook_study_paths
from tests.helpers_polymarket_canonical import write_canonical_fixture


def test_resolve_notebook_study_paths_finds_shared_repo_data_from_worktree_anchor(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    worktree_root = repo_root / ".worktrees" / "feature"
    raw_dir = repo_root / "data" / "raw"
    warehouse_dir = repo_root / "data" / "warehouse"

    worktree_root.mkdir(parents=True)
    write_canonical_fixture(raw_dir)
    warehouse_dir.mkdir(parents=True)

    paths = resolve_notebook_study_paths(start_dir=worktree_root)

    assert paths.raw_dir == raw_dir
    assert paths.polymarket_root == raw_dir / "data" / "polymarket"
    assert paths.warehouse_path == warehouse_dir / "extreme_probability.duckdb"
    assert paths.canonical_ready is False
    assert paths.analysis_ready is False


def test_ensure_notebook_study_context_builds_missing_canonical_and_analysis_tables(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    warehouse_path = tmp_path / "warehouse" / "extreme_probability.duckdb"
    write_canonical_fixture(raw_dir)

    context = ensure_notebook_study_context(
        raw_dir=raw_dir,
        warehouse_path=warehouse_path,
        bootstrap_samples=100,
        random_seed=19,
    )

    assert context.canonical_built is True
    assert context.analysis_built is True
    assert context.analysis_result is not None
    assert context.paths.raw_dir == raw_dir
    assert context.paths.polymarket_root == raw_dir / "data" / "polymarket"
    assert context.paths.warehouse_path == warehouse_path
    assert context.paths.canonical_ready is True
    assert context.paths.analysis_ready is True

    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        table_names = {
            row[0]
            for row in connection.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
                """
            ).fetchall()
        }

    assert "tick_observations" in table_names
    assert "threshold_entry_events" in table_names
    assert "calibration_summaries" in table_names


def test_ensure_notebook_study_context_reuses_existing_warehouse_tables(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    warehouse_path = tmp_path / "warehouse" / "extreme_probability.duckdb"
    write_canonical_fixture(raw_dir)

    first_context = ensure_notebook_study_context(
        raw_dir=raw_dir,
        warehouse_path=warehouse_path,
        bootstrap_samples=80,
        random_seed=7,
    )
    second_context = ensure_notebook_study_context(
        raw_dir=raw_dir,
        warehouse_path=warehouse_path,
        bootstrap_samples=80,
        random_seed=7,
    )

    assert first_context.canonical_built is True
    assert first_context.analysis_built is True
    assert second_context.canonical_built is False
    assert second_context.analysis_built is False
    assert second_context.analysis_result is None
