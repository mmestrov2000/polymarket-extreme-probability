from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import duckdb

from src.datasets import build_polymarket_canonical_dataset
from tests.helpers_polymarket_canonical import load_canonical_script_module, write_canonical_fixture


def test_build_polymarket_canonical_dataset_persists_expected_tables(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    warehouse_path = tmp_path / "warehouse" / "extreme_probability.duckdb"
    write_canonical_fixture(raw_dir)

    result = build_polymarket_canonical_dataset(raw_dir, warehouse_path=warehouse_path)

    assert result.counts.archive_inventory == 3
    assert result.counts.market_catalog == 3
    assert result.counts.contract_catalog == 6
    assert result.counts.tick_observations == 9
    assert result.counts.resolution_outcomes == 6
    assert result.counts.threshold_entry_events == 7
    assert result.excluded_unmappable_observation_count == 1
    assert result.unresolved_market_count == 1
    assert result.ambiguous_resolution_market_count == 0

    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        inventory_rows = connection.execute(
            """
            SELECT relative_path, dataset
            FROM archive_inventory
            ORDER BY relative_path
            """
        ).fetchall()
        assert inventory_rows == [
            ("data/polymarket/fpmm_collateral_lookup.json", "lookup_json"),
            ("data/polymarket/markets/markets_0_10000.parquet", "markets"),
            ("data/polymarket/markets/markets_10000_20000.parquet", "markets"),
        ]

        market_rows = connection.execute(
            """
            SELECT market_id, tick_observation_count, market_resolved_outcome
            FROM market_catalog
            ORDER BY market_id
            """
        ).fetchall()
        assert market_rows == [
            ("m-high", 3, "NO"),
            ("m-low", 2, "YES"),
            ("m-reentry", 4, "YES"),
        ]

        contract_rows = connection.execute(
            """
            SELECT market_id, contract_id, contract_side, paired_contract_id
            FROM contract_catalog
            ORDER BY market_id, CASE contract_side WHEN 'YES' THEN 0 ELSE 1 END
            """
        ).fetchall()
        assert contract_rows == [
            ("m-high", "y-high", "YES", "n-high"),
            ("m-high", "n-high", "NO", "y-high"),
            ("m-low", "y-low", "YES", "n-low"),
            ("m-low", "n-low", "NO", "y-low"),
            ("m-reentry", "y-reentry", "YES", "n-reentry"),
            ("m-reentry", "n-reentry", "NO", "y-reentry"),
        ]

        tick_rows = connection.execute(
            """
            SELECT market_id, contract_id, observation_time_utc, probability, price_source
            FROM tick_observations
            ORDER BY contract_id, observation_time_utc
            """
        ).fetchall()
        assert tick_rows == [
            ("m-high", "y-high", _ts("2026-03-01 11:00:00"), Decimal("0.950000000000000000"), "yes_outcome_price"),
            ("m-high", "y-high", _ts("2026-03-01 12:00:00"), Decimal("0.930000000000000000"), "yes_outcome_price"),
            ("m-high", "y-high", _ts("2026-03-02 11:00:00"), Decimal("0E-18"), "yes_outcome_price"),
            ("m-low", "y-low", _ts("2026-03-01 10:00:00"), Decimal("0.080000000000000000"), "yes_outcome_price"),
            ("m-low", "y-low", _ts("2026-03-02 10:00:00"), Decimal("1.000000000000000000"), "yes_outcome_price"),
            (
                "m-reentry",
                "y-reentry",
                _ts("2026-03-01 12:00:00"),
                Decimal("0.080000000000000000"),
                "yes_outcome_price",
            ),
            (
                "m-reentry",
                "y-reentry",
                _ts("2026-03-01 13:00:00"),
                Decimal("0.120000000000000000"),
                "yes_outcome_price",
            ),
            (
                "m-reentry",
                "y-reentry",
                _ts("2026-03-01 14:00:00"),
                Decimal("0.070000000000000000"),
                "yes_outcome_price",
            ),
            (
                "m-reentry",
                "y-reentry",
                _ts("2026-03-02 09:00:00"),
                Decimal("1.000000000000000000"),
                "yes_outcome_price",
            ),
        ]

        resolution_rows = connection.execute(
            """
            SELECT market_id, contract_id, contract_side, resolved_outcome, market_resolved_outcome
            FROM resolution_outcomes
            ORDER BY market_id, CASE contract_side WHEN 'YES' THEN 0 ELSE 1 END
            """
        ).fetchall()
        assert resolution_rows == [
            ("m-high", "y-high", "YES", "NO", "NO"),
            ("m-high", "n-high", "NO", "YES", "NO"),
            ("m-low", "y-low", "YES", "YES", "YES"),
            ("m-low", "n-low", "NO", "NO", "YES"),
            ("m-reentry", "y-reentry", "YES", "YES", "YES"),
            ("m-reentry", "n-reentry", "NO", "NO", "YES"),
        ]

        threshold_rows = connection.execute(
            """
            SELECT contract_id, threshold_bucket, entry_time_utc, event_index, resolved_outcome
            FROM threshold_entry_events
            ORDER BY contract_id, event_index
            """
        ).fetchall()
        assert threshold_rows == [
            ("y-high", "high_probability", _ts("2026-03-01 11:00:00"), 1, "NO"),
            ("y-high", "low_probability", _ts("2026-03-02 11:00:00"), 2, "NO"),
            ("y-low", "low_probability", _ts("2026-03-01 10:00:00"), 1, "YES"),
            ("y-low", "high_probability", _ts("2026-03-02 10:00:00"), 2, "YES"),
            ("y-reentry", "low_probability", _ts("2026-03-01 12:00:00"), 1, "YES"),
            ("y-reentry", "low_probability", _ts("2026-03-01 14:00:00"), 2, "YES"),
            ("y-reentry", "high_probability", _ts("2026-03-02 09:00:00"), 3, "YES"),
        ]


def test_build_polymarket_canonical_dataset_script_writes_expected_output(tmp_path, capsys) -> None:
    module = load_canonical_script_module()
    raw_dir = tmp_path / "raw"
    warehouse_path = tmp_path / "warehouse" / "extreme_probability.duckdb"
    write_canonical_fixture(raw_dir)

    exit_code = module.main(
        [
            "--raw-dir",
            str(raw_dir),
            "--warehouse-path",
            str(warehouse_path),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Polymarket canonical dataset written" in captured.out
    assert "archive_inventory: 3" in captured.out
    assert "market_catalog: 3" in captured.out
    assert "contract_catalog: 6" in captured.out
    assert "tick_observations: 9" in captured.out
    assert "resolution_outcomes: 6" in captured.out
    assert "threshold_entry_events: 7" in captured.out
    assert warehouse_path.exists() is True


def _ts(value: str):
    return duckdb.execute(f"SELECT TIMESTAMP '{value}'").fetchone()[0]
