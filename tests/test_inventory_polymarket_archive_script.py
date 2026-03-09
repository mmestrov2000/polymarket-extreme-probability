from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "inventory_polymarket_archive.py"


def load_script_module():
    spec = importlib.util.spec_from_file_location("inventory_polymarket_archive", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_main_writes_manifest_and_markdown_summary(tmp_path, capsys) -> None:
    module = load_script_module()
    raw_dir = tmp_path / "raw"
    _write_inventory_fixture(raw_dir)
    manifest_path = tmp_path / "reports" / "polymarket_inventory_manifest.json"
    summary_path = tmp_path / "reports" / "polymarket_inventory_summary.md"

    exit_code = module.main(
        [
            "--raw-dir",
            str(raw_dir),
            "--manifest-path",
            str(manifest_path),
            "--summary-path",
            str(summary_path),
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Polymarket inventory report written" in captured.out
    assert "Included dataset: markets (1 files" in captured.out
    assert "Deferred datasets: trades (1 files" in captured.out
    assert "legacy_trades (1 files" in captured.out

    manifest = json.loads(manifest_path.read_text())
    summary = summary_path.read_text()

    assert manifest["included_dataset"] == "markets"
    assert manifest["deferred_datasets"] == ["trades", "legacy_trades"]
    assert manifest["ignored_entries"] == [".DS_Store", "data/.DS_Store"]

    dataset_groups = {item["dataset"]: item for item in manifest["dataset_groups"]}
    assert dataset_groups["markets"]["status"] == "included"
    assert dataset_groups["trades"]["status"] == "deferred"
    assert dataset_groups["legacy_trades"]["status"] == "deferred"
    assert dataset_groups["blocks"]["file_count"] == 0

    assert "# Polymarket Milestone 1 Inventory and Schema Mapping" in summary
    assert "First-pass analysis source: `markets` snapshot parquet files" in summary
    assert "Deferred record types: `trades`, `legacy_trades`" in summary
    assert "priced probability" in summary.lower()


def _write_inventory_fixture(raw_dir: Path) -> None:
    polymarket_root = raw_dir / "data" / "polymarket"
    (polymarket_root / "blocks").mkdir(parents=True)
    (polymarket_root / "markets").mkdir()
    (polymarket_root / "trades").mkdir()
    (polymarket_root / "legacy_trades").mkdir()
    (raw_dir / ".DS_Store").write_text("noise")
    (raw_dir / "data" / ".DS_Store").parent.mkdir(parents=True, exist_ok=True)
    (raw_dir / "data" / ".DS_Store").write_text("noise")

    _write_parquet(
        polymarket_root / "markets" / "markets_0_10000.parquet",
        """
        SELECT
            'm-1' AS id,
            '0xcondition1' AS condition_id,
            'Will it rain?' AS question,
            'will-it-rain' AS slug,
            '["Yes", "No"]' AS outcomes,
            '["0.25", "0.75"]' AS outcome_prices,
            '["111", "222"]' AS clob_token_ids,
            100.0::DOUBLE AS volume,
            50.0::DOUBLE AS liquidity,
            TRUE AS active,
            FALSE AS closed,
            TIMESTAMPTZ '2026-03-12 12:00:00+00:00' AS end_date,
            TIMESTAMPTZ '2026-03-01 00:00:00+00:00' AS created_at,
            '0xmaker' AS market_maker_address,
            TIMESTAMP '2026-03-09 10:00:00' AS _fetched_at
        """,
    )
    _write_parquet(
        polymarket_root / "trades" / "trades_0_10000.parquet",
        """
        SELECT
            10::BIGINT AS block_number,
            '0xtx' AS transaction_hash,
            1::BIGINT AS log_index,
            '0xorder' AS order_hash,
            '0xmaker' AS maker,
            '0xtaker' AS taker,
            '111' AS maker_asset_id,
            '222' AS taker_asset_id,
            100::BIGINT AS maker_amount,
            90::BIGINT AS taker_amount,
            1::BIGINT AS fee,
            '2026-03-09T10:00:00Z' AS timestamp,
            TIMESTAMP '2026-03-09 10:05:00' AS _fetched_at,
            '0xcontract' AS _contract
        """,
    )
    _write_parquet(
        polymarket_root / "legacy_trades" / "trades_0_10000.parquet",
        """
        SELECT
            10::BIGINT AS block_number,
            '0xtx' AS transaction_hash,
            1::BIGINT AS log_index,
            '0xfpmm' AS fpmm_address,
            '0xtrader' AS trader,
            '25.0' AS amount,
            '0.1' AS fee_amount,
            0::BIGINT AS outcome_index,
            '10.0' AS outcome_tokens,
            TRUE AS is_buy,
            '2026-03-09T10:00:00Z' AS timestamp,
            TIMESTAMP '2026-03-09 10:05:00' AS _fetched_at
        """,
    )
    (polymarket_root / "fpmm_collateral_lookup.json").write_text("{}")


def _write_parquet(path: Path, query: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(database=":memory:") as connection:
        safe_path = path.as_posix().replace("'", "''")
        connection.execute(f"COPY ({query}) TO '{safe_path}' (FORMAT PARQUET)")
