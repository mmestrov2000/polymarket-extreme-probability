from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "build_polymarket_canonical_dataset.py"


def load_canonical_script_module():
    spec = importlib.util.spec_from_file_location("build_polymarket_canonical_dataset", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_canonical_fixture(raw_dir: Path) -> None:
    polymarket_root = raw_dir / "data" / "polymarket"
    (polymarket_root / "blocks").mkdir(parents=True)
    (polymarket_root / "markets").mkdir()
    (polymarket_root / "trades").mkdir()
    (polymarket_root / "legacy_trades").mkdir()
    (raw_dir / ".DS_Store").write_text("noise")
    (raw_dir / "data" / ".DS_Store").parent.mkdir(parents=True, exist_ok=True)
    (raw_dir / "data" / ".DS_Store").write_text("noise")
    (polymarket_root / "markets" / "._markets_20000_30000.parquet").write_text("noise")
    (polymarket_root / "fpmm_collateral_lookup.json").write_text("{}")

    _write_parquet(
        polymarket_root / "markets" / "markets_0_10000.parquet",
        """
        SELECT
            'm-low' AS id,
            'condition-low' AS condition_id,
            'Will the low market resolve YES?' AS question,
            'low-market' AS slug,
            '["Yes", "No"]' AS outcomes,
            '["0.08", "0.92"]' AS outcome_prices,
            '["y-low", "n-low"]' AS clob_token_ids,
            120.0::DOUBLE AS volume,
            80.0::DOUBLE AS liquidity,
            TRUE AS active,
            FALSE AS closed,
            TIMESTAMPTZ '2026-03-10 00:00:00+00:00' AS end_date,
            TIMESTAMPTZ '2026-02-28 00:00:00+00:00' AS created_at,
            '0xmaker' AS market_maker_address,
            TIMESTAMP '2026-03-01 10:00:00' AS _fetched_at
        UNION ALL
        SELECT
            'm-high',
            'condition-high',
            'Will the high market resolve NO?',
            'high-market',
            '["Yes", "No"]',
            '["0.95", "0.05"]',
            '["y-high", "n-high"]',
            150.0::DOUBLE,
            90.0::DOUBLE,
            TRUE,
            FALSE,
            TIMESTAMPTZ '2026-03-11 00:00:00+00:00',
            TIMESTAMPTZ '2026-02-28 00:00:00+00:00',
            '0xmaker',
            TIMESTAMP '2026-03-01 11:00:00'
        UNION ALL
        SELECT
            'm-reentry',
            'condition-reentry',
            'Will the reentry market resolve YES?',
            'reentry-market',
            '["Yes", "No"]',
            '["0.08", "0.92"]',
            '["y-reentry", "n-reentry"]',
            175.0::DOUBLE,
            95.0::DOUBLE,
            TRUE,
            FALSE,
            TIMESTAMPTZ '2026-03-12 00:00:00+00:00',
            TIMESTAMPTZ '2026-02-28 00:00:00+00:00',
            '0xmaker',
            TIMESTAMP '2026-03-01 12:00:00'
        UNION ALL
        SELECT
            'm-zero',
            'condition-zero',
            'Will the zero market ever resolve clearly?',
            'zero-market',
            '["Yes", "No"]',
            '["0", "0"]',
            '["y-zero", "n-zero"]',
            90.0::DOUBLE,
            40.0::DOUBLE,
            TRUE,
            TRUE,
            TIMESTAMPTZ '2026-03-10 00:00:00+00:00',
            TIMESTAMPTZ '2026-02-28 00:00:00+00:00',
            '0xmaker',
            TIMESTAMP '2026-03-01 15:00:00'
        UNION ALL
        SELECT
            'm-amb',
            'condition-amb',
            'Will the ambiguous market be excluded?',
            'ambiguous-market',
            '["Up", "Down"]',
            '["0.40", "0.60"]',
            '["y-amb", "n-amb"]',
            90.0::DOUBLE,
            40.0::DOUBLE,
            TRUE,
            FALSE,
            TIMESTAMPTZ '2026-03-10 00:00:00+00:00',
            TIMESTAMPTZ '2026-02-28 00:00:00+00:00',
            '0xmaker',
            TIMESTAMP '2026-03-01 16:00:00'
        """,
    )
    _write_parquet(
        polymarket_root / "markets" / "markets_10000_20000.parquet",
        """
        SELECT
            'm-low' AS id,
            'condition-low' AS condition_id,
            'Will the low market resolve YES?' AS question,
            'low-market' AS slug,
            '["Yes", "No"]' AS outcomes,
            '["1", "0"]' AS outcome_prices,
            '["y-low", "n-low"]' AS clob_token_ids,
            120.0::DOUBLE AS volume,
            80.0::DOUBLE AS liquidity,
            TRUE AS active,
            TRUE AS closed,
            TIMESTAMPTZ '2026-03-10 00:00:00+00:00' AS end_date,
            TIMESTAMPTZ '2026-02-28 00:00:00+00:00' AS created_at,
            '0xmaker' AS market_maker_address,
            TIMESTAMP '2026-03-02 10:00:00' AS _fetched_at
        UNION ALL
        SELECT
            'm-high',
            'condition-high',
            'Will the high market resolve NO?',
            'high-market',
            '["Yes", "No"]',
            '["0.93", "0.07"]',
            '["y-high", "n-high"]',
            150.0::DOUBLE,
            90.0::DOUBLE,
            TRUE,
            FALSE,
            TIMESTAMPTZ '2026-03-11 00:00:00+00:00',
            TIMESTAMPTZ '2026-02-28 00:00:00+00:00',
            '0xmaker',
            TIMESTAMP '2026-03-01 12:00:00'
        UNION ALL
        SELECT
            'm-high',
            'condition-high',
            'Will the high market resolve NO?',
            'high-market',
            '["Yes", "No"]',
            '["0", "1"]',
            '["y-high", "n-high"]',
            150.0::DOUBLE,
            90.0::DOUBLE,
            TRUE,
            TRUE,
            TIMESTAMPTZ '2026-03-11 00:00:00+00:00',
            TIMESTAMPTZ '2026-02-28 00:00:00+00:00',
            '0xmaker',
            TIMESTAMP '2026-03-02 11:00:00'
        UNION ALL
        SELECT
            'm-reentry',
            'condition-reentry',
            'Will the reentry market resolve YES?',
            'reentry-market',
            '["Yes", "No"]',
            '["0.12", "0.88"]',
            '["y-reentry", "n-reentry"]',
            175.0::DOUBLE,
            95.0::DOUBLE,
            TRUE,
            FALSE,
            TIMESTAMPTZ '2026-03-12 00:00:00+00:00',
            TIMESTAMPTZ '2026-02-28 00:00:00+00:00',
            '0xmaker',
            TIMESTAMP '2026-03-01 13:00:00'
        UNION ALL
        SELECT
            'm-reentry',
            'condition-reentry',
            'Will the reentry market resolve YES?',
            'reentry-market',
            '["Yes", "No"]',
            '["0.07", "0.93"]',
            '["y-reentry", "n-reentry"]',
            175.0::DOUBLE,
            95.0::DOUBLE,
            TRUE,
            FALSE,
            TIMESTAMPTZ '2026-03-12 00:00:00+00:00',
            TIMESTAMPTZ '2026-02-28 00:00:00+00:00',
            '0xmaker',
            TIMESTAMP '2026-03-01 14:00:00'
        UNION ALL
        SELECT
            'm-reentry',
            'condition-reentry',
            'Will the reentry market resolve YES?',
            'reentry-market',
            '["Yes", "No"]',
            '["1", "0"]',
            '["y-reentry", "n-reentry"]',
            175.0::DOUBLE,
            95.0::DOUBLE,
            TRUE,
            TRUE,
            TIMESTAMPTZ '2026-03-12 00:00:00+00:00',
            TIMESTAMPTZ '2026-02-28 00:00:00+00:00',
            '0xmaker',
            TIMESTAMP '2026-03-02 09:00:00'
        """,
    )


def _write_parquet(path: Path, query: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(database=":memory:") as connection:
        safe_path = path.as_posix().replace("'", "''")
        connection.execute(f"COPY ({query}) TO '{safe_path}' (FORMAT PARQUET)")
