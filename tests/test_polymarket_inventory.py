from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import duckdb

from src.datasets import (
    build_archive_inventory,
    build_dataset_schema_snapshot,
    parse_partition_range,
    resolve_closed_market_outcome,
    select_market_priced_probability,
)


def test_build_archive_inventory_ignores_noise_and_summarizes_dataset_groups(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    polymarket_root = raw_dir / "data" / "polymarket"
    (polymarket_root / "blocks").mkdir(parents=True)
    (polymarket_root / "markets").mkdir()
    (polymarket_root / "trades").mkdir()
    (polymarket_root / "legacy_trades").mkdir()

    (raw_dir / ".DS_Store").write_text("noise")
    (raw_dir / "data" / ".DS_Store").parent.mkdir(parents=True, exist_ok=True)
    (raw_dir / "data" / ".DS_Store").write_text("noise")
    (polymarket_root / "markets" / "._markets_0_10000.parquet").write_text("noise")

    (polymarket_root / "markets" / "markets_0_10000.parquet").write_text("market")
    (polymarket_root / "markets" / "markets_10000_20000.parquet").write_text("market-2")
    (polymarket_root / "trades" / "trades_0_10000.parquet").write_text("trade")
    (polymarket_root / "legacy_trades" / "trades_0_10000.parquet").write_text("legacy")
    (polymarket_root / "fpmm_collateral_lookup.json").write_text("{}")

    inventory = build_archive_inventory(raw_dir)
    dataset_summaries = inventory.dataset_summary_by_name()

    assert inventory.ignored_entries == (
        ".DS_Store",
        "data/.DS_Store",
        "data/polymarket/markets/._markets_0_10000.parquet",
    )
    assert [file.relative_path for file in inventory.files] == [
        "data/polymarket/fpmm_collateral_lookup.json",
        "data/polymarket/legacy_trades/trades_0_10000.parquet",
        "data/polymarket/markets/markets_0_10000.parquet",
        "data/polymarket/markets/markets_10000_20000.parquet",
        "data/polymarket/trades/trades_0_10000.parquet",
    ]

    markets_summary = dataset_summaries["markets"]
    assert markets_summary.status == "included"
    assert markets_summary.file_count == 2
    assert markets_summary.partition_start_min == 0
    assert markets_summary.partition_end_max == 20000
    assert markets_summary.first_path == "data/polymarket/markets/markets_0_10000.parquet"
    assert markets_summary.last_path == "data/polymarket/markets/markets_10000_20000.parquet"

    trades_summary = dataset_summaries["trades"]
    assert trades_summary.status == "deferred"
    assert trades_summary.file_count == 1

    assert dataset_summaries["blocks"].status == "excluded"
    assert dataset_summaries["blocks"].file_count == 0
    assert dataset_summaries["lookup_json"].status == "auxiliary"
    assert dataset_summaries["lookup_json"].file_count == 1


def test_parse_partition_range_matches_expected_filename_shape() -> None:
    partition = parse_partition_range("trades_144750000_144760000.parquet")

    assert partition is not None
    assert partition.start == 144750000
    assert partition.end == 144760000
    assert parse_partition_range("fpmm_collateral_lookup.json") is None


def test_build_dataset_schema_snapshot_reads_required_columns_from_parquet(tmp_path: Path) -> None:
    raw_dir = tmp_path / "raw"
    parquet_path = raw_dir / "data" / "polymarket" / "markets" / "markets_0_10000.parquet"
    _write_markets_parquet(parquet_path)

    snapshot = build_dataset_schema_snapshot("markets", parquet_path, base_dir=raw_dir)

    assert snapshot.sample_path == "data/polymarket/markets/markets_0_10000.parquet"
    assert snapshot.missing_columns == ()
    assert [column.name for column in snapshot.columns][:6] == [
        "id",
        "condition_id",
        "question",
        "slug",
        "outcomes",
        "outcome_prices",
    ]


def test_select_market_priced_probability_prefers_yes_side_and_falls_back_to_no_side() -> None:
    direct_yes = select_market_priced_probability(
        outcomes='["Yes", "No"]',
        outcome_prices='["0.23", "0.77"]',
        clob_token_ids='["111", "222"]',
    )
    fallback_no = select_market_priced_probability(
        outcomes='["Yes", "No"]',
        outcome_prices='["bad", "0.81"]',
        clob_token_ids='["111", "222"]',
    )

    assert direct_yes.included is True
    assert direct_yes.probability == Decimal("0.23")
    assert direct_yes.probability_source == "yes_outcome_price"
    assert fallback_no.included is True
    assert fallback_no.probability == Decimal("0.19")
    assert fallback_no.probability_source == "1_minus_no_outcome_price"


def test_select_market_priced_probability_excludes_ambiguous_or_invalid_rows() -> None:
    ambiguous = select_market_priced_probability(
        outcomes='["Up", "Down"]',
        outcome_prices='["0.3", "0.7"]',
        clob_token_ids='["111", "222"]',
    )
    non_binary = select_market_priced_probability(
        outcomes='["Yes", "No", "Maybe"]',
        outcome_prices='["0.3", "0.7", "0.0"]',
        clob_token_ids='["111", "222", "333"]',
    )
    missing_tokens = select_market_priced_probability(
        outcomes='["Yes", "No"]',
        outcome_prices='["0.3", "0.7"]',
        clob_token_ids='["111"]',
    )
    out_of_range = select_market_priced_probability(
        outcomes='["Yes", "No"]',
        outcome_prices='["1.4", "-0.4"]',
        clob_token_ids='["111", "222"]',
    )

    assert ambiguous.exclusion_reason == "ambiguous_outcome_labels"
    assert non_binary.exclusion_reason == "non_binary_outcomes"
    assert missing_tokens.exclusion_reason == "missing_token_alignment"
    assert out_of_range.exclusion_reason == "probability_out_of_range"


def test_resolve_closed_market_outcome_requires_terminal_prices() -> None:
    yes_resolution = resolve_closed_market_outcome(
        outcomes='["Yes", "No"]',
        outcome_prices='["1", "0"]',
        clob_token_ids='["111", "222"]',
        closed=True,
    )
    no_resolution = resolve_closed_market_outcome(
        outcomes='["Yes", "No"]',
        outcome_prices='["0", "1"]',
        clob_token_ids='["111", "222"]',
        closed=True,
    )
    open_market = resolve_closed_market_outcome(
        outcomes='["Yes", "No"]',
        outcome_prices='["1", "0"]',
        clob_token_ids='["111", "222"]',
        closed=False,
    )
    unresolved_terminal = resolve_closed_market_outcome(
        outcomes='["Yes", "No"]',
        outcome_prices='["0.6", "0.4"]',
        clob_token_ids='["111", "222"]',
        closed=True,
    )

    assert yes_resolution.resolved_outcome == "YES"
    assert no_resolution.resolved_outcome == "NO"
    assert open_market.reason == "market_not_closed"
    assert unresolved_terminal.reason == "terminal_prices_not_collapsed"


def _write_markets_parquet(path: Path) -> None:
    _write_parquet(
        path,
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


def _write_parquet(path: Path, query: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(database=":memory:") as connection:
        safe_path = path.as_posix().replace("'", "''")
        connection.execute(f"COPY ({query}) TO '{safe_path}' (FORMAT PARQUET)")
