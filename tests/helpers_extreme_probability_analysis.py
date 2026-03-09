from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "build_extreme_probability_analysis.py"


def load_analysis_script_module():
    spec = importlib.util.spec_from_file_location("build_extreme_probability_analysis", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def write_analysis_fixture_warehouse(warehouse_path: Path, *, include_kalshi: bool = True) -> None:
    warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(warehouse_path)) as connection:
        connection.execute(
            """
            CREATE TABLE market_catalog (
                venue VARCHAR,
                market_id VARCHAR,
                condition_id VARCHAR,
                question VARCHAR,
                slug VARCHAR,
                market_end_time_utc TIMESTAMP,
                first_observation_time_utc TIMESTAMP,
                last_observation_time_utc TIMESTAMP,
                tick_observation_count BIGINT,
                market_resolved_outcome VARCHAR
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE tick_observations (
                venue VARCHAR,
                market_id VARCHAR,
                condition_id VARCHAR,
                contract_id VARCHAR,
                contract_side VARCHAR,
                observation_time_utc TIMESTAMP,
                market_end_time_utc TIMESTAMP,
                probability DOUBLE,
                price_source VARCHAR,
                source_dataset VARCHAR,
                source_file VARCHAR
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE threshold_entry_events (
                venue VARCHAR,
                market_id VARCHAR,
                contract_id VARCHAR,
                contract_side VARCHAR,
                threshold_bucket VARCHAR,
                entry_time_utc TIMESTAMP,
                probability DOUBLE,
                price_source VARCHAR,
                event_index BIGINT,
                resolved_outcome VARCHAR,
                market_resolved_outcome VARCHAR
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE resolution_outcomes (
                venue VARCHAR,
                market_id VARCHAR,
                contract_id VARCHAR,
                contract_side VARCHAR,
                resolved_outcome VARCHAR,
                market_resolved_outcome VARCHAR,
                resolution_time_utc TIMESTAMP,
                source VARCHAR
            )
            """
        )

        market_rows = [
            (
                "polymarket",
                "pm-low-a",
                "cond-pm-low-a",
                "Polymarket low A",
                "pm-low-a",
                _ts("2026-03-03 00:00:00"),
                _ts("2026-03-02 12:00:00"),
                _ts("2026-03-02 13:00:00"),
                2,
                "NO",
            ),
            (
                "polymarket",
                "pm-low-b",
                "cond-pm-low-b",
                "Polymarket low B",
                "pm-low-b",
                _ts("2026-03-10 00:00:00"),
                _ts("2026-03-02 12:00:00"),
                _ts("2026-03-02 13:00:00"),
                2,
                "NO",
            ),
            (
                "polymarket",
                "pm-high-a",
                "cond-pm-high-a",
                "Polymarket high A",
                "pm-high-a",
                _ts("2026-03-03 00:00:00"),
                _ts("2026-03-02 12:00:00"),
                _ts("2026-03-02 13:00:00"),
                2,
                "YES",
            ),
            (
                "polymarket",
                "pm-high-b",
                "cond-pm-high-b",
                "Polymarket high B",
                "pm-high-b",
                _ts("2026-03-12 00:00:00"),
                _ts("2026-03-02 12:00:00"),
                _ts("2026-03-02 13:00:00"),
                2,
                "NO",
            ),
        ]
        if include_kalshi:
            market_rows.extend(
                [
                    (
                        "kalshi",
                        "ka-low-a",
                        "cond-ka-low-a",
                        "Kalshi low A",
                        "ka-low-a",
                        _ts("2026-03-03 00:00:00"),
                        _ts("2026-03-02 12:00:00"),
                        _ts("2026-03-02 13:00:00"),
                        2,
                        "YES",
                    ),
                    (
                        "kalshi",
                        "ka-low-b",
                        "cond-ka-low-b",
                        "Kalshi low B",
                        "ka-low-b",
                        _ts("2026-03-11 00:00:00"),
                        _ts("2026-03-02 12:00:00"),
                        _ts("2026-03-02 13:00:00"),
                        2,
                        "NO",
                    ),
                    (
                        "kalshi",
                        "ka-high-a",
                        "cond-ka-high-a",
                        "Kalshi high A",
                        "ka-high-a",
                        _ts("2026-03-03 00:00:00"),
                        _ts("2026-03-02 12:00:00"),
                        _ts("2026-03-02 13:00:00"),
                        2,
                        "YES",
                    ),
                    (
                        "kalshi",
                        "ka-high-b",
                        "cond-ka-high-b",
                        "Kalshi high B",
                        "ka-high-b",
                        _ts("2026-03-15 00:00:00"),
                        _ts("2026-03-02 12:00:00"),
                        _ts("2026-03-02 13:00:00"),
                        2,
                        "YES",
                    ),
                ]
            )

        connection.executemany(
            """
            INSERT INTO market_catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            market_rows,
        )

        tick_rows = [
            ("polymarket", "pm-low-a", "cond-pm-low-a", "pm-low-a-yes", "YES", _ts("2026-03-02 12:00:00"), _ts("2026-03-03 00:00:00"), 0.05, "close", "markets", "pm-low-a"),
            ("polymarket", "pm-low-a", "cond-pm-low-a", "pm-low-a-yes", "YES", _ts("2026-03-02 13:00:00"), _ts("2026-03-03 00:00:00"), 0.06, "close", "markets", "pm-low-a"),
            ("polymarket", "pm-low-b", "cond-pm-low-b", "pm-low-b-yes", "YES", _ts("2026-03-02 12:00:00"), _ts("2026-03-10 00:00:00"), 0.08, "close", "markets", "pm-low-b"),
            ("polymarket", "pm-low-b", "cond-pm-low-b", "pm-low-b-yes", "YES", _ts("2026-03-02 13:00:00"), _ts("2026-03-10 00:00:00"), 0.09, "close", "markets", "pm-low-b"),
            ("polymarket", "pm-high-a", "cond-pm-high-a", "pm-high-a-yes", "YES", _ts("2026-03-02 12:00:00"), _ts("2026-03-03 00:00:00"), 0.94, "close", "markets", "pm-high-a"),
            ("polymarket", "pm-high-a", "cond-pm-high-a", "pm-high-a-yes", "YES", _ts("2026-03-02 13:00:00"), _ts("2026-03-03 00:00:00"), 0.95, "close", "markets", "pm-high-a"),
            ("polymarket", "pm-high-b", "cond-pm-high-b", "pm-high-b-yes", "YES", _ts("2026-03-02 12:00:00"), _ts("2026-03-12 00:00:00"), 0.96, "close", "markets", "pm-high-b"),
            ("polymarket", "pm-high-b", "cond-pm-high-b", "pm-high-b-yes", "YES", _ts("2026-03-02 13:00:00"), _ts("2026-03-12 00:00:00"), 0.97, "close", "markets", "pm-high-b"),
        ]
        threshold_rows = [
            ("polymarket", "pm-low-a", "pm-low-a-yes", "YES", "low_probability", _ts("2026-03-02 12:00:00"), 0.05, "close", 1, "NO", "NO"),
            ("polymarket", "pm-low-b", "pm-low-b-yes", "YES", "low_probability", _ts("2026-03-02 12:00:00"), 0.08, "close", 1, "NO", "NO"),
            ("polymarket", "pm-high-a", "pm-high-a-yes", "YES", "high_probability", _ts("2026-03-02 12:00:00"), 0.94, "close", 1, "YES", "YES"),
            ("polymarket", "pm-high-b", "pm-high-b-yes", "YES", "high_probability", _ts("2026-03-02 12:00:00"), 0.96, "close", 1, "NO", "NO"),
        ]
        resolution_rows = [
            ("polymarket", "pm-low-a", "pm-low-a-yes", "YES", "NO", "NO", _ts("2026-03-03 00:00:00"), "fixture"),
            ("polymarket", "pm-low-b", "pm-low-b-yes", "YES", "NO", "NO", _ts("2026-03-10 00:00:00"), "fixture"),
            ("polymarket", "pm-high-a", "pm-high-a-yes", "YES", "YES", "YES", _ts("2026-03-03 00:00:00"), "fixture"),
            ("polymarket", "pm-high-b", "pm-high-b-yes", "YES", "NO", "NO", _ts("2026-03-12 00:00:00"), "fixture"),
        ]

        if include_kalshi:
            tick_rows.extend(
                [
                    ("kalshi", "ka-low-a", "cond-ka-low-a", "ka-low-a-yes", "YES", _ts("2026-03-02 12:00:00"), _ts("2026-03-03 00:00:00"), 0.03, "close", "markets", "ka-low-a"),
                    ("kalshi", "ka-low-a", "cond-ka-low-a", "ka-low-a-yes", "YES", _ts("2026-03-02 13:00:00"), _ts("2026-03-03 00:00:00"), 0.04, "close", "markets", "ka-low-a"),
                    ("kalshi", "ka-low-b", "cond-ka-low-b", "ka-low-b-yes", "YES", _ts("2026-03-02 12:00:00"), _ts("2026-03-11 00:00:00"), 0.07, "close", "markets", "ka-low-b"),
                    ("kalshi", "ka-low-b", "cond-ka-low-b", "ka-low-b-yes", "YES", _ts("2026-03-02 13:00:00"), _ts("2026-03-11 00:00:00"), 0.08, "close", "markets", "ka-low-b"),
                    ("kalshi", "ka-high-a", "cond-ka-high-a", "ka-high-a-yes", "YES", _ts("2026-03-02 12:00:00"), _ts("2026-03-03 00:00:00"), 0.92, "close", "markets", "ka-high-a"),
                    ("kalshi", "ka-high-a", "cond-ka-high-a", "ka-high-a-yes", "YES", _ts("2026-03-02 13:00:00"), _ts("2026-03-03 00:00:00"), 0.93, "close", "markets", "ka-high-a"),
                    ("kalshi", "ka-high-b", "cond-ka-high-b", "ka-high-b-yes", "YES", _ts("2026-03-02 12:00:00"), _ts("2026-03-15 00:00:00"), 0.95, "close", "markets", "ka-high-b"),
                    ("kalshi", "ka-high-b", "cond-ka-high-b", "ka-high-b-yes", "YES", _ts("2026-03-02 13:00:00"), _ts("2026-03-15 00:00:00"), 0.96, "close", "markets", "ka-high-b"),
                ]
            )
            threshold_rows.extend(
                [
                    ("kalshi", "ka-low-a", "ka-low-a-yes", "YES", "low_probability", _ts("2026-03-02 12:00:00"), 0.03, "close", 1, "YES", "YES"),
                    ("kalshi", "ka-low-b", "ka-low-b-yes", "YES", "low_probability", _ts("2026-03-02 12:00:00"), 0.07, "close", 1, "NO", "NO"),
                    ("kalshi", "ka-high-a", "ka-high-a-yes", "YES", "high_probability", _ts("2026-03-02 12:00:00"), 0.92, "close", 1, "YES", "YES"),
                    ("kalshi", "ka-high-b", "ka-high-b-yes", "YES", "high_probability", _ts("2026-03-02 12:00:00"), 0.95, "close", 1, "YES", "YES"),
                ]
            )
            resolution_rows.extend(
                [
                    ("kalshi", "ka-low-a", "ka-low-a-yes", "YES", "YES", "YES", _ts("2026-03-03 00:00:00"), "fixture"),
                    ("kalshi", "ka-low-b", "ka-low-b-yes", "YES", "NO", "NO", _ts("2026-03-11 00:00:00"), "fixture"),
                    ("kalshi", "ka-high-a", "ka-high-a-yes", "YES", "YES", "YES", _ts("2026-03-03 00:00:00"), "fixture"),
                    ("kalshi", "ka-high-b", "ka-high-b-yes", "YES", "YES", "YES", _ts("2026-03-15 00:00:00"), "fixture"),
                ]
            )

        connection.executemany(
            """
            INSERT INTO tick_observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            tick_rows,
        )
        connection.executemany(
            """
            INSERT INTO threshold_entry_events VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            threshold_rows,
        )
        connection.executemany(
            """
            INSERT INTO resolution_outcomes VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            resolution_rows,
        )


def _ts(value: str):
    return duckdb.execute(f"SELECT TIMESTAMP '{value}'").fetchone()[0]
