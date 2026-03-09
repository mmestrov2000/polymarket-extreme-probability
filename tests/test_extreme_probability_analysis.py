from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.analysis import build_extreme_probability_analysis, render_analysis_report_markdown
from tests.helpers_extreme_probability_analysis import (
    load_analysis_script_module,
    write_analysis_fixture_warehouse,
)
from tests.helpers_polymarket_canonical import write_canonical_fixture


def test_build_extreme_probability_analysis_persists_summary_tables(tmp_path: Path) -> None:
    warehouse_path = tmp_path / "warehouse" / "extreme_probability.duckdb"
    write_analysis_fixture_warehouse(warehouse_path)

    result = build_extreme_probability_analysis(
        warehouse_path,
        bootstrap_samples=200,
        random_seed=11,
    )

    assert result.counts.calibration_summaries == 12
    assert result.counts.calibration_segments == 24
    assert result.counts.calibration_sensitivity == 6
    assert result.present_venues == ("kalshi", "polymarket")
    assert result.missing_expected_venues == ()

    polymarket_low_ticks = next(
        row
        for row in result.summary_rows
        if row.sampling_view == "all_ticks"
        and row.venue == "polymarket"
        and row.probability_bucket == "low_probability"
    )
    assert polymarket_low_ticks.observation_count == 4
    assert polymarket_low_ticks.market_count == 2
    assert polymarket_low_ticks.average_quoted_probability == pytest.approx(0.07)
    assert polymarket_low_ticks.empirical_yes_rate == pytest.approx(0.0)
    assert polymarket_low_ticks.calibration_gap == pytest.approx(-0.07)
    assert polymarket_low_ticks.bootstrap_gap_lower is not None
    assert polymarket_low_ticks.bootstrap_gap_upper is not None
    assert "small market sample" in (polymarket_low_ticks.sample_caveat or "")

    combined_low_threshold = next(
        row
        for row in result.summary_rows
        if row.sampling_view == "threshold_entry"
        and row.venue == "combined"
        and row.probability_bucket == "low_probability"
    )
    assert combined_low_threshold.observation_count == 4
    assert combined_low_threshold.market_count == 4
    assert combined_low_threshold.average_quoted_probability == pytest.approx(0.0575)
    assert combined_low_threshold.empirical_yes_rate == pytest.approx(0.25)
    assert combined_low_threshold.calibration_gap == pytest.approx(0.1925)

    threshold_over_seven_days = next(
        row
        for row in result.segment_rows
        if row.sampling_view == "threshold_entry"
        and row.venue == "polymarket"
        and row.segment_value == "over_seven_days"
        and row.probability_bucket == "high_probability"
    )
    assert threshold_over_seven_days.observation_count == 1
    assert threshold_over_seven_days.market_count == 1
    assert threshold_over_seven_days.calibration_gap == pytest.approx(-0.96)
    assert "small observation sample" in (threshold_over_seven_days.sample_caveat or "")

    kalshi_high_sensitivity = next(
        row
        for row in result.sensitivity_rows
        if row.venue == "kalshi" and row.probability_bucket == "high_probability"
    )
    assert kalshi_high_sensitivity.directional_consistency is True
    assert "broadly stable" in kalshi_high_sensitivity.stability_note

    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        summary_counts = connection.execute(
            """
            SELECT sampling_view, venue, probability_bucket, observation_count, market_count
            FROM calibration_summaries
            ORDER BY sampling_view, venue, probability_bucket
            """
        ).fetchall()
        assert ("all_ticks", "polymarket", "low_probability", 4, 2) in summary_counts

        sensitivity_rows = connection.execute(
            """
            SELECT venue, probability_bucket, directional_consistency
            FROM calibration_sensitivity
            ORDER BY venue, probability_bucket
            """
        ).fetchall()
        assert ("kalshi", "high_probability", True) in sensitivity_rows


def test_render_analysis_report_markdown_mentions_missing_expected_venues(tmp_path: Path) -> None:
    warehouse_path = tmp_path / "warehouse" / "extreme_probability.duckdb"
    write_analysis_fixture_warehouse(warehouse_path, include_kalshi=False)

    result = build_extreme_probability_analysis(
        warehouse_path,
        bootstrap_samples=100,
        random_seed=3,
    )
    report = render_analysis_report_markdown(result)

    assert result.present_venues == ("polymarket",)
    assert result.missing_expected_venues == ("kalshi",)
    assert "Coverage note" in report
    assert "`kalshi`" in report
    assert "Threshold-entry events (primary inference)" in report
    assert "Segmentation by time to expiry" in report


def test_build_extreme_probability_analysis_script_builds_canonical_dataset_and_report(
    tmp_path: Path, capsys
) -> None:
    module = load_analysis_script_module()
    raw_dir = tmp_path / "raw"
    warehouse_path = tmp_path / "warehouse" / "extreme_probability.duckdb"
    report_path = tmp_path / "reports" / "milestone3.md"
    write_canonical_fixture(raw_dir)

    exit_code = module.main(
        [
            "--raw-dir",
            str(raw_dir),
            "--warehouse-path",
            str(warehouse_path),
            "--report-path",
            str(report_path),
            "--bootstrap-samples",
            "100",
            "--random-seed",
            "19",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Canonical dataset built for analysis" in captured.out
    assert "Milestone 3 statistical analysis written" in captured.out
    assert "calibration_summaries: 8" in captured.out
    assert "calibration_segments: 8" in captured.out
    assert "calibration_sensitivity: 4" in captured.out
    assert "Present venues: polymarket" in captured.out
    assert "Missing expected venues: kalshi" in captured.out
    assert report_path.exists() is True
    assert "Coverage note" in report_path.read_text(encoding="utf-8")
