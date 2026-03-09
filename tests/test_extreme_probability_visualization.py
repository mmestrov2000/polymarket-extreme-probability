from __future__ import annotations

from pathlib import Path

from src.analysis import build_extreme_probability_analysis
from src.visualization import write_milestone4_artifacts
from tests.helpers_extreme_probability_analysis import (
    load_reporting_script_module,
    write_analysis_fixture_warehouse,
)
from tests.helpers_polymarket_canonical import write_canonical_fixture


def test_write_milestone4_artifacts_writes_venue_figures_cross_venue_figure_and_memo(
    tmp_path: Path,
) -> None:
    warehouse_path = tmp_path / "warehouse" / "extreme_probability.duckdb"
    figures_dir = tmp_path / "reports" / "figures"
    memo_path = tmp_path / "reports" / "summaries" / "milestone4.md"
    write_analysis_fixture_warehouse(warehouse_path)

    analysis_result = build_extreme_probability_analysis(
        warehouse_path,
        bootstrap_samples=120,
        random_seed=5,
    )
    artifact_result = write_milestone4_artifacts(
        warehouse_path,
        analysis_result,
        figures_dir=figures_dir,
        memo_path=memo_path,
    )

    assert artifact_result.present_venues == ("kalshi", "polymarket")
    assert artifact_result.missing_expected_venues == ()
    assert artifact_result.recommendation == "stop"
    assert figures_dir.joinpath("polymarket_bucketed_calibration.svg").exists() is True
    assert figures_dir.joinpath("kalshi_bucketed_calibration.svg").exists() is True
    assert figures_dir.joinpath("cross_venue_gap_comparison.svg").exists() is True
    assert memo_path.exists() is True

    polymarket_svg = figures_dir.joinpath("polymarket_bucketed_calibration.svg").read_text(encoding="utf-8")
    cross_venue_svg = figures_dir.joinpath("cross_venue_gap_comparison.svg").read_text(encoding="utf-8")
    memo = memo_path.read_text(encoding="utf-8")

    assert "<svg" in polymarket_svg
    assert "Polymarket extreme-probability calibration" in polymarket_svg
    assert "Threshold-entry calibration" in polymarket_svg
    assert "Kalshi" in cross_venue_svg
    assert "Cross-venue extreme-bucket comparison" in cross_venue_svg
    assert "Low-probability overvaluation" in memo
    assert "High-probability undervaluation" in memo
    assert "Recommendation: `stop`" in memo


def test_write_milestone4_artifacts_marks_missing_expected_venue_in_outputs(tmp_path: Path) -> None:
    warehouse_path = tmp_path / "warehouse" / "extreme_probability.duckdb"
    figures_dir = tmp_path / "reports" / "figures"
    memo_path = tmp_path / "reports" / "summaries" / "milestone4.md"
    write_analysis_fixture_warehouse(warehouse_path, include_kalshi=False)

    analysis_result = build_extreme_probability_analysis(
        warehouse_path,
        bootstrap_samples=120,
        random_seed=7,
    )
    artifact_result = write_milestone4_artifacts(
        warehouse_path,
        analysis_result,
        figures_dir=figures_dir,
        memo_path=memo_path,
    )

    assert artifact_result.present_venues == ("polymarket",)
    assert artifact_result.missing_expected_venues == ("kalshi",)
    assert artifact_result.figure_paths == (
        figures_dir.joinpath("polymarket_bucketed_calibration.svg").as_posix(),
        figures_dir.joinpath("cross_venue_gap_comparison.svg").as_posix(),
    )

    cross_venue_svg = figures_dir.joinpath("cross_venue_gap_comparison.svg").read_text(encoding="utf-8")
    memo = memo_path.read_text(encoding="utf-8")

    assert "Kalshi archive missing" in cross_venue_svg
    assert "Missing expected venue coverage: `kalshi`." in memo
    assert "Recommendation: `stop`" in memo


def test_build_extreme_probability_reporting_script_builds_artifacts(
    tmp_path: Path, capsys
) -> None:
    module = load_reporting_script_module()
    raw_dir = tmp_path / "raw"
    warehouse_path = tmp_path / "warehouse" / "extreme_probability.duckdb"
    figures_dir = tmp_path / "reports" / "figures"
    memo_path = tmp_path / "reports" / "summaries" / "milestone4.md"
    write_canonical_fixture(raw_dir)

    exit_code = module.main(
        [
            "--raw-dir",
            str(raw_dir),
            "--warehouse-path",
            str(warehouse_path),
            "--figures-dir",
            str(figures_dir),
            "--memo-path",
            str(memo_path),
            "--bootstrap-samples",
            "100",
            "--random-seed",
            "19",
        ]
    )
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Canonical dataset built for reporting" in captured.out
    assert "Milestone 4 reporting written" in captured.out
    assert "Recommendation: stop" in captured.out
    assert figures_dir.joinpath("polymarket_bucketed_calibration.svg").exists() is True
    assert figures_dir.joinpath("cross_venue_gap_comparison.svg").exists() is True
    assert memo_path.exists() is True
