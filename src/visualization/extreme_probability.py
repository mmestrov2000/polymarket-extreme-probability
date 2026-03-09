from __future__ import annotations

from dataclasses import dataclass
from html import escape
import math
from pathlib import Path

import duckdb

from src.analysis import CalibrationSummaryRow, ExtremeProbabilityAnalysisResult


EXPECTED_VENUES = ("polymarket", "kalshi")
SAMPLING_VIEWS = ("all_ticks", "threshold_entry")
LOW_PROBABILITY_BUCKET = "low_probability"
HIGH_PROBABILITY_BUCKET = "high_probability"


@dataclass(frozen=True, slots=True)
class CalibrationPlotPoint:
    venue: str
    sampling_view: str
    bucket_start: float
    bucket_end: float
    bucket_label: str
    observation_count: int
    market_count: int
    average_quoted_probability: float
    empirical_yes_rate: float
    calibration_gap: float


@dataclass(frozen=True, slots=True)
class VenueCoverageRow:
    venue: str
    source_file_count: int
    market_count: int
    tick_observation_count: int
    threshold_event_count: int
    source_datasets: tuple[str, ...]
    price_sources: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class HypothesisAssessment:
    probability_bucket: str
    status: str
    quoted_probability: float | None
    empirical_yes_rate: float | None
    calibration_gap: float | None
    bootstrap_gap_lower: float | None
    bootstrap_gap_upper: float | None
    observation_count: int
    market_count: int
    rationale: str
    sample_caveat: str | None


@dataclass(frozen=True, slots=True)
class Milestone4ArtifactsResult:
    warehouse_path: str
    figure_paths: tuple[str, ...]
    memo_path: str
    present_venues: tuple[str, ...]
    missing_expected_venues: tuple[str, ...]
    coverage_rows: tuple[VenueCoverageRow, ...]
    low_probability_assessment: HypothesisAssessment
    high_probability_assessment: HypothesisAssessment
    recommendation: str

    def to_dict(self) -> dict[str, object]:
        return {
            "warehouse_path": self.warehouse_path,
            "figure_paths": list(self.figure_paths),
            "memo_path": self.memo_path,
            "present_venues": list(self.present_venues),
            "missing_expected_venues": list(self.missing_expected_venues),
            "recommendation": self.recommendation,
        }


def write_milestone4_artifacts(
    warehouse_path: Path,
    analysis_result: ExtremeProbabilityAnalysisResult,
    *,
    figures_dir: Path,
    memo_path: Path,
    expected_venues: tuple[str, ...] = EXPECTED_VENUES,
) -> Milestone4ArtifactsResult:
    warehouse_path = Path(warehouse_path)
    figures_dir = Path(figures_dir)
    memo_path = Path(memo_path)
    figures_dir.mkdir(parents=True, exist_ok=True)
    memo_path.parent.mkdir(parents=True, exist_ok=True)

    plot_points = _load_bucketed_calibration_points(warehouse_path)
    coverage_rows = _load_venue_coverage_rows(warehouse_path, expected_venues=expected_venues)
    coverage_lookup = {row.venue: row for row in coverage_rows}

    figure_paths: list[str] = []
    for venue in _sorted_venues({point.venue for point in plot_points}, expected_venues=expected_venues):
        venue_points = [point for point in plot_points if point.venue == venue]
        if not venue_points:
            continue
        figure_path = figures_dir / f"{venue}_bucketed_calibration.svg"
        figure_path.write_text(
            _render_venue_calibration_svg(
                venue,
                venue_points,
                coverage_lookup.get(venue),
            ),
            encoding="utf-8",
        )
        figure_paths.append(figure_path.as_posix())

    cross_venue_path = figures_dir / "cross_venue_gap_comparison.svg"
    cross_venue_path.write_text(
        _render_cross_venue_gap_svg(
            analysis_result.summary_rows,
            expected_venues=expected_venues,
            present_venues=analysis_result.present_venues,
        ),
        encoding="utf-8",
    )
    figure_paths.append(cross_venue_path.as_posix())

    low_probability_assessment = _assess_hypothesis(
        analysis_result.summary_rows,
        probability_bucket=LOW_PROBABILITY_BUCKET,
    )
    high_probability_assessment = _assess_hypothesis(
        analysis_result.summary_rows,
        probability_bucket=HIGH_PROBABILITY_BUCKET,
    )
    recommendation = _recommend_next_step(
        analysis_result,
        low_probability_assessment,
        high_probability_assessment,
    )

    memo_path.write_text(
        _render_research_conclusion_memo_markdown(
            analysis_result,
            coverage_rows=coverage_rows,
            figure_paths=tuple(figure_paths),
            low_probability_assessment=low_probability_assessment,
            high_probability_assessment=high_probability_assessment,
            recommendation=recommendation,
        ),
        encoding="utf-8",
    )

    return Milestone4ArtifactsResult(
        warehouse_path=warehouse_path.as_posix(),
        figure_paths=tuple(figure_paths),
        memo_path=memo_path.as_posix(),
        present_venues=analysis_result.present_venues,
        missing_expected_venues=analysis_result.missing_expected_venues,
        coverage_rows=coverage_rows,
        low_probability_assessment=low_probability_assessment,
        high_probability_assessment=high_probability_assessment,
        recommendation=recommendation,
    )


def _load_bucketed_calibration_points(warehouse_path: Path) -> tuple[CalibrationPlotPoint, ...]:
    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        rows = connection.execute(
            """
            WITH observations AS (
                SELECT
                    'all_ticks' AS sampling_view,
                    tick.venue,
                    tick.market_id,
                    CAST(tick.probability AS DOUBLE) AS probability,
                    CASE resolution.resolved_outcome WHEN 'YES' THEN 1.0 ELSE 0.0 END AS resolved_yes
                FROM tick_observations AS tick
                INNER JOIN resolution_outcomes AS resolution
                    ON resolution.venue = tick.venue
                   AND resolution.market_id = tick.market_id
                   AND resolution.contract_id = tick.contract_id
                   AND resolution.contract_side = tick.contract_side
                WHERE tick.contract_side = 'YES'

                UNION ALL

                SELECT
                    'threshold_entry' AS sampling_view,
                    event.venue,
                    event.market_id,
                    CAST(event.probability AS DOUBLE) AS probability,
                    CASE event.resolved_outcome WHEN 'YES' THEN 1.0 ELSE 0.0 END AS resolved_yes
                FROM threshold_entry_events AS event
                WHERE event.contract_side = 'YES'
            ),
            extreme_observations AS (
                SELECT
                    sampling_view,
                    venue,
                    market_id,
                    probability,
                    resolved_yes,
                    CASE
                        WHEN probability < 0.10
                            THEN LEAST(CAST(FLOOR(probability / 0.02) AS INTEGER), 4)
                        WHEN probability > 0.90
                            THEN LEAST(CAST(FLOOR((probability - 0.90) / 0.02) AS INTEGER), 4) + 45
                        ELSE NULL
                    END AS bucket_index
                FROM observations
                WHERE probability < 0.10 OR probability > 0.90
            )
            SELECT
                sampling_view,
                venue,
                CASE
                    WHEN bucket_index < 45 THEN bucket_index * 0.02
                    ELSE 0.90 + (bucket_index - 45) * 0.02
                END AS bucket_start,
                CASE
                    WHEN bucket_index < 45 THEN LEAST(bucket_index * 0.02 + 0.02, 0.10)
                    ELSE LEAST(0.90 + (bucket_index - 45) * 0.02 + 0.02, 1.00)
                END AS bucket_end,
                COUNT(*) AS observation_count,
                COUNT(DISTINCT market_id) AS market_count,
                AVG(probability) AS average_quoted_probability,
                AVG(resolved_yes) AS empirical_yes_rate
            FROM extreme_observations
            WHERE bucket_index IS NOT NULL
            GROUP BY 1, 2, 3, 4
            ORDER BY
                CASE sampling_view WHEN 'all_ticks' THEN 0 ELSE 1 END,
                venue,
                bucket_start
            """
        ).fetchall()

    return tuple(
        CalibrationPlotPoint(
            venue=row[1],
            sampling_view=row[0],
            bucket_start=float(row[2]),
            bucket_end=float(row[3]),
            bucket_label=_format_bucket_range(float(row[2]), float(row[3])),
            observation_count=int(row[4]),
            market_count=int(row[5]),
            average_quoted_probability=float(row[6]),
            empirical_yes_rate=float(row[7]),
            calibration_gap=float(row[7] - row[6]),
        )
        for row in rows
    )


def _load_venue_coverage_rows(
    warehouse_path: Path,
    *,
    expected_venues: tuple[str, ...],
) -> tuple[VenueCoverageRow, ...]:
    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        market_rows = connection.execute(
            """
            SELECT venue, COUNT(*) AS market_count
            FROM market_catalog
            GROUP BY venue
            """
        ).fetchall()
        tick_rows = connection.execute(
            """
            SELECT venue, COUNT(*) AS tick_count, COUNT(DISTINCT source_file) AS source_file_count
            FROM tick_observations
            GROUP BY venue
            """
        ).fetchall()
        threshold_rows = connection.execute(
            """
            SELECT venue, COUNT(*) AS threshold_count
            FROM threshold_entry_events
            GROUP BY venue
            """
        ).fetchall()
        source_dataset_rows = connection.execute(
            """
            SELECT venue, source_dataset
            FROM tick_observations
            GROUP BY venue, source_dataset
            ORDER BY venue, source_dataset
            """
        ).fetchall()
        price_source_rows = connection.execute(
            """
            SELECT venue, price_source
            FROM tick_observations
            GROUP BY venue, price_source
            ORDER BY venue, price_source
            """
        ).fetchall()

    venues = set()
    venues.update(row[0] for row in market_rows)
    venues.update(row[0] for row in tick_rows)
    venues.update(row[0] for row in threshold_rows)

    market_counts = {row[0]: int(row[1]) for row in market_rows}
    tick_counts = {row[0]: int(row[1]) for row in tick_rows}
    source_file_counts = {row[0]: int(row[2]) for row in tick_rows}
    threshold_counts = {row[0]: int(row[1]) for row in threshold_rows}
    source_datasets: dict[str, list[str]] = {}
    price_sources: dict[str, list[str]] = {}
    for venue, source_dataset in source_dataset_rows:
        source_datasets.setdefault(venue, []).append(str(source_dataset))
    for venue, price_source in price_source_rows:
        price_sources.setdefault(venue, []).append(str(price_source))

    rows = []
    for venue in _sorted_venues(venues, expected_venues=expected_venues):
        rows.append(
            VenueCoverageRow(
                venue=venue,
                source_file_count=source_file_counts.get(venue, 0),
                market_count=market_counts.get(venue, 0),
                tick_observation_count=tick_counts.get(venue, 0),
                threshold_event_count=threshold_counts.get(venue, 0),
                source_datasets=tuple(source_datasets.get(venue, [])),
                price_sources=tuple(price_sources.get(venue, [])),
            )
        )
    return tuple(rows)


def _assess_hypothesis(
    summary_rows: tuple[CalibrationSummaryRow, ...],
    *,
    probability_bucket: str,
) -> HypothesisAssessment:
    row = next(
        (
            candidate
            for candidate in summary_rows
            if candidate.sampling_view == "threshold_entry"
            and candidate.venue == "combined"
            and candidate.probability_bucket == probability_bucket
        ),
        None,
    )
    if row is None:
        return HypothesisAssessment(
            probability_bucket=probability_bucket,
            status="inconclusive",
            quoted_probability=None,
            empirical_yes_rate=None,
            calibration_gap=None,
            bootstrap_gap_lower=None,
            bootstrap_gap_upper=None,
            observation_count=0,
            market_count=0,
            rationale="No threshold-entry summary was available for the combined view.",
            sample_caveat="threshold-entry summary missing",
        )

    expected_positive = probability_bucket == HIGH_PROBABILITY_BUCKET
    interval_supports_hypothesis = (
        row.bootstrap_gap_lower is not None and row.bootstrap_gap_lower > 0
        if expected_positive
        else row.bootstrap_gap_upper is not None and row.bootstrap_gap_upper < 0
    )
    interval_supports_opposite = (
        row.bootstrap_gap_upper is not None and row.bootstrap_gap_upper < 0
        if expected_positive
        else row.bootstrap_gap_lower is not None and row.bootstrap_gap_lower > 0
    )
    if interval_supports_hypothesis:
        status = "supported"
    elif interval_supports_opposite:
        status = "unsupported"
    else:
        status = "inconclusive"

    gap_direction = "positive" if row.calibration_gap > 0 else "negative" if row.calibration_gap < 0 else "flat"
    if status == "supported":
        rationale = (
            f"The threshold-entry combined gap is {_format_gap(row.calibration_gap)} and the 95% bootstrap gap "
            f"interval stays on the hypothesized {gap_direction} side of zero."
        )
    elif status == "unsupported":
        rationale = (
            f"The threshold-entry combined gap is {_format_gap(row.calibration_gap)}, which points in the opposite "
            f"direction from the hypothesis."
        )
    else:
        rationale = (
            f"The threshold-entry combined gap is {_format_gap(row.calibration_gap)}, but the 95% bootstrap gap "
            f"interval {_interval_zero_note(row.bootstrap_gap_lower, row.bootstrap_gap_upper)}."
        )

    return HypothesisAssessment(
        probability_bucket=probability_bucket,
        status=status,
        quoted_probability=row.average_quoted_probability,
        empirical_yes_rate=row.empirical_yes_rate,
        calibration_gap=row.calibration_gap,
        bootstrap_gap_lower=row.bootstrap_gap_lower,
        bootstrap_gap_upper=row.bootstrap_gap_upper,
        observation_count=row.observation_count,
        market_count=row.market_count,
        rationale=rationale,
        sample_caveat=row.sample_caveat,
    )


def _recommend_next_step(
    analysis_result: ExtremeProbabilityAnalysisResult,
    low_probability_assessment: HypothesisAssessment,
    high_probability_assessment: HypothesisAssessment,
) -> str:
    if low_probability_assessment.status != "supported":
        return "stop"
    if high_probability_assessment.status != "supported":
        return "stop"
    if analysis_result.missing_expected_venues:
        return "stop"

    unstable_rows = [
        row
        for row in analysis_result.sensitivity_rows
        if row.venue != "combined"
        and (
            not row.directional_consistency
            or "spans zero" in row.stability_note
        )
    ]
    return "stop" if unstable_rows else "create_separate_execution_project"


def _render_research_conclusion_memo_markdown(
    analysis_result: ExtremeProbabilityAnalysisResult,
    *,
    coverage_rows: tuple[VenueCoverageRow, ...],
    figure_paths: tuple[str, ...],
    low_probability_assessment: HypothesisAssessment,
    high_probability_assessment: HypothesisAssessment,
    recommendation: str,
) -> str:
    lines = [
        "# Milestone 4 Decision Memo",
        "",
        "Primary inference continues to use the `threshold_entry` market-aware sample. "
        "The statements below summarize the current local archive rather than an external or hand-curated dataset.",
        "",
        "## Dataset coverage",
        "",
    ]
    for row in coverage_rows:
        source_datasets = ", ".join(f"`{value}`" for value in row.source_datasets) or "none"
        price_sources = ", ".join(f"`{value}`" for value in row.price_sources) or "none"
        lines.append(
            f"- `{row.venue}`: {row.market_count} resolved markets, {row.tick_observation_count} YES-side tick "
            f"observations, {row.threshold_event_count} threshold-entry events, {row.source_file_count} source files, "
            f"datasets {source_datasets}, price sources {price_sources}."
        )
    if analysis_result.missing_expected_venues:
        missing = ", ".join(f"`{venue}`" for venue in analysis_result.missing_expected_venues)
        lines.append(f"- Missing expected venue coverage: {missing}.")
    lines.append("")
    lines.append("## Key caveats")
    lines.append("")
    for caveat in _build_key_caveats(
        analysis_result,
        coverage_rows=coverage_rows,
        low_probability_assessment=low_probability_assessment,
        high_probability_assessment=high_probability_assessment,
    ):
        lines.append(f"- {caveat}")
    lines.append("")
    lines.append("## Conclusion")
    lines.append("")
    lines.append(
        f"Low-probability overvaluation: `{low_probability_assessment.status}`. "
        f"Quoted { _format_percent_or_dash(low_probability_assessment.quoted_probability) }, "
        f"realized YES { _format_percent_or_dash(low_probability_assessment.empirical_yes_rate) }, "
        f"gap { _format_gap_or_dash(low_probability_assessment.calibration_gap) }, "
        f"bootstrap gap 95% { _format_interval(low_probability_assessment.bootstrap_gap_lower, low_probability_assessment.bootstrap_gap_upper, as_gap=True) }."
    )
    lines.append(f"Evidence: {low_probability_assessment.rationale}")
    lines.append("")
    lines.append(
        f"High-probability undervaluation: `{high_probability_assessment.status}`. "
        f"Quoted { _format_percent_or_dash(high_probability_assessment.quoted_probability) }, "
        f"realized YES { _format_percent_or_dash(high_probability_assessment.empirical_yes_rate) }, "
        f"gap { _format_gap_or_dash(high_probability_assessment.calibration_gap) }, "
        f"bootstrap gap 95% { _format_interval(high_probability_assessment.bootstrap_gap_lower, high_probability_assessment.bootstrap_gap_upper, as_gap=True) }."
    )
    lines.append(f"Evidence: {high_probability_assessment.rationale}")
    lines.append("")
    lines.append("## Recommendation")
    lines.append("")
    if recommendation == "create_separate_execution_project":
        lines.append(
            "Recommendation: `create_separate_execution_project`. Both extreme-bucket hypotheses are supported in the "
            "primary inference view, the local archive covers all expected venues, and the sensitivity tables do not "
            "show obvious directional breakdowns."
        )
    else:
        lines.append(
            "Recommendation: `stop`. The current archive does not yet justify a follow-on execution project because "
            "the evidence is incomplete, unstable, missing expected venue replication, or some combination of those "
            "issues."
        )
    lines.append("")
    lines.append("## Generated figures")
    lines.append("")
    for figure_path in figure_paths:
        lines.append(f"- `{figure_path}`")
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _build_key_caveats(
    analysis_result: ExtremeProbabilityAnalysisResult,
    *,
    coverage_rows: tuple[VenueCoverageRow, ...],
    low_probability_assessment: HypothesisAssessment,
    high_probability_assessment: HypothesisAssessment,
) -> tuple[str, ...]:
    caveats: list[str] = []
    if analysis_result.missing_expected_venues:
        missing = ", ".join(analysis_result.missing_expected_venues)
        caveats.append(
            f"The current local archive is missing expected venue coverage for {missing}, so the cross-venue "
            "comparison is only partial."
        )

    for row in coverage_rows:
        if row.source_datasets == ("markets",):
            caveats.append(
                f"The `{row.venue}` first pass uses market snapshot outcome prices from `markets` rather than "
                "trade-derived prices."
            )

    threshold_caveats = {
        assessment.sample_caveat
        for assessment in (low_probability_assessment, high_probability_assessment)
        if assessment.sample_caveat
    }
    for caveat in sorted(threshold_caveats):
        caveats.append(f"Primary inference caveat: {caveat}.")

    stability_notes = {
        row.stability_note
        for row in analysis_result.sensitivity_rows
        if row.venue != "combined" and "stable" not in row.stability_note
    }
    for note in sorted(stability_notes):
        caveats.append(f"Sensitivity note: {note}.")

    return tuple(dict.fromkeys(caveats))


def _render_venue_calibration_svg(
    venue: str,
    plot_points: list[CalibrationPlotPoint],
    coverage_row: VenueCoverageRow | None,
) -> str:
    width = 980
    height = 620
    panel_width = 410
    panel_height = 360
    panel_y = 150
    panel_left = 70
    panel_gap = 20

    body = [
        f'<rect class="page" x="0" y="0" width="{width}" height="{height}" rx="24" />',
        _svg_text(width / 2, 52, f"{_title_case_identifier(venue)} extreme-probability calibration", "title", "middle"),
        _svg_text(
            width / 2,
            80,
            "2 percentage-point buckets below 10% and above 90%; diagonal line marks perfect calibration.",
            "subtitle",
            "middle",
        ),
    ]
    if coverage_row is not None:
        body.append(
            _svg_text(
                width / 2,
                108,
                f"{coverage_row.market_count} resolved markets | {coverage_row.tick_observation_count} ticks | "
                f"{coverage_row.threshold_event_count} threshold-entry events",
                "eyebrow",
                "middle",
            )
        )

    body.append(
        '<text class="axis-label" x="28" y="332" transform="rotate(-90 28 332)">Realized YES rate</text>'
    )
    body.append(_svg_text(width / 2, 596, "Quoted probability", "axis-label", "middle"))

    for index, sampling_view in enumerate(SAMPLING_VIEWS):
        panel_x = panel_left + index * (panel_width + panel_gap)
        panel_points = sorted(
            [point for point in plot_points if point.sampling_view == sampling_view],
            key=lambda point: point.bucket_start,
        )
        body.extend(
            _render_calibration_panel(
                panel_x,
                panel_y,
                panel_width,
                panel_height,
                sampling_view=sampling_view,
                plot_points=panel_points,
            )
        )

    return _wrap_svg_document(width, height, body)


def _render_calibration_panel(
    x: int,
    y: int,
    width: int,
    height: int,
    *,
    sampling_view: str,
    plot_points: list[CalibrationPlotPoint],
) -> list[str]:
    plot_left = x + 54
    plot_top = y + 52
    plot_width = width - 84
    plot_height = height - 112
    plot_bottom = plot_top + plot_height
    plot_right = plot_left + plot_width

    elements = [
        f'<rect class="panel" x="{x}" y="{y}" width="{width}" height="{height}" rx="18" />',
        _svg_text(
            x + width / 2,
            y + 30,
            _sampling_view_label(sampling_view),
            "panel-title",
            "middle",
        ),
        _svg_text(
            x + width / 2,
            y + height - 18,
            "Bucket labels show probability range and rows per point.",
            "note",
            "middle",
        ),
    ]

    for tick in range(5):
        fraction = tick / 4
        grid_x = plot_left + fraction * plot_width
        grid_y = plot_top + fraction * plot_height
        elements.append(
            f'<line class="grid" x1="{grid_x:.2f}" y1="{plot_top:.2f}" x2="{grid_x:.2f}" y2="{plot_bottom:.2f}" />'
        )
        elements.append(
            f'<line class="grid" x1="{plot_left:.2f}" y1="{grid_y:.2f}" x2="{plot_right:.2f}" y2="{grid_y:.2f}" />'
        )
        elements.append(_svg_text(grid_x, plot_bottom + 22, f"{fraction:.0%}", "tick", "middle"))
        elements.append(_svg_text(plot_left - 18, plot_bottom - fraction * plot_height + 4, f"{fraction:.0%}", "tick", "end"))

    elements.append(
        f'<line class="ideal" x1="{plot_left:.2f}" y1="{plot_bottom:.2f}" x2="{plot_right:.2f}" y2="{plot_top:.2f}" />'
    )

    by_side = {
        "low": [point for point in plot_points if point.bucket_start < 0.10],
        "high": [point for point in plot_points if point.bucket_start >= 0.90],
    }
    for side_name, points in by_side.items():
        if len(points) > 1:
            polyline_points = " ".join(
                f"{_scale_probability(point.average_quoted_probability, plot_left, plot_width):.2f},"
                f"{_scale_yes_rate(point.empirical_yes_rate, plot_top, plot_height):.2f}"
                for point in points
            )
            elements.append(
                f'<polyline class="series {side_name}" points="{polyline_points}" />'
            )

        for point in points:
            circle_x = _scale_probability(point.average_quoted_probability, plot_left, plot_width)
            circle_y = _scale_yes_rate(point.empirical_yes_rate, plot_top, plot_height)
            radius = min(10.0, 4.0 + math.sqrt(point.observation_count))
            elements.append(
                f'<circle class="point {side_name}" cx="{circle_x:.2f}" cy="{circle_y:.2f}" r="{radius:.2f}" />'
            )
            label_y = circle_y - 12 if side_name == "low" else circle_y + 18
            anchor = "start" if circle_x <= (plot_left + plot_width / 2) else "end"
            label_x = circle_x + 8 if anchor == "start" else circle_x - 8
            elements.append(
                _svg_text(
                    label_x,
                    label_y,
                    f"{point.bucket_label} | n={point.observation_count}",
                    "point-label",
                    anchor,
                )
            )

    return elements


def _render_cross_venue_gap_svg(
    summary_rows: tuple[CalibrationSummaryRow, ...],
    *,
    expected_venues: tuple[str, ...],
    present_venues: tuple[str, ...],
) -> str:
    width = 980
    height = 560
    panel_width = 410
    panel_height = 290
    panel_y = 176
    panel_left = 70
    panel_gap = 20

    relevant_rows = [
        row
        for row in summary_rows
        if row.venue in expected_venues and row.sampling_view in SAMPLING_VIEWS
    ]
    values = []
    for row in relevant_rows:
        values.append(abs(row.calibration_gap))
        if row.bootstrap_gap_lower is not None:
            values.append(abs(row.bootstrap_gap_lower))
        if row.bootstrap_gap_upper is not None:
            values.append(abs(row.bootstrap_gap_upper))
    limit = max(0.10, _round_up((max(values) if values else 0.10) + 0.02, 0.05))

    body = [
        f'<rect class="page" x="0" y="0" width="{width}" height="{height}" rx="24" />',
        _svg_text(width / 2, 52, "Cross-venue extreme-bucket comparison", "title", "middle"),
        _svg_text(
            width / 2,
            80,
            "Bars show threshold-entry calibration gaps, whiskers show bootstrap gap intervals, and short markers show all-tick gaps.",
            "subtitle",
            "middle",
        ),
        _svg_text(width / 2, 108, "Threshold bars are the primary inference view.", "eyebrow", "middle"),
        '<text class="axis-label" x="28" y="320" transform="rotate(-90 28 320)">Calibration gap</text>',
    ]

    for index, venue in enumerate(expected_venues):
        panel_x = panel_left + index * (panel_width + panel_gap)
        body.extend(
            _render_cross_venue_panel(
                panel_x,
                panel_y,
                panel_width,
                panel_height,
                venue=venue,
                summary_rows=summary_rows,
                venue_present=venue in present_venues,
                limit=limit,
            )
        )

    return _wrap_svg_document(width, height, body)


def _render_cross_venue_panel(
    x: int,
    y: int,
    width: int,
    height: int,
    *,
    venue: str,
    summary_rows: tuple[CalibrationSummaryRow, ...],
    venue_present: bool,
    limit: float,
) -> list[str]:
    plot_left = x + 54
    plot_top = y + 48
    plot_width = width - 84
    plot_height = height - 92
    plot_bottom = plot_top + plot_height
    zero_y = _scale_gap(0.0, plot_top, plot_height, limit)

    elements = [
        f'<rect class="panel" x="{x}" y="{y}" width="{width}" height="{height}" rx="18" />',
        _svg_text(x + width / 2, y + 30, _title_case_identifier(venue), "panel-title", "middle"),
    ]

    if not venue_present:
        elements.extend(
            [
                f'<rect class="placeholder" x="{plot_left:.2f}" y="{plot_top:.2f}" width="{plot_width:.2f}" height="{plot_height:.2f}" rx="14" />',
                _svg_text(x + width / 2, y + height / 2 - 10, f"{_title_case_identifier(venue)} archive missing", "placeholder-text", "middle"),
                _svg_text(x + width / 2, y + height / 2 + 16, "No local warehouse rows were available for this venue.", "note", "middle"),
            ]
        )
        return elements

    tick_labels = (-limit, -limit / 2, 0.0, limit / 2, limit)
    for value in tick_labels:
        grid_y = _scale_gap(value, plot_top, plot_height, limit)
        elements.append(
            f'<line class="grid" x1="{plot_left:.2f}" y1="{grid_y:.2f}" x2="{plot_left + plot_width:.2f}" y2="{grid_y:.2f}" />'
        )
        elements.append(_svg_text(plot_left - 18, grid_y + 4, _format_gap(value), "tick", "end"))

    elements.append(
        f'<line class="baseline" x1="{plot_left:.2f}" y1="{zero_y:.2f}" x2="{plot_left + plot_width:.2f}" y2="{zero_y:.2f}" />'
    )

    bucket_positions = {
        LOW_PROBABILITY_BUCKET: plot_left + plot_width * 0.30,
        HIGH_PROBABILITY_BUCKET: plot_left + plot_width * 0.70,
    }
    for probability_bucket, center_x in bucket_positions.items():
        threshold_row = _lookup_summary_row(
            summary_rows,
            venue=venue,
            probability_bucket=probability_bucket,
            sampling_view="threshold_entry",
        )
        tick_row = _lookup_summary_row(
            summary_rows,
            venue=venue,
            probability_bucket=probability_bucket,
            sampling_view="all_ticks",
        )
        if threshold_row is None:
            continue

        color_class = "low" if probability_bucket == LOW_PROBABILITY_BUCKET else "high"
        bar_width = 72
        bar_y = _scale_gap(threshold_row.calibration_gap, plot_top, plot_height, limit)
        rect_y = min(bar_y, zero_y)
        rect_height = max(abs(zero_y - bar_y), 1.0)
        elements.append(
            f'<rect class="gap-bar {color_class}" x="{center_x - bar_width / 2:.2f}" y="{rect_y:.2f}" '
            f'width="{bar_width:.2f}" height="{rect_height:.2f}" rx="10" />'
        )
        if threshold_row.bootstrap_gap_lower is not None and threshold_row.bootstrap_gap_upper is not None:
            lower_y = _scale_gap(threshold_row.bootstrap_gap_lower, plot_top, plot_height, limit)
            upper_y = _scale_gap(threshold_row.bootstrap_gap_upper, plot_top, plot_height, limit)
            elements.append(
                f'<line class="whisker" x1="{center_x:.2f}" y1="{lower_y:.2f}" x2="{center_x:.2f}" y2="{upper_y:.2f}" />'
            )
            elements.append(
                f'<line class="whisker" x1="{center_x - 12:.2f}" y1="{lower_y:.2f}" x2="{center_x + 12:.2f}" y2="{lower_y:.2f}" />'
            )
            elements.append(
                f'<line class="whisker" x1="{center_x - 12:.2f}" y1="{upper_y:.2f}" x2="{center_x + 12:.2f}" y2="{upper_y:.2f}" />'
            )

        if tick_row is not None:
            tick_y = _scale_gap(tick_row.calibration_gap, plot_top, plot_height, limit)
            elements.append(
                f'<line class="tick-marker" x1="{center_x - 24:.2f}" y1="{tick_y:.2f}" x2="{center_x + 24:.2f}" y2="{tick_y:.2f}" />'
            )

        elements.append(
            _svg_text(
                center_x,
                plot_bottom + 22,
                "Low <10%" if probability_bucket == LOW_PROBABILITY_BUCKET else "High >90%",
                "axis-label",
                "middle",
            )
        )
        elements.append(
            _svg_text(
                center_x,
                plot_bottom + 42,
                f"n={threshold_row.observation_count}",
                "note",
                "middle",
            )
        )
        label_y = rect_y - 10 if threshold_row.calibration_gap >= 0 else rect_y + rect_height + 18
        elements.append(
            _svg_text(
                center_x,
                label_y,
                _format_gap(threshold_row.calibration_gap),
                "point-label",
                "middle",
            )
        )

    return elements


def _lookup_summary_row(
    summary_rows: tuple[CalibrationSummaryRow, ...],
    *,
    venue: str,
    probability_bucket: str,
    sampling_view: str,
) -> CalibrationSummaryRow | None:
    return next(
        (
            row
            for row in summary_rows
            if row.venue == venue
            and row.probability_bucket == probability_bucket
            and row.sampling_view == sampling_view
        ),
        None,
    )


def _wrap_svg_document(width: int, height: int, body: list[str]) -> str:
    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">',
        "<title>Milestone 4 extreme-probability figures</title>",
        "<desc>Bucketed calibration plots and cross-venue comparison for the extreme-probability study.</desc>",
        "<style>",
        "  .page { fill: #f4efe4; stroke: #ded3c3; stroke-width: 1; }",
        "  .panel { fill: #fffaf2; stroke: #d8cab6; stroke-width: 1.2; }",
        "  .placeholder { fill: #f8f2e9; stroke: #c7b79f; stroke-width: 1.2; stroke-dasharray: 8 6; }",
        "  .grid { stroke: #e4dacd; stroke-width: 1; }",
        "  .baseline { stroke: #40342a; stroke-width: 1.4; }",
        "  .ideal { stroke: #92826c; stroke-width: 1.6; stroke-dasharray: 6 6; }",
        "  .series { fill: none; stroke-width: 2.6; stroke-linecap: round; stroke-linejoin: round; }",
        "  .series.low { stroke: #b35c17; }",
        "  .series.high { stroke: #0d6b6b; }",
        "  .point.low { fill: #cf7b2c; stroke: #8c4e14; stroke-width: 1.2; }",
        "  .point.high { fill: #1b8f8f; stroke: #115959; stroke-width: 1.2; }",
        "  .gap-bar.low { fill: #d88a42; }",
        "  .gap-bar.high { fill: #2f9f9f; }",
        "  .whisker { stroke: #2d241d; stroke-width: 1.8; }",
        "  .tick-marker { stroke: #2d241d; stroke-width: 3.2; stroke-linecap: round; }",
        "  .title { font: 700 28px Georgia, 'Times New Roman', serif; fill: #2a2018; }",
        "  .subtitle { font: 15px 'Helvetica Neue', Arial, sans-serif; fill: #5d4f41; }",
        "  .eyebrow { font: 600 12px 'Helvetica Neue', Arial, sans-serif; letter-spacing: 0.08em; text-transform: uppercase; fill: #8a7257; }",
        "  .panel-title { font: 700 18px Georgia, 'Times New Roman', serif; fill: #30261d; }",
        "  .axis-label { font: 600 12px 'Helvetica Neue', Arial, sans-serif; fill: #4b3f33; }",
        "  .tick { font: 11px 'Helvetica Neue', Arial, sans-serif; fill: #6b5d4d; }",
        "  .note { font: 12px 'Helvetica Neue', Arial, sans-serif; fill: #766857; }",
        "  .point-label { font: 600 11px 'Helvetica Neue', Arial, sans-serif; fill: #2f261e; }",
        "  .placeholder-text { font: 700 18px Georgia, 'Times New Roman', serif; fill: #6f5f4a; }",
        "</style>",
    ]
    lines.extend(body)
    lines.append("</svg>")
    return "\n".join(lines) + "\n"


def _sorted_venues(venues: set[str], *, expected_venues: tuple[str, ...]) -> list[str]:
    expected_index = {venue: index for index, venue in enumerate(expected_venues)}
    return sorted(venues, key=lambda venue: (expected_index.get(venue, len(expected_index)), venue))


def _sampling_view_label(sampling_view: str) -> str:
    if sampling_view == "threshold_entry":
        return "Threshold-entry calibration"
    return "All-tick calibration"


def _title_case_identifier(value: str) -> str:
    return value.replace("_", " ").title()


def _format_bucket_range(bucket_start: float, bucket_end: float) -> str:
    start_value = int(round(bucket_start * 100))
    end_value = int(round(bucket_end * 100))
    return f"{start_value}-{end_value}%"


def _format_percent(value: float) -> str:
    return f"{value * 100:.1f}%"


def _format_percent_or_dash(value: float | None) -> str:
    return "-" if value is None else _format_percent(value)


def _format_gap(value: float) -> str:
    return f"{value * 100:+.1f} pp"


def _format_gap_or_dash(value: float | None) -> str:
    return "-" if value is None else _format_gap(value)


def _format_interval(lower: float | None, upper: float | None, *, as_gap: bool) -> str:
    if lower is None or upper is None:
        return "-"
    if as_gap:
        return f"[{_format_gap(lower)}, {_format_gap(upper)}]"
    return f"[{_format_percent(lower)}, {_format_percent(upper)}]"


def _interval_zero_note(lower: float | None, upper: float | None) -> str:
    if lower is None or upper is None:
        return "is unavailable"
    if lower <= 0 <= upper:
        return "spans zero"
    return "stays away from zero"


def _scale_probability(probability: float, plot_left: float, plot_width: float) -> float:
    return plot_left + probability * plot_width


def _scale_yes_rate(yes_rate: float, plot_top: float, plot_height: float) -> float:
    return plot_top + (1 - yes_rate) * plot_height


def _scale_gap(gap: float, plot_top: float, plot_height: float, limit: float) -> float:
    return plot_top + (limit - gap) / (2 * limit) * plot_height


def _round_up(value: float, step: float) -> float:
    return math.ceil(value / step) * step


def _svg_text(x: float, y: float, text: str, class_name: str, anchor: str) -> str:
    return (
        f'<text class="{class_name}" x="{x:.2f}" y="{y:.2f}" text-anchor="{anchor}">'
        f"{escape(text)}</text>"
    )
