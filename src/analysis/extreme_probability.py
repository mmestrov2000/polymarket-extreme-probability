from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
import math
import random
from pathlib import Path

import duckdb


EXPECTED_VENUES = ("polymarket", "kalshi")
SAMPLING_VIEWS = ("all_ticks", "threshold_entry")
PROBABILITY_BUCKETS = ("low_probability", "high_probability")
TIME_TO_EXPIRY_BUCKETS = ("under_24h", "one_to_seven_days", "over_seven_days", "unknown")
LOW_PROBABILITY_THRESHOLD = 0.10
HIGH_PROBABILITY_THRESHOLD = 0.90
WILSON_Z_95 = 1.959963984540054
SMALL_MARKET_SAMPLE_THRESHOLD = 5
SMALL_OBSERVATION_SAMPLE_THRESHOLD = 30


@dataclass(frozen=True, slots=True)
class CalibrationSummaryRow:
    sampling_view: str
    venue: str
    probability_bucket: str
    observation_count: int
    market_count: int
    contract_count: int
    average_quoted_probability: float
    empirical_yes_rate: float
    calibration_gap: float
    wilson_interval_lower: float
    wilson_interval_upper: float
    bootstrap_yes_rate_lower: float | None
    bootstrap_yes_rate_upper: float | None
    bootstrap_gap_lower: float | None
    bootstrap_gap_upper: float | None
    sample_caveat: str | None

    def to_tuple(self) -> tuple[object, ...]:
        return (
            self.sampling_view,
            self.venue,
            self.probability_bucket,
            self.observation_count,
            self.market_count,
            self.contract_count,
            self.average_quoted_probability,
            self.empirical_yes_rate,
            self.calibration_gap,
            self.wilson_interval_lower,
            self.wilson_interval_upper,
            self.bootstrap_yes_rate_lower,
            self.bootstrap_yes_rate_upper,
            self.bootstrap_gap_lower,
            self.bootstrap_gap_upper,
            self.sample_caveat,
        )


@dataclass(frozen=True, slots=True)
class CalibrationSegmentRow:
    sampling_view: str
    venue: str
    segment_name: str
    segment_value: str
    probability_bucket: str
    observation_count: int
    market_count: int
    contract_count: int
    average_quoted_probability: float
    empirical_yes_rate: float
    calibration_gap: float
    wilson_interval_lower: float
    wilson_interval_upper: float
    bootstrap_yes_rate_lower: float | None
    bootstrap_yes_rate_upper: float | None
    bootstrap_gap_lower: float | None
    bootstrap_gap_upper: float | None
    sample_caveat: str | None

    def to_tuple(self) -> tuple[object, ...]:
        return (
            self.sampling_view,
            self.venue,
            self.segment_name,
            self.segment_value,
            self.probability_bucket,
            self.observation_count,
            self.market_count,
            self.contract_count,
            self.average_quoted_probability,
            self.empirical_yes_rate,
            self.calibration_gap,
            self.wilson_interval_lower,
            self.wilson_interval_upper,
            self.bootstrap_yes_rate_lower,
            self.bootstrap_yes_rate_upper,
            self.bootstrap_gap_lower,
            self.bootstrap_gap_upper,
            self.sample_caveat,
        )


@dataclass(frozen=True, slots=True)
class CalibrationSensitivityRow:
    venue: str
    probability_bucket: str
    tick_observation_count: int
    threshold_event_count: int
    tick_market_count: int
    threshold_market_count: int
    tick_calibration_gap: float
    threshold_calibration_gap: float
    gap_difference: float
    directional_consistency: bool
    stability_note: str

    def to_tuple(self) -> tuple[object, ...]:
        return (
            self.venue,
            self.probability_bucket,
            self.tick_observation_count,
            self.threshold_event_count,
            self.tick_market_count,
            self.threshold_market_count,
            self.tick_calibration_gap,
            self.threshold_calibration_gap,
            self.gap_difference,
            self.directional_consistency,
            self.stability_note,
        )


@dataclass(frozen=True, slots=True)
class AnalysisTableCounts:
    calibration_summaries: int
    calibration_segments: int
    calibration_sensitivity: int

    def to_dict(self) -> dict[str, int]:
        return {
            "calibration_summaries": self.calibration_summaries,
            "calibration_segments": self.calibration_segments,
            "calibration_sensitivity": self.calibration_sensitivity,
        }


@dataclass(frozen=True, slots=True)
class ExtremeProbabilityAnalysisResult:
    warehouse_path: str
    counts: AnalysisTableCounts
    present_venues: tuple[str, ...]
    missing_expected_venues: tuple[str, ...]
    summary_rows: tuple[CalibrationSummaryRow, ...]
    segment_rows: tuple[CalibrationSegmentRow, ...]
    sensitivity_rows: tuple[CalibrationSensitivityRow, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "warehouse_path": self.warehouse_path,
            "counts": self.counts.to_dict(),
            "present_venues": list(self.present_venues),
            "missing_expected_venues": list(self.missing_expected_venues),
        }


@dataclass(frozen=True, slots=True)
class _Observation:
    sampling_view: str
    venue: str
    market_id: str
    contract_id: str
    probability_bucket: str
    probability: float
    resolved_yes: int
    time_to_expiry_bucket: str


@dataclass(frozen=True, slots=True)
class _BootstrapInterval:
    yes_rate_lower: float | None
    yes_rate_upper: float | None
    gap_lower: float | None
    gap_upper: float | None


def build_extreme_probability_analysis(
    warehouse_path: Path,
    *,
    bootstrap_samples: int = 400,
    random_seed: int = 17,
    expected_venues: tuple[str, ...] = EXPECTED_VENUES,
) -> ExtremeProbabilityAnalysisResult:
    warehouse_path = Path(warehouse_path)
    _ensure_required_tables(warehouse_path)

    with duckdb.connect(str(warehouse_path)) as connection:
        observations = _load_extreme_observations(connection)
        if not observations:
            raise ValueError(
                "No extreme-probability observations were found in the warehouse. "
                "Build the canonical dataset first."
            )

        summary_rows = tuple(
            _build_summary_rows(observations, bootstrap_samples=bootstrap_samples, random_seed=random_seed)
        )
        segment_rows = tuple(
            _build_segment_rows(observations, bootstrap_samples=bootstrap_samples, random_seed=random_seed)
        )
        sensitivity_rows = tuple(_build_sensitivity_rows(summary_rows))

        connection.execute("BEGIN")
        try:
            _replace_table(connection, "calibration_summaries", _create_calibration_summaries_table_sql())
            _replace_table(connection, "calibration_segments", _create_calibration_segments_table_sql())
            _replace_table(connection, "calibration_sensitivity", _create_calibration_sensitivity_table_sql())

            connection.executemany(
                """
                INSERT INTO calibration_summaries (
                    sampling_view,
                    venue,
                    probability_bucket,
                    observation_count,
                    market_count,
                    contract_count,
                    average_quoted_probability,
                    empirical_yes_rate,
                    calibration_gap,
                    wilson_interval_lower,
                    wilson_interval_upper,
                    bootstrap_yes_rate_lower,
                    bootstrap_yes_rate_upper,
                    bootstrap_gap_lower,
                    bootstrap_gap_upper,
                    sample_caveat
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [row.to_tuple() for row in summary_rows],
            )
            connection.executemany(
                """
                INSERT INTO calibration_segments (
                    sampling_view,
                    venue,
                    segment_name,
                    segment_value,
                    probability_bucket,
                    observation_count,
                    market_count,
                    contract_count,
                    average_quoted_probability,
                    empirical_yes_rate,
                    calibration_gap,
                    wilson_interval_lower,
                    wilson_interval_upper,
                    bootstrap_yes_rate_lower,
                    bootstrap_yes_rate_upper,
                    bootstrap_gap_lower,
                    bootstrap_gap_upper,
                    sample_caveat
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [row.to_tuple() for row in segment_rows],
            )
            connection.executemany(
                """
                INSERT INTO calibration_sensitivity (
                    venue,
                    probability_bucket,
                    tick_observation_count,
                    threshold_event_count,
                    tick_market_count,
                    threshold_market_count,
                    tick_calibration_gap,
                    threshold_calibration_gap,
                    gap_difference,
                    directional_consistency,
                    stability_note
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [row.to_tuple() for row in sensitivity_rows],
            )
        except Exception:
            connection.execute("ROLLBACK")
            raise
        else:
            connection.execute("COMMIT")

    present_venues = tuple(sorted({row.venue for row in summary_rows if row.venue != "combined"}))
    missing_expected_venues = tuple(
        venue for venue in expected_venues if venue not in present_venues
    )
    return ExtremeProbabilityAnalysisResult(
        warehouse_path=warehouse_path.as_posix(),
        counts=AnalysisTableCounts(
            calibration_summaries=len(summary_rows),
            calibration_segments=len(segment_rows),
            calibration_sensitivity=len(sensitivity_rows),
        ),
        present_venues=present_venues,
        missing_expected_venues=missing_expected_venues,
        summary_rows=summary_rows,
        segment_rows=segment_rows,
        sensitivity_rows=sensitivity_rows,
    )


def render_analysis_report_markdown(result: ExtremeProbabilityAnalysisResult) -> str:
    lines = [
        "# Milestone 3 Statistical Analysis",
        "",
        "Primary inference uses `threshold_entry` market-aware events. "
        "`all_ticks` remains a descriptive view to show how repeated ticks change the headline calibration gaps.",
        "",
    ]
    if result.missing_expected_venues:
        present = ", ".join(result.present_venues) or "none"
        missing = ", ".join(result.missing_expected_venues)
        lines.append(
            f"Coverage note: the warehouse currently contains `{present}` data. "
            f"Expected venue data still missing from this local archive: `{missing}`."
        )
        lines.append("")

    lines.append("## Extreme-bucket calibration")
    lines.append("")
    for sampling_view in SAMPLING_VIEWS:
        rows = [row for row in result.summary_rows if row.sampling_view == sampling_view]
        lines.append(f"### {_sampling_view_label(sampling_view)}")
        lines.append("")
        lines.extend(
            _render_markdown_table(
                headers=(
                    "Venue",
                    "Bucket",
                    "Obs",
                    "Markets",
                    "Quoted",
                    "YES rate",
                    "Gap",
                    "Wilson 95%",
                    "Bootstrap gap 95%",
                    "Caveat",
                ),
                rows=[
                    (
                        _title_case_identifier(row.venue),
                        _bucket_label(row.probability_bucket),
                        str(row.observation_count),
                        str(row.market_count),
                        _format_percent(row.average_quoted_probability),
                        _format_percent(row.empirical_yes_rate),
                        _format_gap(row.calibration_gap),
                        _format_interval(row.wilson_interval_lower, row.wilson_interval_upper),
                        _format_interval(row.bootstrap_gap_lower, row.bootstrap_gap_upper, as_gap=True),
                        row.sample_caveat or "-",
                    )
                    for row in rows
                ],
            )
        )
        lines.append("")

    lines.append("## Sensitivity checks")
    lines.append("")
    lines.extend(
        _render_markdown_table(
            headers=("Venue", "Bucket", "All-tick gap", "Threshold gap", "Delta", "Direction stable", "Note"),
            rows=[
                (
                    _title_case_identifier(row.venue),
                    _bucket_label(row.probability_bucket),
                    _format_gap(row.tick_calibration_gap),
                    _format_gap(row.threshold_calibration_gap),
                    _format_gap(row.gap_difference),
                    "yes" if row.directional_consistency else "no",
                    row.stability_note,
                )
                for row in result.sensitivity_rows
            ],
        )
    )
    lines.append("")

    lines.append("## Segmentation by time to expiry")
    lines.append("")
    segment_rows = [
        row
        for row in result.segment_rows
        if row.segment_name == "time_to_expiry" and row.sampling_view == "threshold_entry"
    ]
    lines.extend(
        _render_markdown_table(
            headers=("Venue", "Bucket", "Time to expiry", "Events", "Markets", "Quoted", "YES rate", "Gap", "Caveat"),
            rows=[
                (
                    _title_case_identifier(row.venue),
                    _bucket_label(row.probability_bucket),
                    _segment_value_label(row.segment_value),
                    str(row.observation_count),
                    str(row.market_count),
                    _format_percent(row.average_quoted_probability),
                    _format_percent(row.empirical_yes_rate),
                    _format_gap(row.calibration_gap),
                    row.sample_caveat or "-",
                )
                for row in segment_rows
            ],
        )
    )
    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _ensure_required_tables(warehouse_path: Path) -> None:
    if not warehouse_path.exists():
        raise FileNotFoundError(f"Warehouse does not exist: {warehouse_path.as_posix()}")

    required_tables = {
        "market_catalog",
        "resolution_outcomes",
        "threshold_entry_events",
        "tick_observations",
    }
    with duckdb.connect(str(warehouse_path), read_only=True) as connection:
        rows = connection.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            """
        ).fetchall()
    available_tables = {row[0] for row in rows}
    missing_tables = sorted(required_tables - available_tables)
    if missing_tables:
        missing_list = ", ".join(missing_tables)
        raise ValueError(
            "Warehouse is missing canonical tables required for statistical analysis: "
            f"{missing_list}"
        )


def _load_extreme_observations(connection: duckdb.DuckDBPyConnection) -> list[_Observation]:
    rows = connection.execute(
        """
        SELECT
            'all_ticks' AS sampling_view,
            tick.venue,
            tick.market_id,
            tick.contract_id,
            tick.observation_time_utc AS sample_time_utc,
            tick.market_end_time_utc,
            CAST(tick.probability AS DOUBLE) AS probability,
            CASE resolution.resolved_outcome WHEN 'YES' THEN 1 ELSE 0 END AS resolved_yes
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
            event.contract_id,
            event.entry_time_utc AS sample_time_utc,
            market.market_end_time_utc,
            CAST(event.probability AS DOUBLE) AS probability,
            CASE event.resolved_outcome WHEN 'YES' THEN 1 ELSE 0 END AS resolved_yes
        FROM threshold_entry_events AS event
        INNER JOIN market_catalog AS market
            ON market.venue = event.venue
           AND market.market_id = event.market_id
        WHERE event.contract_side = 'YES'
        ORDER BY sampling_view, venue, market_id, contract_id, sample_time_utc
        """
    ).fetchall()

    observations: list[_Observation] = []
    for row in rows:
        probability_bucket = _classify_probability_bucket(row[6])
        if probability_bucket is None:
            continue
        observations.append(
            _Observation(
                sampling_view=row[0],
                venue=row[1],
                market_id=row[2],
                contract_id=row[3],
                probability_bucket=probability_bucket,
                probability=float(row[6]),
                resolved_yes=int(row[7]),
                time_to_expiry_bucket=_time_to_expiry_bucket(row[4], row[5]),
            )
        )
    return observations


def _build_summary_rows(
    observations: list[_Observation],
    *,
    bootstrap_samples: int,
    random_seed: int,
) -> list[CalibrationSummaryRow]:
    grouped: dict[tuple[str, str, str], list[_Observation]] = defaultdict(list)
    for observation in observations:
        for venue in ("combined", observation.venue):
            grouped[(observation.sampling_view, venue, observation.probability_bucket)].append(observation)

    rows = []
    for key in sorted(grouped, key=_summary_sort_key):
        sampling_view, venue, probability_bucket = key
        metrics = _summarize_observations(
            grouped[key],
            bootstrap_samples=bootstrap_samples,
            random_seed=random_seed,
            seed_label=(sampling_view, venue, probability_bucket, "overview"),
        )
        rows.append(
            CalibrationSummaryRow(
                sampling_view=sampling_view,
                venue=venue,
                probability_bucket=probability_bucket,
                observation_count=metrics["observation_count"],
                market_count=metrics["market_count"],
                contract_count=metrics["contract_count"],
                average_quoted_probability=metrics["average_quoted_probability"],
                empirical_yes_rate=metrics["empirical_yes_rate"],
                calibration_gap=metrics["calibration_gap"],
                wilson_interval_lower=metrics["wilson_interval_lower"],
                wilson_interval_upper=metrics["wilson_interval_upper"],
                bootstrap_yes_rate_lower=metrics["bootstrap_yes_rate_lower"],
                bootstrap_yes_rate_upper=metrics["bootstrap_yes_rate_upper"],
                bootstrap_gap_lower=metrics["bootstrap_gap_lower"],
                bootstrap_gap_upper=metrics["bootstrap_gap_upper"],
                sample_caveat=metrics["sample_caveat"],
            )
        )
    return rows


def _build_segment_rows(
    observations: list[_Observation],
    *,
    bootstrap_samples: int,
    random_seed: int,
) -> list[CalibrationSegmentRow]:
    grouped: dict[tuple[str, str, str, str], list[_Observation]] = defaultdict(list)
    for observation in observations:
        for venue in ("combined", observation.venue):
            grouped[
                (
                    observation.sampling_view,
                    venue,
                    observation.probability_bucket,
                    observation.time_to_expiry_bucket,
                )
            ].append(observation)

    rows = []
    for key in sorted(grouped, key=_segment_sort_key):
        sampling_view, venue, probability_bucket, segment_value = key
        metrics = _summarize_observations(
            grouped[key],
            bootstrap_samples=bootstrap_samples,
            random_seed=random_seed,
            seed_label=(sampling_view, venue, probability_bucket, "time_to_expiry", segment_value),
        )
        rows.append(
            CalibrationSegmentRow(
                sampling_view=sampling_view,
                venue=venue,
                segment_name="time_to_expiry",
                segment_value=segment_value,
                probability_bucket=probability_bucket,
                observation_count=metrics["observation_count"],
                market_count=metrics["market_count"],
                contract_count=metrics["contract_count"],
                average_quoted_probability=metrics["average_quoted_probability"],
                empirical_yes_rate=metrics["empirical_yes_rate"],
                calibration_gap=metrics["calibration_gap"],
                wilson_interval_lower=metrics["wilson_interval_lower"],
                wilson_interval_upper=metrics["wilson_interval_upper"],
                bootstrap_yes_rate_lower=metrics["bootstrap_yes_rate_lower"],
                bootstrap_yes_rate_upper=metrics["bootstrap_yes_rate_upper"],
                bootstrap_gap_lower=metrics["bootstrap_gap_lower"],
                bootstrap_gap_upper=metrics["bootstrap_gap_upper"],
                sample_caveat=metrics["sample_caveat"],
            )
        )
    return rows


def _build_sensitivity_rows(
    summary_rows: tuple[CalibrationSummaryRow, ...]
) -> list[CalibrationSensitivityRow]:
    lookup = {
        (row.venue, row.probability_bucket, row.sampling_view): row for row in summary_rows
    }
    rows = []
    venues = sorted({row.venue for row in summary_rows}, key=_venue_sort_value)
    for venue in venues:
        for probability_bucket in PROBABILITY_BUCKETS:
            tick_row = lookup.get((venue, probability_bucket, "all_ticks"))
            threshold_row = lookup.get((venue, probability_bucket, "threshold_entry"))
            if tick_row is None or threshold_row is None:
                continue

            directional_consistency = _directional_consistency(
                tick_row.calibration_gap, threshold_row.calibration_gap
            )
            stability_reasons = []
            if not directional_consistency:
                stability_reasons.append("direction flips between descriptive and market-aware views")
            if _interval_spans_zero(tick_row.bootstrap_gap_lower, tick_row.bootstrap_gap_upper) or _interval_spans_zero(
                threshold_row.bootstrap_gap_lower, threshold_row.bootstrap_gap_upper
            ):
                stability_reasons.append("at least one bootstrap gap interval spans zero")
            gap_difference = threshold_row.calibration_gap - tick_row.calibration_gap
            if abs(gap_difference) >= 0.05:
                stability_reasons.append("gap magnitude shifts by at least 5 percentage points")

            stability_note = (
                "; ".join(stability_reasons)
                if stability_reasons
                else "direction and magnitude are broadly stable across sampling views"
            )
            rows.append(
                CalibrationSensitivityRow(
                    venue=venue,
                    probability_bucket=probability_bucket,
                    tick_observation_count=tick_row.observation_count,
                    threshold_event_count=threshold_row.observation_count,
                    tick_market_count=tick_row.market_count,
                    threshold_market_count=threshold_row.market_count,
                    tick_calibration_gap=tick_row.calibration_gap,
                    threshold_calibration_gap=threshold_row.calibration_gap,
                    gap_difference=gap_difference,
                    directional_consistency=directional_consistency,
                    stability_note=stability_note,
                )
            )
    return rows


def _summarize_observations(
    observations: list[_Observation],
    *,
    bootstrap_samples: int,
    random_seed: int,
    seed_label: tuple[object, ...],
) -> dict[str, object]:
    observation_count = len(observations)
    market_count = len({observation.market_id for observation in observations})
    contract_count = len({observation.contract_id for observation in observations})
    total_probability = sum(observation.probability for observation in observations)
    total_yes = sum(observation.resolved_yes for observation in observations)
    average_quoted_probability = total_probability / observation_count
    empirical_yes_rate = total_yes / observation_count
    calibration_gap = empirical_yes_rate - average_quoted_probability
    wilson_interval_lower, wilson_interval_upper = _wilson_interval(total_yes, observation_count)

    bootstrap_interval = _market_clustered_bootstrap(
        observations,
        iterations=bootstrap_samples,
        seed=_seed_for(random_seed, *seed_label),
    )
    sample_caveat = _build_sample_caveat(
        observation_count=observation_count,
        market_count=market_count,
        bootstrap_interval=bootstrap_interval,
    )
    return {
        "observation_count": observation_count,
        "market_count": market_count,
        "contract_count": contract_count,
        "average_quoted_probability": average_quoted_probability,
        "empirical_yes_rate": empirical_yes_rate,
        "calibration_gap": calibration_gap,
        "wilson_interval_lower": wilson_interval_lower,
        "wilson_interval_upper": wilson_interval_upper,
        "bootstrap_yes_rate_lower": bootstrap_interval.yes_rate_lower,
        "bootstrap_yes_rate_upper": bootstrap_interval.yes_rate_upper,
        "bootstrap_gap_lower": bootstrap_interval.gap_lower,
        "bootstrap_gap_upper": bootstrap_interval.gap_upper,
        "sample_caveat": sample_caveat,
    }


def _market_clustered_bootstrap(
    observations: list[_Observation],
    *,
    iterations: int,
    seed: int,
) -> _BootstrapInterval:
    clusters: dict[str, tuple[int, float, int]] = {}
    for observation in observations:
        cluster_size, probability_sum, yes_sum = clusters.get(observation.market_id, (0, 0.0, 0))
        clusters[observation.market_id] = (
            cluster_size + 1,
            probability_sum + observation.probability,
            yes_sum + observation.resolved_yes,
        )

    market_ids = tuple(sorted(clusters))
    if iterations <= 0 or len(market_ids) < 2:
        return _BootstrapInterval(None, None, None, None)

    rng = random.Random(seed)
    yes_rates: list[float] = []
    gaps: list[float] = []
    for _ in range(iterations):
        total_observations = 0
        total_probability = 0.0
        total_yes = 0
        for _ in market_ids:
            market_id = market_ids[rng.randrange(len(market_ids))]
            cluster_size, probability_sum, yes_sum = clusters[market_id]
            total_observations += cluster_size
            total_probability += probability_sum
            total_yes += yes_sum
        yes_rate = total_yes / total_observations
        average_probability = total_probability / total_observations
        yes_rates.append(yes_rate)
        gaps.append(yes_rate - average_probability)

    return _BootstrapInterval(
        yes_rate_lower=_quantile(yes_rates, 0.025),
        yes_rate_upper=_quantile(yes_rates, 0.975),
        gap_lower=_quantile(gaps, 0.025),
        gap_upper=_quantile(gaps, 0.975),
    )


def _build_sample_caveat(
    *,
    observation_count: int,
    market_count: int,
    bootstrap_interval: _BootstrapInterval,
) -> str | None:
    caveats = []
    if market_count < SMALL_MARKET_SAMPLE_THRESHOLD:
        caveats.append(f"small market sample ({market_count} markets)")
    if observation_count < SMALL_OBSERVATION_SAMPLE_THRESHOLD:
        caveats.append(f"small observation sample ({observation_count} rows)")
    if bootstrap_interval.gap_lower is None or bootstrap_interval.gap_upper is None:
        caveats.append("market-clustered bootstrap unavailable")
    elif _interval_spans_zero(bootstrap_interval.gap_lower, bootstrap_interval.gap_upper):
        caveats.append("market-clustered bootstrap gap spans zero")
    return "; ".join(caveats) if caveats else None


def _wilson_interval(successes: int | float, trials: int) -> tuple[float, float]:
    if trials <= 0:
        return (0.0, 0.0)

    proportion = successes / trials
    z_squared = WILSON_Z_95**2
    denominator = 1 + z_squared / trials
    center = (proportion + z_squared / (2 * trials)) / denominator
    margin = (
        WILSON_Z_95
        * math.sqrt((proportion * (1 - proportion) + z_squared / (4 * trials)) / trials)
        / denominator
    )
    return (max(0.0, center - margin), min(1.0, center + margin))


def _replace_table(
    connection: duckdb.DuckDBPyConnection, table_name: str, create_table_sql: str
) -> None:
    connection.execute(f"DROP TABLE IF EXISTS {table_name}")
    connection.execute(create_table_sql)


def _create_calibration_summaries_table_sql() -> str:
    return """
        CREATE TABLE calibration_summaries (
            sampling_view VARCHAR NOT NULL,
            venue VARCHAR NOT NULL,
            probability_bucket VARCHAR NOT NULL,
            observation_count BIGINT NOT NULL,
            market_count BIGINT NOT NULL,
            contract_count BIGINT NOT NULL,
            average_quoted_probability DOUBLE NOT NULL,
            empirical_yes_rate DOUBLE NOT NULL,
            calibration_gap DOUBLE NOT NULL,
            wilson_interval_lower DOUBLE NOT NULL,
            wilson_interval_upper DOUBLE NOT NULL,
            bootstrap_yes_rate_lower DOUBLE,
            bootstrap_yes_rate_upper DOUBLE,
            bootstrap_gap_lower DOUBLE,
            bootstrap_gap_upper DOUBLE,
            sample_caveat VARCHAR
        )
    """


def _create_calibration_segments_table_sql() -> str:
    return """
        CREATE TABLE calibration_segments (
            sampling_view VARCHAR NOT NULL,
            venue VARCHAR NOT NULL,
            segment_name VARCHAR NOT NULL,
            segment_value VARCHAR NOT NULL,
            probability_bucket VARCHAR NOT NULL,
            observation_count BIGINT NOT NULL,
            market_count BIGINT NOT NULL,
            contract_count BIGINT NOT NULL,
            average_quoted_probability DOUBLE NOT NULL,
            empirical_yes_rate DOUBLE NOT NULL,
            calibration_gap DOUBLE NOT NULL,
            wilson_interval_lower DOUBLE NOT NULL,
            wilson_interval_upper DOUBLE NOT NULL,
            bootstrap_yes_rate_lower DOUBLE,
            bootstrap_yes_rate_upper DOUBLE,
            bootstrap_gap_lower DOUBLE,
            bootstrap_gap_upper DOUBLE,
            sample_caveat VARCHAR
        )
    """


def _create_calibration_sensitivity_table_sql() -> str:
    return """
        CREATE TABLE calibration_sensitivity (
            venue VARCHAR NOT NULL,
            probability_bucket VARCHAR NOT NULL,
            tick_observation_count BIGINT NOT NULL,
            threshold_event_count BIGINT NOT NULL,
            tick_market_count BIGINT NOT NULL,
            threshold_market_count BIGINT NOT NULL,
            tick_calibration_gap DOUBLE NOT NULL,
            threshold_calibration_gap DOUBLE NOT NULL,
            gap_difference DOUBLE NOT NULL,
            directional_consistency BOOLEAN NOT NULL,
            stability_note VARCHAR NOT NULL
        )
    """


def _summary_sort_key(item: tuple[str, str, str]) -> tuple[int, int, int]:
    sampling_view, venue, probability_bucket = item
    return (
        SAMPLING_VIEWS.index(sampling_view),
        _venue_sort_value(venue),
        PROBABILITY_BUCKETS.index(probability_bucket),
    )


def _segment_sort_key(item: tuple[str, str, str, str]) -> tuple[int, int, int, int]:
    sampling_view, venue, probability_bucket, segment_value = item
    return (
        SAMPLING_VIEWS.index(sampling_view),
        _venue_sort_value(venue),
        PROBABILITY_BUCKETS.index(probability_bucket),
        TIME_TO_EXPIRY_BUCKETS.index(segment_value),
    )


def _venue_sort_value(venue: str) -> int:
    if venue == "combined":
        return 0
    if venue in EXPECTED_VENUES:
        return EXPECTED_VENUES.index(venue) + 1
    return len(EXPECTED_VENUES) + 1


def _classify_probability_bucket(probability: float) -> str | None:
    if probability < LOW_PROBABILITY_THRESHOLD:
        return "low_probability"
    if probability > HIGH_PROBABILITY_THRESHOLD:
        return "high_probability"
    return None


def _time_to_expiry_bucket(
    sample_time_utc: datetime | None, market_end_time_utc: datetime | None
) -> str:
    if sample_time_utc is None or market_end_time_utc is None:
        return "unknown"

    hours = (market_end_time_utc - sample_time_utc).total_seconds() / 3600
    if hours < 0:
        return "unknown"
    if hours < 24:
        return "under_24h"
    if hours <= 24 * 7:
        return "one_to_seven_days"
    return "over_seven_days"


def _seed_for(base_seed: int, *parts: object) -> int:
    payload = "|".join(str(part) for part in (base_seed, *parts))
    digest = sha256(payload.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


def _quantile(values: list[float], probability: float) -> float:
    if not values:
        raise ValueError("Expected at least one value to compute a quantile.")

    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]

    position = (len(ordered) - 1) * probability
    lower_index = math.floor(position)
    upper_index = math.ceil(position)
    lower_value = ordered[lower_index]
    upper_value = ordered[upper_index]
    if lower_index == upper_index:
        return lower_value
    fraction = position - lower_index
    return lower_value + (upper_value - lower_value) * fraction


def _interval_spans_zero(lower: float | None, upper: float | None) -> bool:
    if lower is None or upper is None:
        return False
    return lower <= 0 <= upper


def _directional_consistency(left: float, right: float) -> bool:
    return _gap_sign(left) == _gap_sign(right)


def _gap_sign(value: float) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _render_markdown_table(
    *,
    headers: tuple[str, ...],
    rows: list[tuple[str, ...]],
) -> list[str]:
    if not rows:
        return ["No rows available.", ""]

    header_line = "| " + " | ".join(headers) + " |"
    divider_line = "| " + " | ".join("---" for _ in headers) + " |"
    body_lines = ["| " + " | ".join(row) + " |" for row in rows]
    return [header_line, divider_line, *body_lines]


def _sampling_view_label(sampling_view: str) -> str:
    return {
        "all_ticks": "All ticks (descriptive)",
        "threshold_entry": "Threshold-entry events (primary inference)",
    }[sampling_view]


def _bucket_label(probability_bucket: str) -> str:
    return {
        "low_probability": "<10%",
        "high_probability": ">90%",
    }[probability_bucket]


def _segment_value_label(segment_value: str) -> str:
    return {
        "under_24h": "<24h",
        "one_to_seven_days": "1-7d",
        "over_seven_days": ">7d",
        "unknown": "unknown",
    }[segment_value]


def _title_case_identifier(value: str) -> str:
    return value.replace("_", " ").title()


def _format_percent(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value * 100:.1f}%"


def _format_gap(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value * 100:+.1f} pp"


def _format_interval(lower: float | None, upper: float | None, *, as_gap: bool = False) -> str:
    if lower is None or upper is None:
        return "-"
    if as_gap:
        return f"[{_format_gap(lower)}, {_format_gap(upper)}]"
    return f"[{_format_percent(lower)}, {_format_percent(upper)}]"
