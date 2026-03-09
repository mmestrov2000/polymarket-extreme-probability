# Milestone 4 Decision Memo

Primary inference continues to use the `threshold_entry` market-aware sample. The statements below summarize the current local archive rather than an external or hand-curated dataset.

## Dataset coverage

- `polymarket`: 139633 resolved markets, 139633 YES-side tick observations, 139633 threshold-entry events, 41 source files, datasets `markets`, price sources `yes_outcome_price`.
- Missing expected venue coverage: `kalshi`.

## Key caveats

- The current local archive is missing expected venue coverage for kalshi, so the cross-venue comparison is only partial.
- The `polymarket` first pass uses market snapshot outcome prices from `markets` rather than trade-derived prices.
- Primary inference caveat: market-clustered bootstrap gap spans zero.
- Sensitivity note: at least one bootstrap gap interval spans zero.

## Conclusion

Low-probability overvaluation: `inconclusive`. Quoted 0.0%, realized YES 0.0%, gap +0.0 pp, bootstrap gap 95% [+0.0 pp, +0.0 pp].
Evidence: The threshold-entry combined gap is +0.0 pp, but the 95% bootstrap gap interval spans zero.

High-probability undervaluation: `inconclusive`. Quoted 100.0%, realized YES 100.0%, gap +0.0 pp, bootstrap gap 95% [+0.0 pp, +0.0 pp].
Evidence: The threshold-entry combined gap is +0.0 pp, but the 95% bootstrap gap interval spans zero.

## Recommendation

Recommendation: `stop`. The current archive does not yet justify a follow-on execution project because the evidence is incomplete, unstable, missing expected venue replication, or some combination of those issues.

## Generated figures

- `reports/figures/polymarket_bucketed_calibration.svg`
- `reports/figures/cross_venue_gap_comparison.svg`
