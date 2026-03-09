# Polymarket Milestone 1 Inventory and Schema Mapping

## Scope
- Venue: `polymarket`
- Raw root: `data/raw`
- Extracted root: `data/raw/data/polymarket`
- First-pass analysis source: `markets` snapshot parquet files
- Deferred record types: `trades`, `legacy_trades`

## Inventory
| Dataset | Status | Files | Size | Partitions | Access Pattern | Decision |
| --- | --- | ---: | ---: | --- | --- | --- |
| markets | included | 41 | 101.61 MiB | 0-408863 | `data/polymarket/markets/*` | Use market snapshots as the v1 source of priced-probability observations. |
| trades | deferred | 40454 | 44.89 GiB | 0-404540000 | `data/polymarket/trades/*` | Inventory only for Milestone 1; trade price derivation and contract-side mapping are deferred. |
| legacy_trades | deferred | 221 | 210.78 MiB | 0-2210000 | `data/polymarket/legacy_trades/*` | Inventory only for Milestone 1; legacy FPMM trade normalization is deferred. |
| blocks | excluded | 785 | 841.63 MiB | 4000000-82468431 | `data/polymarket/blocks/*` | Ignore for the first-pass study because no analysis-ready snapshot records are present. |
| lookup_json | auxiliary | 1 | 1.57 MiB | n/a | `data/polymarket/fpmm_collateral_lookup.json` | Keep as a collateral lookup reference but do not use it as the priced-probability source. |

## Ignored Noise
- `.DS_Store`
- `data/.DS_Store`
- `data/polymarket/.DS_Store`

## Schema Snapshots
### `markets`
- Sample file: `data/polymarket/markets/markets_0_10000.parquet`
- Missing required columns: none

| Column | DuckDB Type |
| --- | --- |
| `id` | `VARCHAR` |
| `condition_id` | `VARCHAR` |
| `question` | `VARCHAR` |
| `slug` | `VARCHAR` |
| `outcomes` | `VARCHAR` |
| `outcome_prices` | `VARCHAR` |
| `clob_token_ids` | `VARCHAR` |
| `volume` | `DOUBLE` |
| `liquidity` | `DOUBLE` |
| `active` | `BOOLEAN` |
| `closed` | `BOOLEAN` |
| `end_date` | `TIMESTAMP WITH TIME ZONE` |
| `created_at` | `TIMESTAMP WITH TIME ZONE` |
| `market_maker_address` | `VARCHAR` |
| `_fetched_at` | `TIMESTAMP_NS` |

### `trades`
- Sample file: `data/polymarket/trades/trades_0_10000.parquet`
- Missing required columns: none

| Column | DuckDB Type |
| --- | --- |
| `block_number` | `BIGINT` |
| `transaction_hash` | `VARCHAR` |
| `log_index` | `BIGINT` |
| `order_hash` | `VARCHAR` |
| `maker` | `VARCHAR` |
| `taker` | `VARCHAR` |
| `maker_asset_id` | `VARCHAR` |
| `taker_asset_id` | `VARCHAR` |
| `maker_amount` | `BIGINT` |
| `taker_amount` | `BIGINT` |
| `fee` | `BIGINT` |
| `timestamp` | `INTEGER` |
| `_fetched_at` | `TIMESTAMP_NS` |
| `_contract` | `VARCHAR` |

### `legacy_trades`
- Sample file: `data/polymarket/legacy_trades/trades_0_10000.parquet`
- Missing required columns: none

| Column | DuckDB Type |
| --- | --- |
| `block_number` | `BIGINT` |
| `transaction_hash` | `VARCHAR` |
| `log_index` | `BIGINT` |
| `fpmm_address` | `VARCHAR` |
| `trader` | `VARCHAR` |
| `amount` | `VARCHAR` |
| `fee_amount` | `VARCHAR` |
| `outcome_index` | `BIGINT` |
| `outcome_tokens` | `VARCHAR` |
| `is_buy` | `BOOLEAN` |
| `timestamp` | `INTEGER` |
| `_fetched_at` | `TIMESTAMP_NS` |

## Canonical Mapping
| Canonical Field | Source Field | Note |
| --- | --- | --- |
| `market_id` | `markets.id` | Primary market snapshot identifier for Polymarket. |
| `condition_id` | `markets.condition_id` | Cross-record market condition identifier for later joins. |
| `question` | `markets.question` | Human-readable market prompt. |
| `slug` | `markets.slug` | Stable URL-like market slug. |
| `observation_time_utc` | `markets._fetched_at` | Snapshot collection time for priced-probability observations. |
| `market_end_time_utc` | `markets.end_date` | Scheduled market end timestamp. |
| `yes_outcome_label` | `markets.outcomes[yes_index]` | YES label after strict binary label normalization. |
| `no_outcome_label` | `markets.outcomes[no_index]` | NO label after strict binary label normalization. |
| `yes_contract_id` | `markets.clob_token_ids[yes_index]` | CLOB token aligned to the YES outcome. |
| `no_contract_id` | `markets.clob_token_ids[no_index]` | CLOB token aligned to the NO outcome. |

## Inclusion Rules
- Include only Polymarket market snapshot rows with exactly two outcomes.
- Normalize outcomes strictly to YES and NO labels before building canonical contract fields.
- Require `clob_token_ids` to contain exactly two token ids aligned with the binary outcomes.
- Use the YES-side element of `outcome_prices` as the priced probability at `_fetched_at`.
- Use `1 - no_price` only when the YES-side value is not mappable and the NO-side value is valid.

## Exclusion Rules
- Exclude non-binary markets and rows with more than two outcomes.
- Exclude rows with ambiguous outcome labels that do not normalize cleanly to YES/NO.
- Exclude rows with missing token alignment in `clob_token_ids`.
- Exclude rows whose candidate prices cannot be normalized to the `[0, 1]` interval.
- Exclude unresolved markets or closed snapshots whose YES/NO prices do not collapse to `1/0` or `0/1`.

## Probability and Resolution Decisions
- Priced probability: use `markets.outcome_prices[yes_index]` at `_fetched_at`.
- Fallback: `1 - markets.outcome_prices[no_index]`.
- Normalization: Probabilities must be in the inclusive `[0, 1]` interval.
- Extreme buckets: low `< 0.10`, high `> 0.90`.
- Resolution: Use the terminal `closed=true` snapshot where YES/NO prices collapse to `1/0` or `0/1`.
