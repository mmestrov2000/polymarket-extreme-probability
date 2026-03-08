# Polymarket Whale Signals Bot

## Project Overview

Build a Python-based, research-first Polymarket bot that starts with data validation and signal research, then graduates to backtesting, paper trading, and only later controlled autonomous trading.

The core problem is not "how do we place trades?" The first problem is "what Polymarket data actually exists, how reliable is it, and is it sufficient to support a profitable strategy?" The repository must answer that question before any live execution work is treated as in scope.

Primary user:
- the maintainer or researcher building and validating the strategy

Desired outcome:
- a reproducible local research platform that can collect Polymarket data, detect candidate whale-driven signals, evaluate them historically, and eventually support paper trading and guarded live trading

## Strategy Concept

The strategy combines two information sources.

### 1. Market Signals

Detect abnormal market behavior that may indicate informed flow using observable market data only.

Candidate market features:
- volume spike versus rolling baseline
- order flow imbalance
- price impact efficiency
- temporal clustering of trades
- z-score deviation from baseline activity
- order book liquidity shifts
- sudden open interest changes

### 2. Whale Wallet Analysis

When a market anomaly is detected, identify which wallets were active and estimate whether they are historically worth following.

Candidate wallet features:
- historical win rate
- ROI and realized profitability
- category-specific profitability
- average trade size
- position holding time
- past prediction accuracy
- wallet clustering behavior

### 3. Combined Strategy

A signal becomes actionable only when both conditions are met:

- `market_signal_score` is high
- `wallet_quality_score` is high

The bot must not blindly copy large traders. It should follow a scored decision rule:

`expected_edge_after_costs = f(market_signal_score, wallet_quality_score, entry_costs, liquidity_context)`

Only signals with positive estimated edge after costs should progress to paper trading or live execution.

## Development Principles

- Validate data availability before building strategy logic.
- Build only what is needed for the current milestone.
- Keep every feature tied to a confirmed data source.
- Prefer simple, interpretable baselines before complex models.
- Separate exploratory validation from production code.
- Treat forward-collected microstructure data as more trustworthy than assumed historical depth reconstruction.
- Measure profitability net of fees, spread, slippage, and entry delay.

## Current Scope

In scope now:
- verify Polymarket connectivity and payload shapes
- confirm which public and authenticated endpoints are actually usable
- collect market, trade, and wallet data into a reproducible local store
- engineer initial market and wallet features
- generate event-level research datasets
- run baseline historical studies and leakage-safe backtests

Out of scope now:
- full autonomous live trading
- large capital deployment
- complex on-chain wallet attribution as a hard dependency
- strategies that require perfect historical level-2 order book reconstruction

## Data Sources

The following sources are expected to matter, but each must be verified in Milestone 1 before it is relied upon.

| Source | Expected use | Data we expect | Key questions to verify |
| --- | --- | --- | --- |
| Gamma API | Market universe discovery | Market metadata, event metadata, outcomes, categories, timestamps, status | Which identifiers are stable across APIs? What pagination and rate limits apply? |
| CLOB API | Market microstructure and prices | Order book snapshots, trades, price history, token-level market state | Which endpoints are public? What historical granularity exists? Is wallet attribution available on trade records? |
| WebSocket feeds | Live collection | Live market updates, trade flow, order book changes | Which channels are available? How are reconnects, sequence gaps, and heartbeats handled? |
| Data API | Wallet and concentration research | Positions, holders, trade history, closed positions, open interest, wallet-level activity | Which wallet fields are present and complete? Are historical closed positions accessible and time-aligned? |
| Authenticated trading endpoints | Later execution work | Signed requests, order placement, order status, balances | What credential flow is required and which environments support safe testing? |

## Features and Signals

### Market-Level Features

- rolling volume z-score
- rolling trade-count z-score
- buy versus sell flow imbalance
- average trade size versus baseline
- price move over 1m, 5m, 15m, and 60m windows
- realized volatility before and after the trigger
- price impact per unit notional
- spread and top-of-book depth for forward-collected data
- liquidity depletion and refill metrics for forward-collected data
- open-interest changes where available

### Wallet-Level Features

- realized PnL
- ROI
- hit rate
- profit by category
- average holding period
- average trade size
- recent activity recency
- market specialization
- sample size and reliability score
- measured post-trade drift in the wallet's direction

### Combined Event Features

Each event row should capture:
- market state at trigger time
- recent price and trade features
- participating-wallet summary statistics
- concentration metrics for the wallets behind the move
- aggregate wallet-quality score
- estimated entry cost and liquidity context
- future outcome labels for continuation, reversal, and net trade PnL

## System Capabilities

The system must eventually support the following capabilities, but only the current milestone should be implemented at any point in time.

Near-term capabilities:
- connection verification notebook with saved sample payloads
- thin Python clients for Gamma, CLOB, WebSocket, and Data API access
- repeatable raw data collection for chosen markets and wallets
- normalized local storage for markets, trades, prices, and wallet metrics
- event detection from market data
- wallet scoring from time-aware historical data
- event dataset generation for offline research
- baseline backtesting with cost assumptions

Later capabilities:
- real-time signal scoring
- paper trading
- authenticated order placement
- live risk controls and limited autonomous trading

## Milestones

### Milestone 0 - Project Bootstrap
- finalize spec, architecture, and tasks
- create the minimal research-first folder scaffold
- prepare project configuration and environment template

### Milestone 1 - Polymarket API Connectivity
- verify every planned API surface through a dedicated notebook
- save sample payloads and write down missing fields, auth requirements, and caveats

### Milestone 2 - Market Data Collection
- implement thin clients and raw storage
- backfill sample markets for metadata, prices, trades, and forward live snapshots

### Milestone 3 - Whale Wallet Data Collection
- define wallet universe rules
- collect wallet history and build wallet profiles

### Milestone 4 - Signal Feature Engineering
- compute market anomaly features
- compute wallet-quality features
- emit event records with explanation payloads

### Milestone 5 - Dataset Generation
- label historical events
- build the training and evaluation dataset
- verify no obvious leakage

### Milestone 6 - Signal Classifier Training
- compare rule-based baselines with simple statistical or tree-based models

### Milestone 7 - Backtesting Engine
- run walk-forward tests with realistic cost assumptions

### Milestone 8 - Paper Trading
- score live signals and paper-trade them end to end

### Milestone 9 - Autonomous Trading
- add authenticated execution, hard risk controls, and limited live deployment

## Risks and Constraints

### Data Risks

- Some Polymarket APIs may not expose all fields needed to attribute trades to wallets.
- Historical depth data may be incomplete or unavailable for proper order book reconstruction.
- Endpoint behavior, rate limits, or field naming may differ from documentation.
- WebSocket streams may require reconnect handling and gap detection before they are trustworthy.

### Strategy Risks

- Whale-like activity may not be predictive after costs.
- Wallet performance may be category-specific, unstable, or sample-size dependent.
- Apparent edge may come from data leakage or labeling mistakes.
- Fast price moves may be impossible to follow with acceptable slippage.

### Operational Risks

- Authenticated trading requires secure credential handling and wallet signing.
- Incorrect market resolution assumptions can corrupt labels and PnL calculations.
- Local collectors can silently fail without persistence, logging, and idempotency checks.

### Constraints

- Use Python.
- Keep architecture modular and local-first.
- Do not add components that are not justified by the current milestone.
- Separate research notebooks from reusable production code.

## Definition of MVP

The MVP for this repository is a research platform, not a live trader.

The MVP is complete when all of the following are true:
- Polymarket public connectivity is verified in `notebooks/polymarket_connection_checks/00_api_connection.ipynb`
- sample payloads exist for the core endpoints and their useful fields are documented
- a small sample of markets can be backfilled reproducibly for metadata, prices, and trades
- a wallet profile table can be generated from confirmed wallet-related endpoints
- a first event dataset can be built that joins market features and wallet features
- a baseline report compares market-only signals against combined market-plus-wallet signals
- no real-money execution is required

## Open Research Questions

- Which endpoint or combination of endpoints reliably exposes wallet identities on trade activity?
- How much historical price and trade data can be backfilled without gaps?
- Can order-book-liquidity features be computed historically, or only from forward collection?
- Which market categories produce stable wallet specialization signals?
- Is any observed edge robust after fees, spread, slippage, and delayed entry?

## Test Strategy

Unit test scope:
- payload normalization
- feature calculations
- wallet score computation
- label generation

Integration test scope:
- client request and parsing behavior against saved fixtures
- ingestion idempotency
- dataset joins and time-aware feature snapshots

Research validation scope:
- notebook smoke run for connectivity checks
- deterministic dataset builds on fixture data
- walk-forward backtest outputs with documented assumptions
