# Architecture

## Architectural Goal

Support a research-first Polymarket workflow that can validate data availability, collect reusable datasets, and evaluate signal quality before any live execution is attempted.

The architecture should stay local-first and minimal until the research scope proves there is a real edge worth operationalizing.

## Scope Boundaries

### Implemented Now

- canonical project documents
- minimal folder scaffold for current-scope Python modules
- connection-verification notebook scaffold

### Current Build Target

- endpoint validation notebook
- thin API clients
- raw and normalized local storage
- historical and live data collection
- market-signal and wallet-feature pipelines
- dataset generation and offline evaluation

### Deferred Until Later Milestones

- always-on online scoring service
- paper trading journal and alert delivery
- authenticated order placement
- live risk controls and deployment automation

## Repository Layout

Only the directories needed for the current scope are required now.

```text
.
├─ PROJECT_SPEC.md
├─ ARCHITECTURE.md
├─ TASKS.md
├─ notebooks/
│  └─ polymarket_connection_checks/
│     └─ 00_api_connection.ipynb
├─ src/
│  ├─ clients/
│  ├─ ingestion/
│  ├─ storage/
│  ├─ signals/
│  └─ research/
└─ tests/
```

Deferred directories such as `src/execution/` or `src/paper_trading/` should only be added when their milestone begins.

## Component Responsibilities

| Path | Responsibility | Notes |
| --- | --- | --- |
| `notebooks/polymarket_connection_checks/` | Exploratory endpoint validation and payload inspection | Must remain a sandbox for discovery, not a production dependency |
| `src/clients/` | Thin wrappers around Polymarket API surfaces | No business logic; only request construction, retries, parsing, and auth handling where needed |
| `src/ingestion/` | Backfill and stream collection orchestration | Owns idempotent collection jobs and source-to-storage normalization |
| `src/storage/` | Local persistence, schemas, and read helpers | Owns raw payload layout and normalized analytical tables |
| `src/signals/` | Market anomaly features, wallet features, and event extraction | Only uses observable data and time-safe wallet metrics |
| `src/research/` | Dataset generation, labeling, offline evaluation, and reports | Contains leakage-safe research code and baseline backtests |
| `tests/` | Unit and integration tests | Prefer fixtures captured from verified payloads |

## Data Flow

1. The connection notebook verifies endpoint reachability, auth requirements, message structure, and key identifiers.
2. Thin clients in `src/clients/` wrap each API surface and return normalized Python objects or validated dictionaries.
3. Collectors in `src/ingestion/` write raw payloads to disk and upsert normalized records into the analytical store.
4. Feature logic in `src/signals/` computes market anomaly metrics and wallet-quality metrics as of event time.
5. Research code in `src/research/` joins features into event datasets, assigns future labels, and runs offline evaluation.
6. Later milestones reuse the same data model for real-time scoring, paper trading, and eventually live execution.

## Storage Design

Use a simple local analytical stack first:
- raw API payload archive on disk for reproducibility and schema evolution
- DuckDB as the default local analytical database
- optional Parquet extracts for notebook-friendly intermediate datasets

### Raw Storage

Proposed layout:

```text
data/
├─ raw/
│  ├─ gamma/
│  ├─ clob/
│  ├─ websocket/
│  └─ data_api/
└─ warehouse/
   └─ polymarket.duckdb
```

Raw storage rules:
- store payloads with source, request parameters, and collection timestamp
- keep raw records append-only
- prefer JSONL for list-like payload capture
- never overwrite raw data silently

### Normalized Tables

Initial normalized tables:
- `markets`
- `market_tokens`
- `price_history`
- `trades`
- `wallet_positions`
- `wallet_closed_positions`
- `wallet_profiles`
- `signal_events`
- `event_dataset_rows`

Forward-only tables for later milestones:
- `order_book_snapshots`
- `paper_trades`
- `live_orders`
- `risk_events`

### Primary Join Keys

- `market_id`
- `token_id`
- `wallet_address`
- `event_time_utc`
- `collection_time_utc`

All timestamps should be normalized to UTC before storage.

## Module Design Principles

- Keep API clients thin and testable.
- Keep ingestion idempotent and restart-safe.
- Separate raw collection from feature computation.
- Keep wallet metrics time-aware to avoid leakage.
- Make research outputs reproducible from raw or normalized data.
- Favor explicit schemas over ad hoc notebook transformations once a field is confirmed useful.

## External Dependencies

Expected Python dependencies for the current scope:
- `httpx` for REST clients
- `websockets` for streaming
- `pydantic` or `pydantic-settings` for configuration
- `duckdb` for local analytics
- `polars` or `pandas` for transforms
- `jupyter` for connection and research notebooks
- `pytest` for tests

External services:
- Polymarket Gamma API
- Polymarket CLOB API
- Polymarket WebSocket feeds
- Polymarket Data API

No separate database server, queue, or scheduler is required in the current scope.

## Reliability and Observability

Minimum expectations for current-scope code:
- structured logging for collection jobs
- explicit retry and timeout behavior in clients
- persisted sample payloads for debugging parsing changes
- ingestion checkpoints or idempotent upserts
- clear error reporting on missing fields or failed endpoint checks

Metrics and dashboards can wait until paper-trading work begins.

## Security and Secrets

Current scope:
- public data collection should work without secrets
- any future trading credentials must live in local environment configuration, never in notebooks or committed files

Later scope:
- authenticated CLOB credentials
- wallet signing support
- separate risk and execution permissions

## Now Versus Later

| Status | Item | Reason |
| --- | --- | --- |
| Now | connection verification notebook | We must confirm data reality before implementation assumptions spread through the codebase |
| Now | thin clients, collectors, local storage, signal research | These are required to answer whether the strategy is viable |
| Later | paper trading loop | It depends on stable signal scoring and live collection |
| Later | authenticated execution | It should only be added after research and paper results justify the added risk |
