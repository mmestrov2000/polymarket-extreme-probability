from __future__ import annotations

from collections.abc import Iterable
from contextlib import suppress
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import duckdb

from src.clients.clob import PriceHistory
from src.clients.data_api import TradeRecord
from src.clients.gamma import GammaMarket


DEFAULT_WAREHOUSE_PATH = Path("data/warehouse/polymarket.duckdb")
DECIMAL_SQL_TYPE = "DECIMAL(38, 18)"


class PolymarketWarehouse:
    """Owns normalized DuckDB tables for market metadata, prices, and trades."""

    def __init__(self, database_path: str | Path = DEFAULT_WAREHOUSE_PATH) -> None:
        self.database_path = Path(database_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = duckdb.connect(str(self.database_path))
        self.ensure_schema()

    def close(self) -> None:
        with suppress(Exception):
            self._connection.close()

    def __enter__(self) -> PolymarketWarehouse:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def ensure_schema(self) -> None:
        self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS markets (
                market_id VARCHAR PRIMARY KEY,
                question VARCHAR,
                slug VARCHAR,
                condition_id VARCHAR,
                active BOOLEAN,
                end_time_utc TIMESTAMP,
                liquidity {DECIMAL_SQL_TYPE},
                volume {DECIMAL_SQL_TYPE},
                source VARCHAR NOT NULL,
                collection_time_utc TIMESTAMP NOT NULL,
                updated_at_utc TIMESTAMP NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS market_tokens (
                market_id VARCHAR NOT NULL,
                token_id VARCHAR NOT NULL,
                token_index INTEGER NOT NULL,
                source VARCHAR NOT NULL,
                collection_time_utc TIMESTAMP NOT NULL
            )
            """
        )
        self._connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS market_tokens_market_id_token_id_idx
            ON market_tokens (market_id, token_id)
            """
        )
        self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS price_history (
                price_id VARCHAR PRIMARY KEY,
                token_id VARCHAR NOT NULL,
                interval VARCHAR NOT NULL,
                fidelity INTEGER NOT NULL,
                price_time_utc TIMESTAMP NOT NULL,
                price {DECIMAL_SQL_TYPE},
                source VARCHAR NOT NULL,
                collection_time_utc TIMESTAMP NOT NULL,
                updated_at_utc TIMESTAMP NOT NULL
            )
            """
        )
        self._connection.execute(
            f"""
            CREATE TABLE IF NOT EXISTS trades (
                trade_id VARCHAR PRIMARY KEY,
                proxy_wallet VARCHAR,
                asset_id VARCHAR,
                condition_id VARCHAR,
                outcome VARCHAR,
                side VARCHAR,
                size {DECIMAL_SQL_TYPE},
                price {DECIMAL_SQL_TYPE},
                transaction_hash VARCHAR,
                usdc_size {DECIMAL_SQL_TYPE},
                trade_time_utc TIMESTAMP,
                source VARCHAR NOT NULL,
                collection_time_utc TIMESTAMP NOT NULL,
                updated_at_utc TIMESTAMP NOT NULL
            )
            """
        )

    def upsert_markets(
        self,
        markets: Iterable[GammaMarket],
        *,
        source: str = "gamma.markets",
        collection_time: datetime | None = None,
    ) -> int:
        collected_at = _normalize_utc_timestamp(collection_time or datetime.now(UTC))
        market_rows: dict[str, tuple[object, ...]] = {}
        token_rows: dict[tuple[str, str], tuple[object, ...]] = {}

        for market in markets:
            if not market.market_id:
                continue

            market_rows[market.market_id] = (
                market.market_id,
                market.question,
                market.slug,
                market.condition_id,
                market.active,
                _normalize_nullable_utc_timestamp(market.end_date),
                market.liquidity,
                market.volume,
                source,
                collected_at,
                collected_at,
            )

            for token_index, token_id in enumerate(market.clob_token_ids):
                if not token_id:
                    continue
                token_rows[(market.market_id, token_id)] = (
                    market.market_id,
                    token_id,
                    token_index,
                    source,
                    collected_at,
                )

        if not market_rows:
            return 0

        market_ids = [(market_id,) for market_id in market_rows]

        self._begin_transaction()
        try:
            self._connection.executemany("DELETE FROM markets WHERE market_id = ?", market_ids)
            self._connection.executemany(
                """
                INSERT INTO markets (
                    market_id,
                    question,
                    slug,
                    condition_id,
                    active,
                    end_time_utc,
                    liquidity,
                    volume,
                    source,
                    collection_time_utc,
                    updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                list(market_rows.values()),
            )
            self._connection.executemany("DELETE FROM market_tokens WHERE market_id = ?", market_ids)
            if token_rows:
                self._connection.executemany(
                    """
                    INSERT INTO market_tokens (
                        market_id,
                        token_id,
                        token_index,
                        source,
                        collection_time_utc
                    )
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    list(token_rows.values()),
                )
            self._commit_transaction()
        except Exception:
            self._rollback_transaction()
            raise

        return len(market_rows)

    def upsert_price_history(
        self,
        histories: Iterable[PriceHistory],
        *,
        source: str = "clob.prices_history",
        collection_time: datetime | None = None,
    ) -> int:
        collected_at = _normalize_utc_timestamp(collection_time or datetime.now(UTC))
        price_rows: dict[str, tuple[object, ...]] = {}

        for history in histories:
            if not history.token_id:
                continue
            for point in history.points:
                if point.timestamp is None:
                    continue
                price_time = _normalize_utc_timestamp(point.timestamp)
                price_id = _stable_hash(
                    history.token_id,
                    history.interval,
                    history.fidelity,
                    price_time.isoformat(),
                )
                price_rows[price_id] = (
                    price_id,
                    history.token_id,
                    history.interval,
                    history.fidelity,
                    price_time,
                    point.price,
                    source,
                    collected_at,
                    collected_at,
                )

        if not price_rows:
            return 0

        price_ids = [(price_id,) for price_id in price_rows]

        self._begin_transaction()
        try:
            self._connection.executemany("DELETE FROM price_history WHERE price_id = ?", price_ids)
            self._connection.executemany(
                """
                INSERT INTO price_history (
                    price_id,
                    token_id,
                    interval,
                    fidelity,
                    price_time_utc,
                    price,
                    source,
                    collection_time_utc,
                    updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                list(price_rows.values()),
            )
            self._commit_transaction()
        except Exception:
            self._rollback_transaction()
            raise

        return len(price_rows)

    def upsert_trades(
        self,
        trades: Iterable[TradeRecord],
        *,
        source: str = "data_api.trades",
        collection_time: datetime | None = None,
    ) -> int:
        collected_at = _normalize_utc_timestamp(collection_time or datetime.now(UTC))
        trade_rows: dict[str, tuple[object, ...]] = {}

        for trade in trades:
            trade_time = _normalize_nullable_utc_timestamp(trade.timestamp)
            trade_id = _build_trade_id(trade, trade_time)
            trade_rows[trade_id] = (
                trade_id,
                trade.proxy_wallet,
                trade.asset_id,
                trade.condition_id,
                trade.outcome,
                trade.side,
                trade.size,
                trade.price,
                trade.transaction_hash,
                trade.usdc_size,
                trade_time,
                source,
                collected_at,
                collected_at,
            )

        if not trade_rows:
            return 0

        trade_ids = [(trade_id,) for trade_id in trade_rows]

        self._begin_transaction()
        try:
            self._connection.executemany("DELETE FROM trades WHERE trade_id = ?", trade_ids)
            self._connection.executemany(
                """
                INSERT INTO trades (
                    trade_id,
                    proxy_wallet,
                    asset_id,
                    condition_id,
                    outcome,
                    side,
                    size,
                    price,
                    transaction_hash,
                    usdc_size,
                    trade_time_utc,
                    source,
                    collection_time_utc,
                    updated_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                list(trade_rows.values()),
            )
            self._commit_transaction()
        except Exception:
            self._rollback_transaction()
            raise

        return len(trade_rows)

    def _begin_transaction(self) -> None:
        self._connection.execute("BEGIN TRANSACTION")

    def _commit_transaction(self) -> None:
        self._connection.execute("COMMIT")

    def _rollback_transaction(self) -> None:
        self._connection.execute("ROLLBACK")


def _build_trade_id(trade: TradeRecord, trade_time: datetime | None) -> str:
    transaction_hash = _normalize_identity(trade.transaction_hash)
    asset_id = _normalize_identity(trade.asset_id)

    if transaction_hash and asset_id:
        return _stable_hash(
            "trade",
            transaction_hash,
            asset_id,
            _normalize_identity(trade.side),
            _decimal_as_text(trade.price),
            _decimal_as_text(trade.size),
            trade_time.isoformat() if trade_time else "",
        )

    return _stable_hash(
        "trade-fallback",
        _normalize_identity(trade.proxy_wallet),
        asset_id,
        _normalize_identity(trade.condition_id),
        _normalize_identity(trade.outcome),
        _normalize_identity(trade.side),
        _decimal_as_text(trade.price),
        _decimal_as_text(trade.size),
        _decimal_as_text(trade.usdc_size),
        trade_time.isoformat() if trade_time else "",
    )


def _normalize_identity(value: str | None) -> str:
    return value.strip().lower() if value else ""


def _decimal_as_text(value: Decimal | None) -> str:
    return "" if value is None else format(value, "f")


def _stable_hash(*parts: object) -> str:
    payload = "|".join(str(part) for part in parts)
    return sha256(payload.encode("utf-8")).hexdigest()


def _normalize_utc_timestamp(value: datetime) -> datetime:
    normalized = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    return normalized.replace(tzinfo=None)


def _normalize_nullable_utc_timestamp(value: datetime | None) -> datetime | None:
    return _normalize_utc_timestamp(value) if value is not None else None
