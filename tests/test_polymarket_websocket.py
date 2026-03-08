from __future__ import annotations

import asyncio
import json
from pathlib import Path

from websockets.asyncio.server import serve

from src.clients.polymarket_websocket import (
    build_market_subscription,
    capture_market_channel_samples,
    market_channel_url,
    summarize_message_shapes,
)


def test_market_channel_url_appends_market_suffix_once() -> None:
    assert market_channel_url("wss://example.com/ws") == "wss://example.com/ws/market"
    assert market_channel_url("wss://example.com/ws/market") == "wss://example.com/ws/market"


def test_build_market_subscription_normalizes_asset_ids() -> None:
    assert build_market_subscription(["123", " 123 ", "", "456"]) == {
        "assets_ids": ["123", "456"],
        "type": "market",
        "custom_feature_enabled": True,
    }


def test_summarize_message_shapes_tracks_list_event_types() -> None:
    assert summarize_message_shapes(
        [
            {
                "payload": [
                    {
                        "event_type": "book",
                        "asset_id": "123",
                        "bids": [],
                        "asks": [],
                    }
                ]
            }
        ]
    ) == {
        "list": {
            "count": 1,
            "payload_type": "list",
            "event_types": ["book"],
            "list_item_keys": ["asks", "asset_id", "bids", "event_type"],
        }
    }


def test_capture_market_channel_samples_persists_messages(tmp_path: Path) -> None:
    async def run_test() -> tuple[dict[str, object], list[dict[str, object]]]:
        received_subscriptions: list[dict[str, object]] = []

        async def handler(websocket) -> None:
            received_subscriptions.append(json.loads(await websocket.recv()))
            await websocket.send(
                json.dumps(
                    {
                        "event_type": "book",
                        "asset_id": "123",
                        "bids": [{"price": "0.45", "size": "10"}],
                        "asks": [{"price": "0.55", "size": "12"}],
                    }
                )
            )

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            capture = await capture_market_channel_samples(
                ws_base_url=f"ws://127.0.0.1:{port}/ws",
                asset_ids=["123"],
                output_dir=tmp_path,
                max_messages=1,
                reconnect_attempts=0,
                message_timeout_seconds=1.0,
            )

        return capture, received_subscriptions

    capture, received_subscriptions = asyncio.run(run_test())

    assert received_subscriptions == [build_market_subscription(["123"])]
    assert capture["message_count"] == 1
    assert capture["message_shapes"] == {
        "book": {
            "count": 1,
            "payload_type": "dict",
            "top_level_keys": ["asks", "asset_id", "bids", "event_type"],
        }
    }

    message = capture["messages"][0]
    assert message["payload"]["event_type"] == "book"
    assert Path(capture["sample_path"]).exists()
    persisted_capture = json.loads(Path(capture["sample_path"]).read_text())
    assert persisted_capture["channel"] == "market"
    assert persisted_capture["message_count"] == 1


def test_capture_market_channel_samples_reconnects_after_close(tmp_path: Path) -> None:
    async def run_test() -> tuple[dict[str, object], int]:
        connection_count = 0

        async def handler(websocket) -> None:
            nonlocal connection_count
            connection_count += 1
            await websocket.recv()

            if connection_count == 1:
                await websocket.close(code=1011, reason="force reconnect")
                return

            await websocket.send(
                json.dumps(
                    {
                        "event_type": "price_change",
                        "asset_id": "123",
                        "price": "0.57",
                    }
                )
            )

        async with serve(handler, "127.0.0.1", 0) as server:
            port = server.sockets[0].getsockname()[1]
            capture = await capture_market_channel_samples(
                ws_base_url=f"ws://127.0.0.1:{port}/ws",
                asset_ids=["123"],
                output_dir=tmp_path,
                max_messages=1,
                reconnect_attempts=1,
                message_timeout_seconds=1.0,
            )

        return capture, connection_count

    capture, connection_count = asyncio.run(run_test())

    assert connection_count == 2
    assert capture["messages"][0]["payload"]["event_type"] == "price_change"
    assert any(event["event"] == "reconnect_scheduled" for event in capture["connection_events"])
