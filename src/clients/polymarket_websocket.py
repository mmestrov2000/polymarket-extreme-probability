from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed


DEFAULT_WS_BASE_URL = "wss://ws-subscriptions-clob.polymarket.com/ws"


def timestamp_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def market_channel_url(ws_base_url: str = DEFAULT_WS_BASE_URL) -> str:
    normalized_base_url = ws_base_url.rstrip("/")
    if normalized_base_url.endswith("/market"):
        return normalized_base_url
    return f"{normalized_base_url}/market"


def build_market_subscription(
    asset_ids: Sequence[str],
    *,
    custom_feature_enabled: bool = True,
) -> dict[str, Any]:
    normalized_asset_ids: list[str] = []
    for asset_id in asset_ids:
        normalized_asset_id = str(asset_id).strip()
        if normalized_asset_id and normalized_asset_id not in normalized_asset_ids:
            normalized_asset_ids.append(normalized_asset_id)

    if not normalized_asset_ids:
        raise ValueError("asset_ids must contain at least one non-empty token id.")

    return {
        "assets_ids": normalized_asset_ids,
        "type": "market",
        "custom_feature_enabled": custom_feature_enabled,
    }


def decode_websocket_message(raw_message: str | bytes) -> tuple[str, Any]:
    raw_text = raw_message.decode("utf-8") if isinstance(raw_message, bytes) else raw_message

    try:
        payload = json.loads(raw_text)
    except json.JSONDecodeError:
        payload = raw_text

    return raw_text, payload


def summarize_message_shapes(messages: Sequence[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    summary: dict[str, dict[str, Any]] = {}

    for message in messages:
        payload = message.get("payload")

        if isinstance(payload, dict):
            label = str(payload.get("event_type") or payload.get("type") or "dict")
            bucket = summary.setdefault(
                label,
                {
                    "count": 0,
                    "payload_type": "dict",
                    "top_level_keys": [],
                },
            )
            bucket["count"] += 1
            bucket["top_level_keys"] = sorted(set(bucket["top_level_keys"]).union(payload.keys()))
            continue

        if isinstance(payload, list):
            list_item_keys = set()
            event_types = set()
            for item in payload:
                if isinstance(item, dict):
                    list_item_keys.update(item.keys())
                    if item.get("event_type"):
                        event_types.add(str(item["event_type"]))

            bucket = summary.setdefault(
                "list",
                {
                    "count": 0,
                    "payload_type": "list",
                    "event_types": [],
                    "list_item_keys": [],
                },
            )
            bucket["count"] += 1
            bucket["event_types"] = sorted(set(bucket["event_types"]).union(event_types))
            bucket["list_item_keys"] = sorted(set(bucket["list_item_keys"]).union(list_item_keys))
            continue

        label = type(payload).__name__
        bucket = summary.setdefault(
            label,
            {
                "count": 0,
                "payload_type": label,
            },
        )
        bucket["count"] += 1

    return summary


def save_websocket_capture(output_dir: Path | str, capture: dict[str, Any]) -> Path:
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    sample_path = output_path / f"{timestamp}_market_channel_capture.json"
    sample_path.write_text(json.dumps(capture, indent=2, sort_keys=True))
    return sample_path


async def capture_market_channel_samples(
    ws_base_url: str,
    asset_ids: Sequence[str],
    output_dir: Path | str,
    *,
    max_messages: int = 3,
    reconnect_attempts: int = 1,
    open_timeout_seconds: float = 10.0,
    message_timeout_seconds: float = 20.0,
    ping_interval_seconds: float = 20.0,
    ping_timeout_seconds: float = 20.0,
) -> dict[str, Any]:
    if max_messages <= 0:
        raise ValueError("max_messages must be greater than zero.")
    if reconnect_attempts < 0:
        raise ValueError("reconnect_attempts cannot be negative.")

    channel_url = market_channel_url(ws_base_url)
    subscription_payload = build_market_subscription(asset_ids)
    connection_events: list[dict[str, Any]] = []
    captured_messages: list[dict[str, Any]] = []

    attempts_allowed = reconnect_attempts + 1

    for attempt in range(1, attempts_allowed + 1):
        connection_events.append(
            {
                "event": "connect_attempt",
                "attempt": attempt,
                "occurred_at_utc": timestamp_utc(),
                "url": channel_url,
            }
        )

        try:
            async with connect(
                channel_url,
                open_timeout=open_timeout_seconds,
                ping_interval=ping_interval_seconds,
                ping_timeout=ping_timeout_seconds,
            ) as websocket:
                connection_events.append(
                    {
                        "event": "connected",
                        "attempt": attempt,
                        "occurred_at_utc": timestamp_utc(),
                    }
                )

                await websocket.send(json.dumps(subscription_payload))
                connection_events.append(
                    {
                        "event": "subscription_sent",
                        "attempt": attempt,
                        "occurred_at_utc": timestamp_utc(),
                        "payload": subscription_payload,
                    }
                )

                while len(captured_messages) < max_messages:
                    try:
                        raw_message = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=message_timeout_seconds,
                        )
                    except asyncio.TimeoutError:
                        connection_events.append(
                            {
                                "event": "receive_timeout",
                                "attempt": attempt,
                                "occurred_at_utc": timestamp_utc(),
                                "message_timeout_seconds": message_timeout_seconds,
                            }
                        )
                        if captured_messages:
                            break
                        raise

                    raw_text, payload = decode_websocket_message(raw_message)
                    captured_messages.append(
                        {
                            "attempt": attempt,
                            "received_at_utc": timestamp_utc(),
                            "raw_text": raw_text,
                            "payload": payload,
                        }
                    )

                if len(captured_messages) >= max_messages:
                    break
        except asyncio.TimeoutError as exc:
            if attempt == attempts_allowed:
                raise TimeoutError(
                    f"No WebSocket message arrived within {message_timeout_seconds} seconds."
                ) from exc
        except ConnectionClosed as exc:
            received_close = getattr(exc, "rcvd", None)
            connection_events.append(
                {
                    "event": "connection_closed",
                    "attempt": attempt,
                    "occurred_at_utc": timestamp_utc(),
                    "code": getattr(received_close, "code", None),
                    "reason": getattr(received_close, "reason", ""),
                }
            )
            if attempt == attempts_allowed and not captured_messages:
                raise RuntimeError("WebSocket closed before any messages were captured.") from exc

        if len(captured_messages) >= max_messages:
            break

        if attempt < attempts_allowed:
            connection_events.append(
                {
                    "event": "reconnect_scheduled",
                    "attempt": attempt,
                    "occurred_at_utc": timestamp_utc(),
                    "next_attempt": attempt + 1,
                }
            )

    if not captured_messages:
        raise RuntimeError("WebSocket capture finished without any messages.")

    capture = {
        "source": "websocket",
        "channel": "market",
        "url": channel_url,
        "captured_at_utc": timestamp_utc(),
        "subscription_payload": subscription_payload,
        "connection_events": connection_events,
        "message_count": len(captured_messages),
        "message_shapes": summarize_message_shapes(captured_messages),
        "messages": captured_messages,
    }
    sample_path = save_websocket_capture(output_dir, capture)

    return capture | {"sample_path": str(sample_path)}
