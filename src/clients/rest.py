from __future__ import annotations

import json
import os
import time
from src.compat import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from src.time_compat import UTC, datetime


DEFAULT_REQUEST_TIMEOUT_SECONDS = 20.0
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_RETRY_BACKOFF_SECONDS = 0.5
RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})


class UnexpectedPayloadError(ValueError):
    """Raised when a response payload does not match the expected thin-client shape."""


@dataclass(frozen=True, slots=True)
class RequestConfig:
    timeout_seconds: float = DEFAULT_REQUEST_TIMEOUT_SECONDS
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    retry_backoff_seconds: float = DEFAULT_RETRY_BACKOFF_SECONDS


class RestJsonClient:
    def __init__(
        self,
        base_url: str,
        *,
        request_config: RequestConfig | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.request_config = request_config or RequestConfig()
        if self.request_config.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be greater than zero.")
        if self.request_config.max_attempts <= 0:
            raise ValueError("max_attempts must be greater than zero.")
        if self.request_config.retry_backoff_seconds < 0:
            raise ValueError("retry_backoff_seconds cannot be negative.")

        self.base_url = normalize_base_url(base_url)
        self._client = httpx.Client(
            base_url=self.base_url,
            follow_redirects=True,
            headers={"Accept": "application/json"},
            timeout=self.request_config.timeout_seconds,
            transport=transport,
        )

    @property
    def timeout_seconds(self) -> float:
        return self.request_config.timeout_seconds

    @property
    def max_attempts(self) -> int:
        return self.request_config.max_attempts

    @property
    def retry_backoff_seconds(self) -> float:
        return self.request_config.retry_backoff_seconds

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> RestJsonClient:
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def get_json(self, endpoint: str, *, params: dict[str, Any] | None = None) -> Any:
        normalized_endpoint = endpoint if endpoint.startswith("/") else f"/{endpoint}"

        for attempt in range(1, self.max_attempts + 1):
            try:
                response = self._client.get(normalized_endpoint, params=params)
            except (httpx.TimeoutException, httpx.NetworkError):
                if attempt >= self.max_attempts:
                    raise
                self._sleep_before_retry()
                continue

            if response.status_code in RETRYABLE_STATUS_CODES and attempt < self.max_attempts:
                response.close()
                self._sleep_before_retry()
                continue

            response.raise_for_status()
            return response.json()

        raise RuntimeError("Request attempts exhausted without returning JSON.")

    def _sleep_before_retry(self) -> None:
        if self.retry_backoff_seconds > 0:
            time.sleep(self.retry_backoff_seconds)


def resolve_base_url(base_url: str | None, env_var: str, default: str) -> str:
    if base_url is not None and base_url.strip():
        return normalize_base_url(base_url)
    return normalize_base_url(os.getenv(env_var, default))


def normalize_base_url(base_url: str) -> str:
    return base_url.rstrip("/")


def parse_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def parse_optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if not normalized:
            return None
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    raise UnexpectedPayloadError(f"Could not parse boolean value from {value!r}.")


def parse_optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        return int(normalized)
    raise UnexpectedPayloadError(f"Could not parse integer value from {value!r}.")


def parse_optional_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise UnexpectedPayloadError(f"Could not parse decimal value from {value!r}.") from exc


def parse_optional_datetime(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        timestamp = float(value)
        if timestamp >= 10_000_000_000:
            timestamp /= 1000.0
        return datetime.fromtimestamp(timestamp, tz=UTC)
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        if normalized.isdigit():
            return parse_optional_datetime(int(normalized))
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
        return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    raise UnexpectedPayloadError(f"Could not parse datetime value from {value!r}.")


def parse_string_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, list):
        return tuple(normalized for item in value if (normalized := parse_optional_str(item)))
    if isinstance(value, tuple):
        return tuple(normalized for item in value if (normalized := parse_optional_str(item)))
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return ()
        try:
            decoded = json.loads(normalized)
        except json.JSONDecodeError:
            return tuple(part.strip() for part in normalized.split(",") if part.strip())
        if isinstance(decoded, list):
            return tuple(parsed for item in decoded if (parsed := parse_optional_str(item)))
    raise UnexpectedPayloadError(f"Could not parse string sequence from {value!r}.")


def extract_records(payload: Any, *, wrapper_keys: tuple[str, ...] = ()) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if isinstance(payload, dict):
        for key in wrapper_keys:
            candidate = payload.get(key)
            if isinstance(candidate, list):
                return [item for item in candidate if isinstance(item, dict)]

        list_values = [
            value
            for value in payload.values()
            if isinstance(value, list) and all(isinstance(item, dict) for item in value)
        ]
        if len(list_values) == 1:
            return list_values[0]

        return [payload]

    raise UnexpectedPayloadError(f"Expected a JSON object or list of objects, got {type(payload)!r}.")


def flatten_nested_records(
    payload: Any,
    nested_key: str,
    *,
    wrapper_keys: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    flattened_records: list[dict[str, Any]] = []

    for record in extract_records(payload, wrapper_keys=wrapper_keys):
        nested_records = record.get(nested_key)
        if isinstance(nested_records, list):
            parent_fields = {key: value for key, value in record.items() if key != nested_key}
            for nested_record in nested_records:
                if isinstance(nested_record, dict):
                    flattened_records.append({**parent_fields, **nested_record})
            continue
        flattened_records.append(record)

    return flattened_records
