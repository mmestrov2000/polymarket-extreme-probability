from __future__ import annotations

import json
import re
from src.compat import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

from src.time_compat import UTC, datetime


DEFAULT_RAW_DATA_DIR = Path("data/raw")
_INVALID_PATH_CHARS = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True, slots=True)
class RawCaptureResult:
    path: Path
    format: str
    records_written: int
    capture_id: str


class RawPayloadStore:
    """Persists append-only raw payload captures under `data/raw/<source>/<dataset>/...`."""

    def __init__(self, base_dir: str | Path = DEFAULT_RAW_DATA_DIR) -> None:
        self.base_dir = Path(base_dir)

    def write_capture(
        self,
        source: str,
        dataset: str,
        payload: Any,
        *,
        endpoint: str | None = None,
        request_params: dict[str, Any] | None = None,
        collection_time: datetime | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RawCaptureResult:
        collected_at = _normalize_utc_timestamp(collection_time or datetime.now(UTC))
        capture_id = uuid4().hex
        destination_dir = (
            self.base_dir
            / _normalize_path_component(source)
            / _normalize_path_component(dataset)
            / f"date={collected_at:%Y-%m-%d}"
        )
        destination_dir.mkdir(parents=True, exist_ok=True)

        envelope = {
            "capture_id": capture_id,
            "source": source,
            "dataset": dataset,
            "endpoint": endpoint,
            "request_params": request_params or {},
            "collection_time_utc": collected_at.isoformat(),
            "metadata": metadata or {},
        }

        if isinstance(payload, (list, tuple)):
            path = destination_dir / f"{collected_at:%Y%m%dT%H%M%S_%fZ}_{capture_id}.jsonl"
            records = list(payload)
            self._write_jsonl_capture(path, envelope, records)
            return RawCaptureResult(
                path=path,
                format="jsonl",
                records_written=len(records),
                capture_id=capture_id,
            )

        path = destination_dir / f"{collected_at:%Y%m%dT%H%M%S_%fZ}_{capture_id}.json"
        self._write_json_capture(path, envelope, payload)
        return RawCaptureResult(path=path, format="json", records_written=1, capture_id=capture_id)

    @staticmethod
    def _write_json_capture(path: Path, envelope: dict[str, Any], payload: Any) -> None:
        record = {**envelope, "payload": payload}
        path.write_text(f"{json.dumps(record, indent=2, sort_keys=True, default=_json_default)}\n")

    @staticmethod
    def _write_jsonl_capture(path: Path, envelope: dict[str, Any], records: list[Any]) -> None:
        if not records:
            lines = [
                json.dumps(
                    {
                        **envelope,
                        "record_count": 0,
                        "record_index": None,
                        "payload": [],
                    },
                    sort_keys=True,
                    default=_json_default,
                )
            ]
        else:
            lines = [
                json.dumps(
                    {
                        **envelope,
                        "record_count": len(records),
                        "record_index": index,
                        "payload": record,
                    },
                    sort_keys=True,
                    default=_json_default,
                )
                for index, record in enumerate(records)
            ]
        path.write_text("\n".join(lines) + "\n")


def _normalize_path_component(value: str) -> str:
    normalized = _INVALID_PATH_CHARS.sub("_", value.strip().lower()).strip("_")
    return normalized or "unknown"


def _normalize_utc_timestamp(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return _normalize_utc_timestamp(value).isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"Object of type {type(value)!r} is not JSON serializable.")
