from __future__ import annotations

from datetime import timezone
from pathlib import Path

from src import compat
from src.time_compat import UTC


def test_dataclass_wrapper_accepts_slots_keyword() -> None:
    @compat.dataclass(frozen=True, slots=True)
    class Example:
        value: int

    example = Example(3)

    assert example.value == 3


def test_dataclass_wrapper_ignores_slots_when_runtime_lacks_support(monkeypatch) -> None:
    monkeypatch.setattr(compat, "_DATACLASS_SUPPORTS_SLOTS", False)

    @compat.dataclass(frozen=True, slots=True)
    class Example:
        value: int

    example = Example(5)

    assert example.value == 5


def test_utc_compat_alias_matches_timezone_utc() -> None:
    assert UTC is timezone.utc


def test_repo_avoids_datetime_utc_imports() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    current_test_path = Path(__file__).resolve()

    forbidden_matches = []
    for path in tuple((repo_root / "src").rglob("*.py")) + tuple((repo_root / "tests").rglob("*.py")):
        if path.resolve() == current_test_path:
            continue
        source = path.read_text()
        if "from datetime import UTC" in source or "datetime.UTC" in source:
            forbidden_matches.append(path.relative_to(repo_root).as_posix())

    assert forbidden_matches == []
