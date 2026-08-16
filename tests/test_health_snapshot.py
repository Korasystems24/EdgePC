from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from src.domain.entities.health_snapshot import HealthSnapshot
from src.domain.value_objects.fatal_error import FatalError
from src.domain.value_objects.worker_health import WorkerHealth


def _snapshot(**overrides: object) -> HealthSnapshot:
    payload: dict[str, object] = {
        "ts": datetime(2026, 8, 16, 9, 29, tzinfo=UTC),
        "workers": {
            "ingestion": WorkerHealth(alive=True),
            "dispatch": WorkerHealth(
                alive=False,
                last_error="dispatch misconfigured",
                last_error_at=datetime(2026, 8, 16, 9, 28, tzinfo=UTC),
                fatal=FatalError(reason="dispatch misconfigured", restartable=False),
            ),
        },
        "pending_count": 12,
        "quarantined_count": 1,
        "free_bytes": 200 * 1024 * 1024,
        "storage_paused": False,
        "cpu_percent": 4.2,
        "memory_percent": 31.0,
        "disk_percent": 42.0,
    }
    payload.update(overrides)
    return HealthSnapshot.model_validate(payload)


def test_snapshot_is_frozen() -> None:
    snapshot = _snapshot()
    with pytest.raises(ValidationError):
        snapshot.pending_count = 0  # type: ignore[misc]


def test_snapshot_rejects_naive_timestamp() -> None:
    with pytest.raises(ValidationError):
        _snapshot(ts=datetime(2026, 8, 16, 9, 29))


def test_snapshot_rejects_negative_counts() -> None:
    with pytest.raises(ValidationError):
        _snapshot(pending_count=-1)
    with pytest.raises(ValidationError):
        _snapshot(free_bytes=-1)


def test_snapshot_rejects_percent_out_of_range() -> None:
    with pytest.raises(ValidationError):
        _snapshot(cpu_percent=100.1)
    with pytest.raises(ValidationError):
        _snapshot(memory_percent=-0.1)


def test_snapshot_does_not_carry_bunker_id() -> None:
    assert "bunker_id" not in HealthSnapshot.model_fields
