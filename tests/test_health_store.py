from datetime import UTC, datetime

from src.application.health_store import HealthStore
from src.domain.entities.health_snapshot import HealthSnapshot
from src.domain.value_objects.worker_health import WorkerHealth


def _snapshot(*, pending_count: int) -> HealthSnapshot:
    return HealthSnapshot(
        ts=datetime(2026, 8, 16, 9, 29, tzinfo=UTC),
        workers={"ingestion": WorkerHealth(alive=True)},
        pending_count=pending_count,
        quarantined_count=0,
        free_bytes=1,
        storage_paused=False,
        cpu_percent=1.0,
        memory_percent=2.0,
        disk_percent=3.0,
    )


def test_current_is_none_before_first_put() -> None:
    assert HealthStore().current() is None


def test_put_replaces_previous_snapshot() -> None:
    store = HealthStore()
    first = _snapshot(pending_count=10)
    second = _snapshot(pending_count=2)
    store.put(first)
    store.put(second)
    assert store.current() is second
    assert store.current() is not first
