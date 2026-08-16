import asyncio
from datetime import datetime, timedelta

from src.application.dispatch.service import DispatchService
from src.application.dispatch.worker import DispatchWorker
from src.application.health_store import HealthStore
from src.config.settings import DispatchSettings
from src.domain.entities.edge_event import EdgeEvent
from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.exceptions import NetworkUnavailable, StorageError
from src.domain.ports.event_repository import EventRepository, StoredEvent
from src.domain.ports.network_gateway import NetworkGateway
from src.domain.ports.telemetry_repository import StoredPacket, TelemetryRepository
from src.domain.entities.health_snapshot import HealthSnapshot


class _Telemetry(TelemetryRepository):
    async def save(self, packet: TelemetryPacket) -> None:
        return None

    async def get_pending(self, limit: int) -> list[StoredPacket]:
        return []

    async def set_sent(self, ids: list[int]) -> None:
        return None

    async def update_rejected(self, ids: list[int], quarantine_after: int) -> None:
        return None

    async def delete_sent_older_than(self, cutoff: datetime) -> int:
        return 0

    async def get_pending_count(self) -> int:
        return 0

    async def get_quarantined_count(self) -> int:
        return 0


class _Events(EventRepository):
    async def save(self, event: EdgeEvent) -> None:
        return None

    async def get_pending(self, limit: int) -> list[StoredEvent]:
        return []

    async def set_sent(self, ids: list[int]) -> None:
        return None


class _Gateway(NetworkGateway):
    async def send_telemetry(self, packets: list[TelemetryPacket]) -> None:
        return None

    async def send_events(self, events: list[EdgeEvent]) -> None:
        return None

    async def send_health(self, snapshot: HealthSnapshot) -> None:
        return None


def _worker(service: DispatchService) -> DispatchWorker:
    return DispatchWorker(
        name="dispatch",
        service=service,
        settings=DispatchSettings(
            interval=0.05,
            tick_deadline=45.0,
            waiting_period_ttl=timedelta(seconds=0),
        ),
    )


def _service(gateway: NetworkGateway | None = None) -> DispatchService:
    return DispatchService(
        repository=_Telemetry(),
        events=_Events(),
        gateway=gateway or _Gateway(),
        health=HealthStore(),
        batch_size=10,
        event_batch_size=50,
        retention_ttl=timedelta(days=7),
        mass_rejection_sample=10,
        quarantine_after_attempts=3,
    )


async def test_tick_timeout_is_network_not_storage_failure() -> None:
    service = _service()

    async def hang() -> int:
        await asyncio.sleep(10)
        return 0

    service.dispatch_once = hang  # type: ignore[method-assign]
    worker = DispatchWorker(
        name="dispatch",
        service=service,
        settings=DispatchSettings(
            interval=0.05,
            tick_deadline=0.05,
            connect_timeout=0.01,
            read_timeout=0.01,
            write_timeout=0.01,
            pool_timeout=0.01,
            retry_attempts=1,
            retry_wait_max=0.0,
        ),
    )
    await worker._safe_tick()
    assert worker._consecutive_failures == 0
    assert worker._network_unavailable is True
    assert worker.fatal_error is None


async def test_retention_runs_after_network_failure() -> None:
    service = _service()
    retention_calls = 0

    async def offline() -> int:
        raise NetworkUnavailable("offline")

    async def retain() -> int:
        nonlocal retention_calls
        retention_calls += 1
        return 0

    service.dispatch_once = offline  # type: ignore[method-assign]
    service.delete_sent_older_than = retain  # type: ignore[method-assign]
    worker = _worker(service)
    worker._retention_deadline = 0.0
    await worker._safe_tick()
    assert retention_calls == 1
    assert worker._network_unavailable is True
    assert worker._consecutive_failures == 0


async def test_retention_skipped_on_storage_error() -> None:
    service = _service()
    retention_calls = 0

    async def boom() -> int:
        raise StorageError("sqlite busy")

    async def retain() -> int:
        nonlocal retention_calls
        retention_calls += 1
        return 0

    service.dispatch_once = boom  # type: ignore[method-assign]
    service.delete_sent_older_than = retain  # type: ignore[method-assign]
    worker = _worker(service)
    worker._retention_deadline = 0.0
    await worker._safe_tick()
    assert retention_calls == 0
    assert worker._consecutive_failures == 1
