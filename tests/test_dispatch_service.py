from datetime import UTC, datetime, timedelta

import asyncio

import pytest

from src.application.dispatch.service import DispatchService
from src.application.health_store import HealthStore
from src.domain.entities.edge_event import EdgeEvent
from src.domain.entities.health_snapshot import HealthSnapshot
from src.domain.value_objects.worker_health import WorkerHealth
from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.exceptions import (
    DispatchMisconfigured,
    DispatchRejected,
    NetworkUnavailable,
)
from src.domain.ports.event_repository import EventRepository, StoredEvent
from src.domain.ports.network_gateway import NetworkGateway
from src.domain.ports.telemetry_repository import StoredPacket, TelemetryRepository
from src.domain.value_objects.beam import Beam


def _packet() -> TelemetryPacket:
    beams = [Beam(height=1.0, raw_status=0, status=()) for _ in range(64)]
    return TelemetryPacket(
        ts=datetime.now(tz=UTC),
        avg_level=1.0,
        mass=1.0,
        volume=1.0,
        beams=beams,
    )


def _snapshot() -> HealthSnapshot:
    return HealthSnapshot(
        ts=datetime(2026, 8, 16, 9, 29, tzinfo=UTC),
        workers={"ingestion": WorkerHealth(alive=True)},
        pending_count=1,
        quarantined_count=0,
        free_bytes=1,
        storage_paused=False,
        cpu_percent=1.0,
        memory_percent=2.0,
        disk_percent=3.0,
    )


class _Telemetry(TelemetryRepository):
    def __init__(self, pending: list[StoredPacket] | None = None) -> None:
        self.pending = list(pending or [])
        self.sent: list[int] = []
        self.rejected: list[int] = []

    async def save(self, packet: TelemetryPacket) -> None:
        return None

    async def get_pending(self, limit: int) -> list[StoredPacket]:
        remaining = [item for item in self.pending if item.id not in self.sent]
        return remaining[:limit]

    async def set_sent(self, ids: list[int]) -> None:
        self.sent.extend(ids)

    async def update_rejected(self, ids: list[int], quarantine_after: int) -> None:
        self.rejected.extend(ids)

    async def delete_sent_older_than(self, cutoff: datetime) -> int:
        return 0

    async def get_pending_count(self) -> int:
        return len(self.pending) - len(self.sent)

    async def get_quarantined_count(self) -> int:
        return 0


class _Events(EventRepository):
    def __init__(self, pending: list[StoredEvent] | None = None) -> None:
        self.pending = list(pending or [])
        self.sent: list[int] = []

    async def save(self, event: EdgeEvent) -> None:
        return None

    async def get_pending(self, limit: int) -> list[StoredEvent]:
        remaining = [item for item in self.pending if item.id not in self.sent]
        return remaining[:limit]

    async def set_sent(self, ids: list[int]) -> None:
        self.sent.extend(ids)


class _Gateway(NetworkGateway):
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.telemetry_sizes: list[int] = []
        self.health_error: Exception | None = None
        self.events_error: Exception | None = None
        self.telemetry_error: Exception | None = None
        self.telemetry_errors: list[Exception | None] | None = None

    async def send_telemetry(self, packets: list[TelemetryPacket]) -> None:
        self.calls.append("telemetry")
        self.telemetry_sizes.append(len(packets))
        if self.telemetry_errors is not None:
            error = self.telemetry_errors.pop(0)
            if error is not None:
                raise error
            return
        if self.telemetry_error is not None:
            raise self.telemetry_error

    async def send_events(self, events: list[EdgeEvent]) -> None:
        self.calls.append("events")
        if self.events_error is not None:
            raise self.events_error

    async def send_health(self, snapshot: HealthSnapshot) -> None:
        self.calls.append("health")
        if self.health_error is not None:
            raise self.health_error


def _service(
    telemetry: TelemetryRepository,
    events: EventRepository,
    gateway: NetworkGateway,
    health: HealthStore,
    *,
    mass_rejection_sample: int = 10,
    events_timeout: float | None = None,
) -> DispatchService:
    return DispatchService(
        repository=telemetry,
        events=events,
        gateway=gateway,
        health=health,
        batch_size=10,
        event_batch_size=50,
        retention_ttl=timedelta(days=7),
        mass_rejection_sample=mass_rejection_sample,
        quarantine_after_attempts=3,
        events_timeout=events_timeout,
    )


def _stored_event(event_id: int, event: EdgeEvent) -> StoredEvent:
    return StoredEvent(
        id=event_id,
        event=event,
        created_at=datetime.now(tz=UTC),
    )


async def test_dispatch_sends_health_then_events_then_telemetry() -> None:
    packet = _packet()
    telemetry = _Telemetry([StoredPacket(id=1, packet=packet)])
    events = _Events(
        [_stored_event(7, EdgeEvent.radar_down(reason="down"))]
    )
    health = HealthStore()
    health.put(_snapshot())
    gateway = _Gateway()

    sent = await _service(telemetry, events, gateway, health).dispatch_once()

    assert gateway.calls == ["health", "events", "telemetry"]
    assert sent == 1
    assert telemetry.sent == [1]
    assert events.sent == [7]


async def test_missing_snapshot_skips_health_but_sends_the_rest() -> None:
    telemetry = _Telemetry([StoredPacket(id=1, packet=_packet())])
    events = _Events(
        [_stored_event(7, EdgeEvent.worker_restarted(worker="ingestion"))]
    )
    gateway = _Gateway()

    await _service(telemetry, events, gateway, HealthStore()).dispatch_once()

    assert gateway.calls == ["events", "telemetry"]


async def test_events_network_error_does_not_block_telemetry() -> None:
    telemetry = _Telemetry([StoredPacket(id=1, packet=_packet())])
    events = _Events(
        [_stored_event(7, EdgeEvent.radar_down(reason="down"))]
    )
    gateway = _Gateway()
    gateway.events_error = NetworkUnavailable("offline")

    sent = await _service(telemetry, events, gateway, HealthStore()).dispatch_once()

    assert gateway.calls == ["events", "telemetry"]
    assert sent == 1
    assert telemetry.sent == [1]
    assert events.sent == []


async def test_events_timeout_does_not_block_telemetry() -> None:
    telemetry = _Telemetry([StoredPacket(id=1, packet=_packet())])
    events = _Events(
        [_stored_event(7, EdgeEvent.radar_down(reason="down"))]
    )
    gateway = _Gateway()

    async def hang(events: list[EdgeEvent]) -> None:
        gateway.calls.append("events")
        await asyncio.sleep(10)

    gateway.send_events = hang  # type: ignore[method-assign]
    sent = await _service(
        telemetry,
        events,
        gateway,
        HealthStore(),
        events_timeout=0.05,
    ).dispatch_once()

    assert "telemetry" in gateway.calls
    assert sent == 1
    assert events.sent == []


async def test_health_network_error_does_not_block_events_or_telemetry() -> None:
    telemetry = _Telemetry([StoredPacket(id=1, packet=_packet())])
    events = _Events(
        [_stored_event(7, EdgeEvent.radar_down(reason="down"))]
    )
    health = HealthStore()
    health.put(_snapshot())
    gateway = _Gateway()
    gateway.health_error = NetworkUnavailable("offline")

    sent = await _service(telemetry, events, gateway, health).dispatch_once()

    assert gateway.calls == ["health", "events", "telemetry"]
    assert sent == 1
    assert telemetry.sent == [1]
    assert events.sent == [7]


async def test_rejected_events_are_misconfigured_not_quarantined() -> None:
    telemetry = _Telemetry([StoredPacket(id=1, packet=_packet())])
    events = _Events(
        [_stored_event(7, EdgeEvent.fatal_error(
            worker="dispatch", reason="x", restartable=False
        ))]
    )
    gateway = _Gateway()
    gateway.events_error = DispatchRejected("HTTP 422")

    with pytest.raises(DispatchMisconfigured, match="события"):
        await _service(telemetry, events, gateway, HealthStore()).dispatch_once()

    assert events.sent == []
    assert telemetry.sent == []
    assert "telemetry" not in gateway.calls


async def test_rejected_health_is_misconfigured() -> None:
    health = HealthStore()
    health.put(_snapshot())
    gateway = _Gateway()
    gateway.health_error = DispatchRejected("HTTP 422")

    with pytest.raises(DispatchMisconfigured, match="health"):
        await _service(_Telemetry(), _Events(), gateway, health).dispatch_once()

    assert gateway.calls == ["health"]


def _stored_packets(count: int) -> list[StoredPacket]:
    return [StoredPacket(id=i, packet=_packet()) for i in range(1, count + 1)]


async def test_rejected_batch_retries_only_the_sample_one_by_one() -> None:
    telemetry = _Telemetry(_stored_packets(5))
    gateway = _Gateway()
    gateway.telemetry_errors = [
        DispatchRejected("HTTP 422"),
        None,
        DispatchRejected("HTTP 422"),
        None,
    ]

    sent = await _service(
        telemetry,
        _Events(),
        gateway,
        HealthStore(),
        mass_rejection_sample=3,
    ).dispatch_once()

    assert gateway.telemetry_sizes == [5, 1, 1, 1]
    assert sent == 2
    assert telemetry.sent == [1, 3]
    assert telemetry.rejected == [2]
    assert 4 not in telemetry.sent and 4 not in telemetry.rejected
    assert 5 not in telemetry.sent and 5 not in telemetry.rejected


async def test_full_sample_rejected_is_misconfigured_not_quarantine() -> None:
    telemetry = _Telemetry(_stored_packets(4))
    gateway = _Gateway()
    gateway.telemetry_error = DispatchRejected("HTTP 422")

    with pytest.raises(DispatchMisconfigured, match="сломан контракт"):
        await _service(
            telemetry,
            _Events(),
            gateway,
            HealthStore(),
            mass_rejection_sample=3,
        ).dispatch_once()

    assert gateway.telemetry_sizes == [4, 1, 1, 1]
    assert telemetry.sent == []
    assert telemetry.rejected == []


async def test_small_batch_all_rejected_goes_to_quarantine_not_misconfigured() -> None:
    telemetry = _Telemetry(_stored_packets(2))
    gateway = _Gateway()
    gateway.telemetry_error = DispatchRejected("HTTP 422")

    sent = await _service(
        telemetry,
        _Events(),
        gateway,
        HealthStore(),
        mass_rejection_sample=10,
    ).dispatch_once()

    assert gateway.telemetry_sizes == [2, 1, 1]
    assert sent == 0
    assert telemetry.sent == []
    assert telemetry.rejected == [1, 2]


async def test_isolation_network_error_quarantines_already_rejected() -> None:
    telemetry = _Telemetry(_stored_packets(3))
    gateway = _Gateway()
    gateway.telemetry_errors = [
        DispatchRejected("HTTP 422"),
        DispatchRejected("HTTP 422"),
        NetworkUnavailable("offline"),
    ]

    with pytest.raises(NetworkUnavailable, match="offline"):
        await _service(
            telemetry,
            _Events(),
            gateway,
            HealthStore(),
            mass_rejection_sample=3,
        ).dispatch_once()

    assert telemetry.rejected == [1]
    assert telemetry.sent == []
