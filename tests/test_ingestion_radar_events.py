import asyncio
from collections.abc import Callable
from datetime import UTC, datetime

from src.application.ingestion.service import IngestionService
from src.application.ingestion.worker import IngestionWorker
from src.config.settings import IngestionSettings
from src.domain.entities.edge_event import EdgeEvent
from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.enum import EdgeEventType
from src.domain.exceptions import RadarSourceUnavailable
from src.domain.ports.event_repository import EventRepository, StoredEvent
from src.domain.ports.radar_adapter import RadarAdapter
from src.domain.ports.storage_monitor import StorageMonitor
from src.domain.ports.telemetry_repository import StoredPacket, TelemetryRepository
from src.domain.value_objects.beam import Beam


class _PlentyOfDisk(StorageMonitor):
    def free_bytes(self) -> int:
        return 10**12


class _TelemetryRepo(TelemetryRepository):
    def __init__(self) -> None:
        self.saved: list[TelemetryPacket] = []

    async def save(self, packet: TelemetryPacket) -> None:
        self.saved.append(packet)

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


class _EventRepo(EventRepository):
    def __init__(self) -> None:
        self.saved: list[EdgeEvent] = []

    async def save(self, event: EdgeEvent) -> None:
        self.saved.append(event)

    async def get_pending(self, limit: int) -> list[StoredEvent]:
        return []

    async def set_sent(self, ids: list[int]) -> None:
        return None


def _packet() -> TelemetryPacket:
    beams = [Beam(height=1.0, raw_status=0, status=()) for _ in range(64)]
    return TelemetryPacket(
        ts=datetime.now(tz=UTC),
        avg_level=1.0,
        mass=1.0,
        volume=1.0,
        beams=beams,
    )


class _UnavailableRadar(RadarAdapter):
    def __init__(self) -> None:
        self.calls = 0

    async def read_packet(self) -> TelemetryPacket:
        self.calls += 1
        raise RadarSourceUnavailable("connection refused")


class _RecoveringRadar(RadarAdapter):
    def __init__(self, fail_times: int) -> None:
        self.calls = 0
        self._fail_times = fail_times

    async def read_packet(self) -> TelemetryPacket:
        self.calls += 1
        if self.calls <= self._fail_times:
            raise RadarSourceUnavailable("connection refused")
        return _packet()


class _OkRadar(RadarAdapter):
    async def read_packet(self) -> TelemetryPacket:
        return _packet()


def _worker(
    radar: RadarAdapter,
    telemetry: TelemetryRepository,
    events: EventRepository,
) -> IngestionWorker:
    return IngestionWorker(
        name="ingestion",
        service=IngestionService(radar_adapter=radar, repository=telemetry),
        settings=IngestionSettings(interval=0.05, tick_deadline=1.0),
        storage_monitor=_PlentyOfDisk(),
        event_repository=events,
        heartbeat_max_silence=60.0,
    )


async def _wait_until(predicate: Callable[[], bool], *, timeout: float = 2.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("условие не выполнилось вовремя")
        await asyncio.sleep(0.01)


async def test_repeated_unavailable_writes_one_radar_down() -> None:
    radar = _UnavailableRadar()
    telemetry = _TelemetryRepo()
    events = _EventRepo()
    worker = _worker(radar, telemetry, events)
    task = asyncio.create_task(worker.start())
    try:
        await _wait_until(lambda: radar.calls >= 5)
        assert [event.type for event in events.saved] == [EdgeEventType.RADAR_DOWN]
        assert events.saved[0].payload.reason == "connection refused"
        assert telemetry.saved == []
    finally:
        worker.stop()
        await asyncio.wait_for(task, timeout=1)


async def test_recovery_writes_one_radar_recovered_and_normal_packet() -> None:
    radar = _RecoveringRadar(fail_times=3)
    telemetry = _TelemetryRepo()
    events = _EventRepo()
    worker = _worker(radar, telemetry, events)
    task = asyncio.create_task(worker.start())
    try:
        await _wait_until(lambda: len(telemetry.saved) >= 1)
        await _wait_until(lambda: len(events.saved) >= 2)
        assert [event.type for event in events.saved] == [
            EdgeEventType.RADAR_DOWN,
            EdgeEventType.RADAR_RECOVERED,
        ]
        downtime = events.saved[1].payload.downtime_seconds
        assert isinstance(downtime, float)
        assert downtime >= 0
        assert "source_state" not in TelemetryPacket.model_fields
    finally:
        worker.stop()
        await asyncio.wait_for(task, timeout=1)


async def test_healthy_ticks_do_not_write_source_events() -> None:
    telemetry = _TelemetryRepo()
    events = _EventRepo()
    worker = _worker(_OkRadar(), telemetry, events)
    task = asyncio.create_task(worker.start())
    try:
        await _wait_until(lambda: len(telemetry.saved) >= 1)
        assert events.saved == []
    finally:
        worker.stop()
        await asyncio.wait_for(task, timeout=1)
