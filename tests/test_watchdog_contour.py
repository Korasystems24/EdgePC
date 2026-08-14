import asyncio
from datetime import UTC, datetime, timedelta

from src.application.dispatch.service import DispatchService
from src.application.dispatch.worker import DispatchWorker
from src.application.ingestion.service import IngestionService
from src.application.ingestion.worker import IngestionWorker
from src.application.watchdog.worker import Watchdog
from src.application.worker_manager import WorkerManager
from src.config.settings import DispatchSettings
from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.exceptions import (
    NetworkUnavailable,
    RadarSourceMisconfigured,
    RadarSourceUnavailable,
    TelemetryStorageError,
)
from src.domain.ports.network_gateway import NetworkGateway
from src.domain.ports.radar_adapter import RadarAdapter
from src.domain.ports.storage_monitor import StorageMonitor
from src.domain.ports.telemetry_repository import StoredPacket, TelemetryRepository
from src.domain.value_objects.beam import Beam


class _PlentyOfDisk(StorageMonitor):
    def free_bytes(self) -> int:
        return 10**12


class _Repo(TelemetryRepository):
    def __init__(self) -> None:
        self.saved = 0

    async def save(self, packet: TelemetryPacket) -> None:
        self.saved += 1

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


class _MisconfiguredRadar(RadarAdapter):
    async def read_packet(self) -> TelemetryPacket:
        raise RadarSourceMisconfigured("найдено 2 vessel")


class _OkRadar(RadarAdapter):
    async def read_packet(self) -> TelemetryPacket:
        return _packet()


class _FailingSaveRepo(_Repo):
    async def save(self, packet: TelemetryPacket) -> None:
        raise TelemetryStorageError("sqlite busy")


class _PendingRepo(_Repo):
    async def get_pending(self, limit: int) -> list[StoredPacket]:
        return [StoredPacket(id=1, packet=_packet())]


class _DownGateway(NetworkGateway):
    def __init__(self) -> None:
        self.calls = 0

    async def send_batch(self, packets: list[TelemetryPacket]) -> None:
        self.calls += 1
        raise NetworkUnavailable("connection refused")


def _watchdog(manager: WorkerManager) -> Watchdog:
    return Watchdog(
        manager,
        interval=5.0,
        restart_limit=3,
        restart_window_seconds=60.0,
    )


def _ingestion(
    radar: RadarAdapter,
    repo: TelemetryRepository,
) -> IngestionWorker:
    return IngestionWorker(
        name="ingestion",
        service=IngestionService(radar_adapter=radar, repository=repo),
        storage_monitor=_PlentyOfDisk(),
        interval=0.05,
        tick_deadline=1.0,
        heartbeat_max_silence=60.0,
    )


def _dispatch(gateway: NetworkGateway, repo: TelemetryRepository) -> DispatchWorker:
    return DispatchWorker(
        name="dispatch",
        service=DispatchService(
            repository=repo,
            gateway=gateway,
            batch_size=10,
            retention_ttl=timedelta(days=7),
            mass_rejection_sample=10,
            quarantine_after_attempts=3,
        ),
        settings=DispatchSettings(interval=0.05, tick_deadline=8.0),
        heartbeat_max_silence=60.0,
    )


async def _wait_calls(counter: object, count: int) -> None:
    deadline = asyncio.get_running_loop().time() + 2
    while getattr(counter, "calls") < count:
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("не дождались тиков")
        await asyncio.sleep(0.01)


async def test_unavailable_radar_keeps_ingestion_and_watchdog_does_not_restart() -> None:
    radar = _UnavailableRadar()
    created: list[IngestionWorker] = []

    def factory() -> IngestionWorker:
        worker = _ingestion(radar, _Repo())
        created.append(worker)
        return worker

    manager = WorkerManager({"ingestion": factory})
    manager.start_all()
    await _wait_calls(radar, 5)
    await _watchdog(manager).inspect()

    worker = manager.worker("ingestion")
    assert worker.fatal_error is None
    assert worker.is_alive()
    assert manager.task_state("ingestion").done is False
    assert len(created) == 1
    await manager.stop_all(timeout=1)


async def test_ingestion_resumes_after_radar_recovers() -> None:
    radar = _RecoveringRadar(fail_times=4)
    repo = _Repo()
    created: list[IngestionWorker] = []

    def factory() -> IngestionWorker:
        worker = _ingestion(radar, repo)
        created.append(worker)
        return worker

    manager = WorkerManager({"ingestion": factory})
    manager.start_all()
    deadline = asyncio.get_running_loop().time() + 2
    while repo.saved < 1:
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("сбор не возобновился после восстановления радара")
        await asyncio.sleep(0.01)
    await _watchdog(manager).inspect()
    assert manager.worker("ingestion").fatal_error is None
    assert manager.worker("ingestion").is_alive()
    assert len(created) == 1
    await manager.stop_all(timeout=1)


async def test_storage_errors_keep_ingestion_alive() -> None:
    radar = _OkRadar()
    created: list[IngestionWorker] = []

    def factory() -> IngestionWorker:
        worker = _ingestion(radar, _FailingSaveRepo())
        created.append(worker)
        return worker

    manager = WorkerManager({"ingestion": factory})
    manager.start_all()
    await asyncio.sleep(0.3)
    await _watchdog(manager).inspect()
    worker = manager.worker("ingestion")
    assert worker.fatal_error is None
    assert worker.is_alive()
    assert len(created) == 1
    await manager.stop_all(timeout=1)


async def test_misconfigured_radar_leaves_ingestion_stopped() -> None:
    created: list[IngestionWorker] = []

    def factory() -> IngestionWorker:
        worker = _ingestion(_MisconfiguredRadar(), _Repo())
        created.append(worker)
        return worker

    manager = WorkerManager({"ingestion": factory})
    manager.start_all()
    deadline = asyncio.get_running_loop().time() + 2
    while created[0].fatal_error is None:
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("воркер не остановился на ошибке конфигурации")
        await asyncio.sleep(0.01)
    await asyncio.sleep(0)
    await _watchdog(manager).inspect()
    assert created[0].fatal_error is not None
    assert created[0].fatal_error.restartable is False
    assert manager.task_state("ingestion").done is True
    assert len(created) == 1
    await manager.stop_all(timeout=1)


async def test_network_down_keeps_dispatch_and_watchdog_does_not_restart() -> None:
    gateway = _DownGateway()
    created: list[DispatchWorker] = []

    def factory() -> DispatchWorker:
        worker = _dispatch(gateway, _PendingRepo())
        created.append(worker)
        return worker

    manager = WorkerManager({"dispatch": factory})
    manager.start_all()
    await _wait_calls(gateway, 5)
    await _watchdog(manager).inspect()
    worker = manager.worker("dispatch")
    assert worker.fatal_error is None
    assert worker.is_alive()
    assert manager.task_state("dispatch").done is False
    assert len(created) == 1
    await manager.stop_all(timeout=1)
