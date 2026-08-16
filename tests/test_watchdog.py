import asyncio
from datetime import datetime

import pytest

from src.application.health_store import HealthStore
from src.config.settings import WatchdogSettings
from src.domain.value_objects.fatal_error import FatalError
from src.application.watchdog.worker import Watchdog
from src.application.worker_manager import WorkerManager
from src.domain.entities.edge_event import EdgeEvent
from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.enum import EdgeEventType
from src.domain.exceptions import ProcessMustExit, StorageError
from src.domain.ports.event_repository import EventRepository, StoredEvent
from src.domain.ports.host_metrics import HostMetrics, HostResourceSample
from src.domain.ports.storage_monitor import StorageMonitor
from src.domain.ports.telemetry_repository import StoredPacket, TelemetryRepository


class _FakeWorker:
    def __init__(
        self,
        name: str,
        *,
        fatal_on_start: FatalError | None = None,
        exit_immediately: bool = False,
        hang_on_stop: bool = False,
    ) -> None:
        self.name = name
        self.fatal_error = fatal_on_start
        self.last_heartbeat: float = 0.0
        self.last_error: str | None = None
        self.last_error_at = None
        self.storage_paused = False
        self._alive = False
        self._exit_immediately = exit_immediately
        self._hang_on_stop = hang_on_stop
        self._stop = asyncio.Event()
        self._released = asyncio.Event()
        self.started = asyncio.Event()

    async def start(self) -> None:
        self.last_heartbeat = 1.0
        self.started.set()
        if self.fatal_error is not None or self._exit_immediately:
            return
        self._alive = True
        if self._hang_on_stop:
            while True:
                try:
                    await self._released.wait()
                    return
                except asyncio.CancelledError:
                    if not self._released.is_set():
                        continue
                    raise
        await self._stop.wait()

    def stop(self) -> None:
        self._stop.set()

    def release(self) -> None:
        self._hang_on_stop = False
        self._released.set()
        self._stop.set()

    def is_alive(self, max_silence: float | None = None) -> bool:
        return self._alive

    def mark_silent(self) -> None:
        self._alive = False

    async def wait_started(self, timeout: float = 1) -> None:
        await asyncio.wait_for(self.started.wait(), timeout=timeout)


class _EventRepo(EventRepository):
    def __init__(self) -> None:
        self.saved: list[EdgeEvent] = []

    async def save(self, event: EdgeEvent) -> None:
        self.saved.append(event)

    async def get_pending(self, limit: int) -> list[StoredEvent]:
        return []

    async def set_sent(self, ids: list[int]) -> None:
        return None


class _FailingEventRepo(_EventRepo):
    async def save(self, event: EdgeEvent) -> None:
        raise StorageError("sqlite busy")


class _FlakyEventRepo(_EventRepo):
    def __init__(self, *, fail_times: int) -> None:
        super().__init__()
        self._fail_times = fail_times

    async def save(self, event: EdgeEvent) -> None:
        if self._fail_times > 0:
            self._fail_times -= 1
            raise StorageError("sqlite busy")
        await super().save(event)


class _TelemetryRepo(TelemetryRepository):
    def __init__(self, *, pending: int = 0, quarantined: int = 0) -> None:
        self.pending = pending
        self.quarantined = quarantined

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
        return self.pending

    async def get_quarantined_count(self) -> int:
        return self.quarantined


class _Storage(StorageMonitor):
    def free_bytes(self) -> int:
        return 200 * 1024 * 1024


class _Metrics(HostMetrics):
    def sample(self) -> HostResourceSample:
        return HostResourceSample(cpu_percent=4.0, memory_percent=31.0, disk_percent=42.0)


def _watchdog(
    manager: WorkerManager,
    *,
    events: EventRepository | None = None,
    telemetry: TelemetryRepository | None = None,
    health: HealthStore | None = None,
    limit: int = 3,
    interval: float = 5.0,
) -> Watchdog:
    return Watchdog(
        manager,
        events=events or _EventRepo(),
        telemetry=telemetry or _TelemetryRepo(),
        storage=_Storage(),
        host_metrics=_Metrics(),
        health=health or HealthStore(),
        settings=WatchdogSettings(
            interval=interval,
            restart_limit=limit,
            restart_window_seconds=60.0,
        ),
    )


async def test_config_fatal_does_not_stop_neighbor() -> None:
    ingestion: list[_FakeWorker] = []
    dispatch: list[_FakeWorker] = []

    def ingestion_factory() -> _FakeWorker:
        worker = _FakeWorker("ingestion")
        ingestion.append(worker)
        return worker

    def dispatch_factory() -> _FakeWorker:
        worker = _FakeWorker(
            "dispatch",
            fatal_on_start=FatalError(reason="bad token", restartable=False),
        )
        dispatch.append(worker)
        return worker

    manager = WorkerManager(
        {"ingestion": ingestion_factory, "dispatch": dispatch_factory}
    )
    manager.start_all()
    await ingestion[0].wait_started()
    await dispatch[0].wait_started()
    await asyncio.sleep(0)

    await _watchdog(manager).inspect()

    assert manager.task_state("ingestion").done is False
    assert ingestion[0].is_alive()
    assert manager.task_state("dispatch").done is True
    assert len(dispatch) == 1
    assert len(ingestion) == 1

    await manager.stop_all(timeout=1)


async def test_restartable_fatal_exits_process() -> None:
    created: list[_FakeWorker] = []

    def factory() -> _FakeWorker:
        worker = _FakeWorker(
            "ingestion",
            fatal_on_start=FatalError(reason="source hung", restartable=True),
        )
        created.append(worker)
        return worker

    manager = WorkerManager({"ingestion": factory})
    manager.start_all()
    await created[0].wait_started()
    await asyncio.sleep(0)

    with pytest.raises(ProcessMustExit, match="source hung"):
        await _watchdog(manager).inspect()
    assert len(created) == 1

    await manager.stop_all(timeout=1)


async def test_stale_heartbeat_restarts_only_that_worker() -> None:
    ingestion: list[_FakeWorker] = []
    dispatch: list[_FakeWorker] = []

    def ingestion_factory() -> _FakeWorker:
        worker = _FakeWorker("ingestion")
        ingestion.append(worker)
        return worker

    def dispatch_factory() -> _FakeWorker:
        worker = _FakeWorker("dispatch")
        dispatch.append(worker)
        return worker

    manager = WorkerManager(
        {"ingestion": ingestion_factory, "dispatch": dispatch_factory}
    )
    manager.start_all()
    await ingestion[0].wait_started()
    await dispatch[0].wait_started()

    ingestion[0].mark_silent()
    await _watchdog(manager).inspect()
    await ingestion[1].wait_started()

    assert len(ingestion) == 2
    assert manager.worker("ingestion") is ingestion[1]
    assert manager.worker("dispatch") is dispatch[0]
    assert len(dispatch) == 1

    await manager.stop_all(timeout=1)


async def test_too_many_restarts_exits_process() -> None:
    created: list[_FakeWorker] = []

    def factory() -> _FakeWorker:
        worker = _FakeWorker("dispatch", exit_immediately=True)
        created.append(worker)
        return worker

    manager = WorkerManager({"dispatch": factory})
    manager.start_all()
    await created[0].wait_started()
    await asyncio.sleep(0)
    watchdog = _watchdog(manager, limit=2)

    await watchdog.inspect()
    await created[1].wait_started()
    await asyncio.sleep(0)
    with pytest.raises(ProcessMustExit, match="слишком много рестартов"):
        await watchdog.inspect()
    assert len(created) == 3

    await manager.stop_all(timeout=1)


async def test_stuck_restart_propagates() -> None:
    created: list[_FakeWorker] = []

    def factory() -> _FakeWorker:
        worker = _FakeWorker("dispatch", hang_on_stop=True)
        created.append(worker)
        return worker

    manager = WorkerManager({"dispatch": factory}, restart_join_timeout=0.05)
    manager.start_all()
    await created[0].wait_started()
    created[0].mark_silent()

    with pytest.raises(ProcessMustExit, match="не завершился"):
        await _watchdog(manager).inspect()
    assert len(created) == 1

    created[0].release()
    await manager.stop_all(timeout=1)


async def test_stop_before_start_does_not_inspect() -> None:
    created: list[_FakeWorker] = []

    def factory() -> _FakeWorker:
        worker = _FakeWorker("ingestion", exit_immediately=True)
        created.append(worker)
        return worker

    manager = WorkerManager({"ingestion": factory})
    manager.start_all()
    await created[0].wait_started()
    await asyncio.sleep(0)
    watchdog = _watchdog(manager, interval=0.05)
    watchdog.stop()
    await asyncio.wait_for(watchdog.start(), timeout=1)
    assert len(created) == 1
    await manager.stop_all(timeout=1)


async def test_start_stops_when_asked() -> None:
    created: list[_FakeWorker] = []

    def factory() -> _FakeWorker:
        worker = _FakeWorker("ingestion")
        created.append(worker)
        return worker

    manager = WorkerManager({"ingestion": factory})
    manager.start_all()
    await created[0].wait_started()
    watchdog = _watchdog(manager, interval=1.0)
    task = asyncio.create_task(watchdog.start())
    await asyncio.sleep(0)
    watchdog.stop()
    await asyncio.wait_for(task, timeout=1)
    await manager.stop_all(timeout=1)


async def test_start_loop_restarts_silent_worker() -> None:
    created: list[_FakeWorker] = []

    def factory() -> _FakeWorker:
        worker = _FakeWorker("ingestion")
        created.append(worker)
        return worker

    manager = WorkerManager({"ingestion": factory})
    manager.start_all()
    await created[0].wait_started()
    watchdog = _watchdog(manager, interval=0.05)
    task = asyncio.create_task(watchdog.start())
    created[0].mark_silent()
    deadline = asyncio.get_running_loop().time() + 1
    while len(created) < 2:
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("воркер не перезапущен")
        await asyncio.sleep(0.01)
    await created[1].wait_started()
    watchdog.stop()
    await asyncio.wait_for(task, timeout=1)
    assert manager.worker("ingestion") is created[1]
    await manager.stop_all(timeout=1)


async def test_inspect_does_not_restart_worker_before_start_runs() -> None:
    created: list[_FakeWorker] = []

    def factory() -> _FakeWorker:
        worker = _FakeWorker("ingestion")
        created.append(worker)
        return worker

    manager = WorkerManager({"ingestion": factory})
    manager.start_all()
    await _watchdog(manager).inspect()
    assert len(created) == 1
    await created[0].wait_started()
    await manager.stop_all(timeout=1)


async def test_config_fatal_stays_stopped_on_later_inspect() -> None:
    created: list[_FakeWorker] = []

    def factory() -> _FakeWorker:
        worker = _FakeWorker(
            "dispatch",
            fatal_on_start=FatalError(reason="bad token", restartable=False),
        )
        created.append(worker)
        return worker

    manager = WorkerManager({"dispatch": factory})
    manager.start_all()
    await created[0].wait_started()
    await asyncio.sleep(0)
    watchdog = _watchdog(manager)
    await watchdog.inspect()
    await watchdog.inspect()
    assert len(created) == 1
    await manager.stop_all(timeout=1)


async def test_inspect_restarts_each_silent_worker() -> None:
    ingestion: list[_FakeWorker] = []
    dispatch: list[_FakeWorker] = []

    def ingestion_factory() -> _FakeWorker:
        worker = _FakeWorker("ingestion")
        ingestion.append(worker)
        return worker

    def dispatch_factory() -> _FakeWorker:
        worker = _FakeWorker("dispatch")
        dispatch.append(worker)
        return worker

    manager = WorkerManager(
        {"ingestion": ingestion_factory, "dispatch": dispatch_factory}
    )
    manager.start_all()
    await ingestion[0].wait_started()
    await dispatch[0].wait_started()
    ingestion[0].mark_silent()
    dispatch[0].mark_silent()
    await _watchdog(manager).inspect()
    await ingestion[1].wait_started()
    await dispatch[1].wait_started()
    assert len(ingestion) == 2
    assert len(dispatch) == 2
    await manager.stop_all(timeout=1)


async def test_inspect_after_stop_does_not_restart() -> None:
    created: list[_FakeWorker] = []

    def factory() -> _FakeWorker:
        worker = _FakeWorker("ingestion")
        created.append(worker)
        return worker

    manager = WorkerManager({"ingestion": factory})
    manager.start_all()
    await created[0].wait_started()
    watchdog = _watchdog(manager)
    watchdog.stop()
    created[0].mark_silent()
    await watchdog.inspect()
    assert len(created) == 1
    await manager.stop_all(timeout=1)


async def test_restart_writes_worker_restarted_event() -> None:
    created: list[_FakeWorker] = []

    def factory() -> _FakeWorker:
        worker = _FakeWorker("ingestion")
        created.append(worker)
        return worker

    events = _EventRepo()
    manager = WorkerManager({"ingestion": factory})
    manager.start_all()
    await created[0].wait_started()
    created[0].mark_silent()
    await _watchdog(manager, events=events).inspect()
    await created[1].wait_started()

    assert [event.type for event in events.saved] == [EdgeEventType.WORKER_RESTARTED]
    assert events.saved[0].payload.worker == "ingestion"
    await manager.stop_all(timeout=1)


async def test_config_fatal_writes_one_fatal_event() -> None:
    created: list[_FakeWorker] = []

    def factory() -> _FakeWorker:
        worker = _FakeWorker(
            "dispatch",
            fatal_on_start=FatalError(reason="bad token", restartable=False),
        )
        created.append(worker)
        return worker

    events = _EventRepo()
    manager = WorkerManager({"dispatch": factory})
    manager.start_all()
    await created[0].wait_started()
    await asyncio.sleep(0)
    watchdog = _watchdog(manager, events=events)
    await watchdog.inspect()
    await watchdog.inspect()

    assert [event.type for event in events.saved] == [EdgeEventType.FATAL_ERROR]
    assert events.saved[0].payload.worker == "dispatch"
    assert events.saved[0].payload.reason == "bad token"
    assert events.saved[0].payload.restartable is False
    await manager.stop_all(timeout=1)


async def test_config_fatal_retries_event_if_save_fails() -> None:
    created: list[_FakeWorker] = []

    def factory() -> _FakeWorker:
        worker = _FakeWorker(
            "dispatch",
            fatal_on_start=FatalError(reason="bad token", restartable=False),
        )
        created.append(worker)
        return worker

    events = _FlakyEventRepo(fail_times=1)
    manager = WorkerManager({"dispatch": factory})
    manager.start_all()
    await created[0].wait_started()
    await asyncio.sleep(0)
    watchdog = _watchdog(manager, events=events)
    await watchdog.inspect()
    assert events.saved == []

    await watchdog.inspect()
    assert [event.type for event in events.saved] == [EdgeEventType.FATAL_ERROR]
    await manager.stop_all(timeout=1)


async def test_restartable_fatal_writes_event_before_exit() -> None:
    created: list[_FakeWorker] = []

    def factory() -> _FakeWorker:
        worker = _FakeWorker(
            "ingestion",
            fatal_on_start=FatalError(reason="source hung", restartable=True),
        )
        created.append(worker)
        return worker

    events = _EventRepo()
    manager = WorkerManager({"ingestion": factory})
    manager.start_all()
    await created[0].wait_started()
    await asyncio.sleep(0)
    with pytest.raises(ProcessMustExit, match="source hung"):
        await _watchdog(manager, events=events).inspect()
    assert [event.type for event in events.saved] == [EdgeEventType.FATAL_ERROR]
    assert events.saved[0].payload.restartable is True
    await manager.stop_all(timeout=1)


async def test_event_save_failure_does_not_block_restart() -> None:
    created: list[_FakeWorker] = []

    def factory() -> _FakeWorker:
        worker = _FakeWorker("ingestion")
        created.append(worker)
        return worker

    manager = WorkerManager({"ingestion": factory})
    manager.start_all()
    await created[0].wait_started()
    created[0].mark_silent()
    await _watchdog(manager, events=_FailingEventRepo()).inspect()
    await created[1].wait_started()
    assert len(created) == 2
    await manager.stop_all(timeout=1)


async def test_inspect_puts_fresh_snapshot_in_health_store() -> None:
    created: list[_FakeWorker] = []

    def factory() -> _FakeWorker:
        worker = _FakeWorker("ingestion")
        created.append(worker)
        return worker

    health = HealthStore()
    telemetry = _TelemetryRepo(pending=12, quarantined=1)
    manager = WorkerManager({"ingestion": factory})
    manager.start_all()
    await created[0].wait_started()
    await _watchdog(manager, telemetry=telemetry, health=health).inspect()

    snapshot = health.current()
    assert snapshot is not None
    assert snapshot.pending_count == 12
    assert snapshot.quarantined_count == 1
    assert snapshot.free_bytes == 200 * 1024 * 1024
    assert snapshot.storage_paused is False
    assert snapshot.cpu_percent == 4.0
    assert snapshot.workers["ingestion"].alive is True
    await manager.stop_all(timeout=1)


async def test_inspect_timeout_does_not_crash() -> None:
    class _SlowTelemetry(_TelemetryRepo):
        async def get_pending_count(self) -> int:
            await asyncio.sleep(5)
            return 0

    watchdog = Watchdog(
        WorkerManager({}),
        events=_EventRepo(),
        telemetry=_SlowTelemetry(),
        storage=_Storage(),
        host_metrics=_Metrics(),
        health=HealthStore(),
        settings=WatchdogSettings(
            interval=5.0,
            inspect_timeout=0.05,
            restart_limit=3,
            restart_window_seconds=60.0,
        ),
    )
    await asyncio.wait_for(watchdog.inspect(), timeout=1.0)
