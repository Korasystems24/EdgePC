import asyncio

import pytest

from src.application.ports.worker import FatalError
from src.application.watchdog.worker import Watchdog
from src.application.worker_manager import WorkerManager
from src.domain.exceptions import ProcessMustExit


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


def _watchdog(
    manager: WorkerManager, *, limit: int = 3, interval: float = 5.0
) -> Watchdog:
    return Watchdog(
        manager,
        interval=interval,
        restart_limit=limit,
        restart_window_seconds=60.0,
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
