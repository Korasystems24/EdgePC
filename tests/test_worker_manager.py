import asyncio
import time
from collections.abc import Callable

import pytest

from src.application.ports.worker import FatalError
from src.application.worker_manager import WorkerManager
from src.domain.exceptions import WorkerAlreadyRunning, WorkerRestartStuck


class _FakeWorker:
    def __init__(
        self,
        name: str,
        *,
        hang_on_stop: bool = False,
        crash: BaseException | None = None,
    ) -> None:
        self.name = name
        self._hang_on_stop = hang_on_stop
        self._crash = crash
        self._stop = asyncio.Event()
        self.started = asyncio.Event()
        self.stopped = asyncio.Event()
        self._released = asyncio.Event()
        self._running = False
        self.fatal_error: FatalError | None = None
        self.last_heartbeat: float = 0.0
        self.start_calls = 0
        self.stop_calls = 0

    async def start(self) -> None:
        self.start_calls += 1
        self._running = True
        self.started.set()
        try:
            if self._crash is not None:
                raise self._crash
            while True:
                try:
                    if self._hang_on_stop:
                        await self._released.wait()
                        return
                    await self._stop.wait()
                    return
                except asyncio.CancelledError:
                    if self._hang_on_stop and not self._released.is_set():
                        continue
                    raise
        finally:
            self._running = False
            self.stopped.set()

    def stop(self) -> None:
        self.stop_calls += 1
        self._stop.set()

    def release(self) -> None:
        self._hang_on_stop = False
        self._released.set()
        self._stop.set()

    def is_alive(self, max_silence: float | None = None) -> bool:
        return self._running

    async def wait_started(self, timeout: float = 1) -> None:
        await asyncio.wait_for(self.started.wait(), timeout=timeout)


def _tracking(
    name: str,
    *,
    hang_on_stop: bool = False,
    crash: BaseException | None = None,
) -> tuple[Callable[[], _FakeWorker], list[_FakeWorker]]:
    created: list[_FakeWorker] = []

    def factory() -> _FakeWorker:
        worker = _FakeWorker(name, hang_on_stop=hang_on_stop, crash=crash)
        created.append(worker)
        return worker

    return factory, created


async def test_fake_worker_stop_unblocks_start() -> None:
    worker = _FakeWorker("ingestion")
    task = asyncio.create_task(worker.start())
    await worker.wait_started()
    assert worker.is_alive()
    assert worker.start_calls == 1

    worker.stop()
    await asyncio.wait_for(task, timeout=1)
    assert not worker.is_alive()
    assert worker.stop_calls == 1


async def test_fake_worker_hang_swallows_cancel_until_release() -> None:
    worker = _FakeWorker("dispatch", hang_on_stop=True)
    task = asyncio.create_task(worker.start())
    await worker.wait_started()

    worker.stop()
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    assert worker.is_alive()

    worker.release()
    await asyncio.wait_for(task, timeout=1)
    assert not worker.is_alive()


async def test_start_all_keeps_tasks_running() -> None:
    ingestion_factory, ingestion = _tracking("ingestion")
    dispatch_factory, dispatch = _tracking("dispatch")
    manager = WorkerManager(
        {"ingestion": ingestion_factory, "dispatch": dispatch_factory}
    )

    manager.start_all()
    await ingestion[0].wait_started()
    await dispatch[0].wait_started()

    assert manager.worker("ingestion") is ingestion[0]
    assert manager.worker("dispatch") is dispatch[0]
    assert manager.task_state("ingestion").done is False
    assert manager.task_state("ingestion").exception is None
    assert manager.task_state("dispatch").done is False
    assert len(ingestion) == 1
    assert len(dispatch) == 1

    await manager.stop_all(timeout=1)


async def test_failed_worker_does_not_stop_neighbor() -> None:
    boom = RuntimeError("boom")
    ingestion_factory, ingestion = _tracking("ingestion")
    dispatch_factory, dispatch = _tracking("dispatch", crash=boom)
    manager = WorkerManager(
        {"ingestion": ingestion_factory, "dispatch": dispatch_factory}
    )

    manager.start_all()
    await ingestion[0].wait_started()
    await dispatch[0].wait_started()
    await asyncio.sleep(0)

    dispatch_state = manager.task_state("dispatch")
    assert dispatch_state.done is True
    assert dispatch_state.exception is boom
    assert manager.task_state("ingestion").done is False
    assert ingestion[0].is_alive()

    await manager.stop_all(timeout=1)


async def test_stop_all_stops_workers_and_tasks() -> None:
    ingestion_factory, ingestion = _tracking("ingestion")
    dispatch_factory, dispatch = _tracking("dispatch")
    manager = WorkerManager(
        {"ingestion": ingestion_factory, "dispatch": dispatch_factory}
    )
    manager.start_all()
    await ingestion[0].wait_started()
    await dispatch[0].wait_started()

    await manager.stop_all(timeout=1)

    assert ingestion[0].stop_calls == 1
    assert dispatch[0].stop_calls == 1
    assert not ingestion[0].is_alive()
    assert not dispatch[0].is_alive()
    assert manager.task_state("ingestion").done is True
    assert manager.task_state("dispatch").done is True


async def test_restart_replaces_only_named_worker() -> None:
    ingestion_factory, ingestion = _tracking("ingestion")
    dispatch_factory, dispatch = _tracking("dispatch")
    manager = WorkerManager(
        {"ingestion": ingestion_factory, "dispatch": dispatch_factory}
    )
    manager.start_all()
    await ingestion[0].wait_started()
    await dispatch[0].wait_started()
    old_dispatch = dispatch[0]
    old_ingestion = ingestion[0]

    await manager.restart("dispatch")
    await dispatch[1].wait_started()

    assert old_dispatch.stop_calls == 1
    assert not old_dispatch.is_alive()
    assert len(dispatch) == 2
    assert dispatch[1] is not old_dispatch
    assert manager.worker("dispatch") is dispatch[1]
    assert manager.task_state("dispatch").done is False
    assert manager.worker("ingestion") is old_ingestion
    assert manager.task_state("ingestion").done is False
    assert len(ingestion) == 1

    await manager.stop_all(timeout=1)


async def test_stuck_restart_does_not_spawn_second_task() -> None:
    factory, created = _tracking("stuck", hang_on_stop=True)
    manager = WorkerManager({"stuck": factory}, restart_join_timeout=0.05)
    manager.start_all()
    await created[0].wait_started()

    with pytest.raises(WorkerRestartStuck):
        await manager.restart("stuck")

    assert len(created) == 1
    assert manager.worker("stuck") is created[0]
    assert manager.task_state("stuck").done is False

    created[0].release()
    await manager.stop_all(timeout=1)


async def test_stop_all_returns_if_worker_ignores_cancel() -> None:
    factory, created = _tracking("stuck", hang_on_stop=True)
    manager = WorkerManager({"stuck": factory})
    manager.start_all()
    await created[0].wait_started()

    timeout = 0.15
    started_at = time.monotonic()
    await manager.stop_all(timeout=timeout)
    elapsed = time.monotonic() - started_at
    assert elapsed < timeout + 0.05
    assert manager.task_state("stuck").done is False

    created[0].release()
    await asyncio.wait_for(created[0].stopped.wait(), timeout=1)


async def test_start_all_twice_does_not_duplicate_running_task() -> None:
    factory, created = _tracking("ingestion")
    manager = WorkerManager({"ingestion": factory})
    manager.start_all()
    await created[0].wait_started()

    with pytest.raises(WorkerAlreadyRunning, match="ещё работает"):
        manager.start_all()
    assert len(created) == 1

    await manager.stop_all(timeout=1)


async def test_restart_respawns_finished_worker() -> None:
    created: list[_FakeWorker] = []
    boom = RuntimeError("boom")

    def factory() -> _FakeWorker:
        crash = boom if not created else None
        worker = _FakeWorker("dispatch", crash=crash)
        created.append(worker)
        return worker

    manager = WorkerManager({"dispatch": factory})
    manager.start_all()
    await created[0].wait_started()
    await asyncio.sleep(0)
    assert manager.task_state("dispatch").done is True

    await manager.restart("dispatch")
    await created[1].wait_started()

    assert len(created) == 2
    assert manager.worker("dispatch") is created[1]
    assert manager.task_state("dispatch").done is False

    await manager.stop_all(timeout=1)
