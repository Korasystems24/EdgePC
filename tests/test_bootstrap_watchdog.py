import asyncio

import pytest
from pydantic import ValidationError

from src.application.watchdog.worker import Watchdog
from src.application.worker_manager import WorkerManager
from src.bootstrap import on_watchdog_task_done, shutdown_runtime
from src.config.settings import Settings
from src.domain.exceptions import ProcessMustExit


async def test_watchdog_callback_ignores_cancel(monkeypatch: pytest.MonkeyPatch) -> None:
    exited: list[int] = []
    monkeypatch.setattr("src.bootstrap.os._exit", exited.append)
    task = asyncio.create_task(asyncio.sleep(60))
    task.add_done_callback(on_watchdog_task_done)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert exited == []


async def test_watchdog_callback_ignores_clean_stop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exited: list[int] = []
    monkeypatch.setattr("src.bootstrap.os._exit", exited.append)

    async def _ok() -> None:
        return None

    task = asyncio.create_task(_ok())
    task.add_done_callback(on_watchdog_task_done)
    await task
    assert exited == []


async def test_watchdog_callback_exits_on_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exited: list[int] = []
    monkeypatch.setattr("src.bootstrap.os._exit", exited.append)

    async def _boom() -> None:
        raise RuntimeError("watchdog down")

    task = asyncio.create_task(_boom())
    task.add_done_callback(on_watchdog_task_done)
    await asyncio.gather(task, return_exceptions=True)
    assert exited == [1]


async def test_shutdown_does_not_exit_if_watchdog_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    exited: list[int] = []
    monkeypatch.setattr("src.bootstrap.os._exit", exited.append)

    async def _stuck() -> None:
        await asyncio.sleep(0.05)
        raise ProcessMustExit("воркер dispatch не завершился за 5.0с")

    watchdog = Watchdog(
        WorkerManager({}),
        interval=5.0,
        restart_limit=3,
        restart_window_seconds=60.0,
    )
    task = asyncio.create_task(_stuck())
    task.add_done_callback(on_watchdog_task_done)
    await shutdown_runtime(
        watchdog,
        task,
        None,
        None,
        None,
        None,
        timeout=1.0,
    )
    assert exited == []
    assert task.done()


def test_restart_join_must_be_shorter_than_shutdown() -> None:
    with pytest.raises(ValidationError, match="restart_join_timeout"):
        Settings(restart_join_timeout=10.0, shutdown_timeout=8.0)


def test_restart_join_shorter_than_shutdown_is_valid() -> None:
    settings = Settings(restart_join_timeout=5.0, shutdown_timeout=8.0)
    assert settings.restart_join_timeout < settings.shutdown_timeout
