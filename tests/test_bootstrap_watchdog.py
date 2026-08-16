import asyncio

import pytest
from pydantic import ValidationError

from datetime import datetime

from src.application.health_store import HealthStore
from src.application.watchdog.worker import Watchdog
from src.application.worker_manager import WorkerManager
from src.bootstrap import on_watchdog_task_done, shutdown_runtime
from src.config.settings import Settings, WatchdogSettings
from src.domain.entities.edge_event import EdgeEvent
from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.exceptions import ProcessMustExit
from src.domain.ports.event_repository import EventRepository, StoredEvent
from src.domain.ports.host_metrics import HostMetrics, HostResourceSample
from src.domain.ports.storage_monitor import StorageMonitor
from src.domain.ports.telemetry_repository import StoredPacket, TelemetryRepository


class _NoopEvents(EventRepository):
    async def save(self, event: EdgeEvent) -> None:
        return None

    async def get_pending(self, limit: int) -> list[StoredEvent]:
        return []

    async def set_sent(self, ids: list[int]) -> None:
        return None


class _NoopTelemetry(TelemetryRepository):
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


class _NoopStorage(StorageMonitor):
    def free_bytes(self) -> int:
        return 1


class _NoopMetrics(HostMetrics):
    def sample(self) -> HostResourceSample:
        return HostResourceSample(cpu_percent=0.0, memory_percent=0.0, disk_percent=0.0)


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
        events=_NoopEvents(),
        telemetry=_NoopTelemetry(),
        storage=_NoopStorage(),
        host_metrics=_NoopMetrics(),
        health=HealthStore(),
        settings=WatchdogSettings(
            interval=5.0,
            restart_limit=3,
            restart_window_seconds=60.0,
        ),
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


def test_hung_source_on_shutdown_exits_with_1(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src.bootstrap import _exit_if_source_hung
    from src.domain.exceptions import RadarSourceHung

    class _Hung:
        def raise_if_hung(self) -> None:
            raise RadarSourceHung("источник не вернул управление")

    exited: list[int] = []
    monkeypatch.setattr("src.bootstrap.os._exit", exited.append)
    _exit_if_source_hung(_Hung())  # type: ignore[arg-type]
    assert exited == [1]


def test_restart_join_must_be_shorter_than_shutdown() -> None:
    with pytest.raises(ValidationError, match="restart_join_timeout"):
        Settings(restart_join_timeout=10.0, shutdown_timeout=8.0)


def test_restart_join_shorter_than_shutdown_is_valid() -> None:
    settings = Settings(restart_join_timeout=5.0, shutdown_timeout=8.0)
    assert settings.restart_join_timeout < settings.shutdown_timeout


def test_mock_radar_and_gateway_fail_rates_are_independent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MOCK_RADAR__FAIL_RATE", "0.1")
    monkeypatch.setenv("MOCK_GATEWAY__FAIL_RATE", "0.9")
    loaded = Settings()
    assert loaded.mock_radar.fail_rate == 0.1
    assert loaded.mock_gateway.fail_rate == 0.9
