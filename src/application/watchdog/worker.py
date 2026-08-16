import asyncio
import time
from collections import defaultdict, deque
from datetime import UTC, datetime

from loguru import logger

from src.application.health_store import HealthStore
from src.application.wait import sleep_until_event
from src.application.watchdog.policy import decide
from src.application.worker_manager import WorkerManager
from src.config.settings import WatchdogSettings
from src.domain.entities.edge_event import EdgeEvent
from src.domain.entities.health_snapshot import HealthSnapshot
from src.domain.enum import WatchdogAction
from src.domain.exceptions import (
    ProcessMustExit,
    StorageError,
    WorkerRestartStuck,
)
from src.domain.ports.event_repository import EventRepository
from src.domain.ports.host_metrics import HostMetrics
from src.domain.ports.storage_monitor import StorageMonitor
from src.domain.ports.telemetry_repository import TelemetryRepository


class Watchdog:
    def __init__(
        self,
        manager: WorkerManager,
        *,
        events: EventRepository,
        telemetry: TelemetryRepository,
        storage: StorageMonitor,
        host_metrics: HostMetrics,
        health: HealthStore,
        settings: WatchdogSettings,
    ) -> None:
        self._manager = manager
        self._events = events
        self._telemetry = telemetry
        self._storage = storage
        self._host_metrics = host_metrics
        self._health = health
        self._settings = settings
        self._restarts: dict[str, deque[float]] = defaultdict(deque)
        self._left_stopped: set[str] = set()
        self._stop = asyncio.Event()
        self._log = logger.bind(name="watchdog")

    async def start(self) -> None:
        self._log.info("Watchdog запущен")
        try:
            while not self._stop.is_set():
                await self.inspect()
                await sleep_until_event(self._stop, self._settings.interval)
        finally:
            self._log.info("Watchdog остановлен")

    def stop(self) -> None:
        self._stop.set()

    async def inspect(self) -> None:
        try:
            async with asyncio.timeout(self._settings.inspect_timeout):
                try:
                    for name in self._manager.names:
                        if self._stop.is_set():
                            break
                        await self._inspect_worker(name)
                finally:
                    await self._refresh_health()
        except TimeoutError:
            self._log.error("Inspect превысил таймаут, пропускаем этот цикл")

    async def _inspect_worker(self, name: str) -> None:
        await self._apply(name, self._decide(name))

    def _decide(self, name: str) -> WatchdogAction:
        worker = self._manager.worker(name)
        return decide(
            is_alive=worker.is_alive(),
            task_done=self._manager.task_state(name).done,
            fatal_error=worker.fatal_error,
            has_started=worker.last_heartbeat != 0,
        )

    async def _apply(self, name: str, action: WatchdogAction) -> None:
        match action:
            case WatchdogAction.KEEP:
                return
            case WatchdogAction.EXIT:
                await self._exit_worker(name)
            case WatchdogAction.LEAVE_STOPPED:
                await self._leave_stopped(name)
            case WatchdogAction.RESTART:
                await self._restart_worker(name)

    async def _exit_worker(self, name: str) -> None:
        worker = self._manager.worker(name)
        reason = worker.fatal_error.reason if worker.fatal_error else "unknown"
        restartable = (
            worker.fatal_error.restartable if worker.fatal_error else True
        )
        await self._record_event(
            EdgeEvent.fatal_error(
                worker=name,
                reason=reason,
                restartable=restartable,
            )
        )
        raise ProcessMustExit(f"воркер {name}: {reason}")

    async def _leave_stopped(self, name: str) -> None:
        if name in self._left_stopped:
            return
        worker = self._manager.worker(name)
        reason = worker.fatal_error.reason if worker.fatal_error else "unknown"
        recorded = await self._record_event(
            EdgeEvent.fatal_error(
                worker=name,
                reason=reason,
                restartable=False,
            )
        )
        if not recorded:
            return
        self._left_stopped.add(name)
        self._log.warning("Воркер {} остановлен без рестарта: {}", name, reason)

    async def _restart_worker(self, name: str) -> None:
        self._log.warning("Рестарт воркера {}", name)
        try:
            await self._manager.restart(name)
        except WorkerRestartStuck as exc:
            await self._record_event(
                EdgeEvent.fatal_error(
                    worker=name,
                    reason=str(exc),
                    restartable=True,
                )
            )
            raise ProcessMustExit(str(exc)) from exc
        await self._record_event(EdgeEvent.worker_restarted(worker=name))
        if self._too_many_restarts(name):
            reason = (
                f"слишком много рестартов "
                f"({len(self._restarts[name])} за {self._settings.restart_window_seconds}с)"
            )
            await self._record_event(
                EdgeEvent.fatal_error(
                    worker=name,
                    reason=reason,
                    restartable=True,
                )
            )
            raise ProcessMustExit(f"воркер {name}: {reason}")

    def _too_many_restarts(self, name: str) -> bool:
        now = time.monotonic()
        stamps = self._restarts[name]
        stamps.append(now)
        window = self._settings.restart_window_seconds
        while stamps and now - stamps[0] > window:
            stamps.popleft()
        return len(stamps) >= self._settings.restart_limit

    async def _record_event(self, event: EdgeEvent) -> bool:
        try:
            await self._events.save(event)
        except StorageError as exc:
            self._log.error("Не удалось записать событие {}: {}", event.type, exc)
            return False
        return True

    async def _refresh_health(self) -> None:
        try:
            pending = await self._telemetry.get_pending_count()
            quarantined = await self._telemetry.get_quarantined_count()
            free_bytes = self._storage.free_bytes()
            sample = self._host_metrics.sample()
            self._health.put(
                HealthSnapshot(
                    ts=datetime.now(tz=UTC),
                    workers=self._manager.health_report(),
                    pending_count=pending,
                    quarantined_count=quarantined,
                    free_bytes=free_bytes,
                    storage_paused=self._storage_paused(),
                    cpu_percent=sample.cpu_percent,
                    memory_percent=sample.memory_percent,
                    disk_percent=sample.disk_percent,
                )
            )
        except StorageError as exc:
            self._log.error("Не удалось собрать health-снапшот: {}", exc)

    def _storage_paused(self) -> bool:
        return any(
            self._manager.worker(name).storage_paused
            for name in self._manager.names
        )
