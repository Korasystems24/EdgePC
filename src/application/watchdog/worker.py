import asyncio
import time
from collections import defaultdict, deque

from loguru import logger

from src.application.watchdog.policy import decide
from src.application.worker_manager import WorkerManager
from src.domain.enum import WatchdogAction
from src.domain.exceptions import ProcessMustExit, WorkerRestartStuck


class Watchdog:
    def __init__(
        self,
        manager: WorkerManager,
        *,
        interval: float,
        restart_limit: int,
        restart_window_seconds: float,
    ) -> None:
        self._manager = manager
        self._interval = interval
        self._restart_limit = restart_limit
        self._restart_window_seconds = restart_window_seconds
        self._restarts: dict[str, deque[float]] = defaultdict(deque)
        self._left_stopped: set[str] = set()
        self._stop = asyncio.Event()
        self._log = logger.bind(name="watchdog")

    async def start(self) -> None:
        self._log.info("Watchdog запущен")
        try:
            while not self._stop.is_set():
                await self.inspect()
                await self._sleep_until_stop(self._interval)
        finally:
            self._log.info("Watchdog остановлен")

    def stop(self) -> None:
        self._stop.set()

    async def inspect(self) -> None:
        for name in self._manager.names:
            if self._stop.is_set():
                return
            worker = self._manager.worker(name)
            action = decide(
                is_alive=worker.is_alive(),
                task_done=self._manager.task_state(name).done,
                fatal_error=worker.fatal_error,
                has_started=worker.last_heartbeat != 0,
            )
            if action is WatchdogAction.EXIT:
                reason = (
                    worker.fatal_error.reason if worker.fatal_error else "unknown"
                )
                raise ProcessMustExit(f"воркер {name}: {reason}")
            if action is WatchdogAction.LEAVE_STOPPED:
                if name not in self._left_stopped:
                    self._left_stopped.add(name)
                    reason = (
                        worker.fatal_error.reason
                        if worker.fatal_error
                        else "unknown"
                    )
                    self._log.warning(
                        "Воркер {} остановлен без рестарта: {}", name, reason
                    )
                continue
            if action is not WatchdogAction.RESTART:
                continue
            self._log.warning("Рестарт воркера {}", name)
            try:
                await self._manager.restart(name)
            except WorkerRestartStuck as exc:
                raise ProcessMustExit(str(exc)) from exc
            now = time.monotonic()
            stamps = self._restarts[name]
            stamps.append(now)
            while stamps and now - stamps[0] > self._restart_window_seconds:
                stamps.popleft()
            if len(stamps) >= self._restart_limit:
                raise ProcessMustExit(
                    f"воркер {name}: слишком много рестартов "
                    f"({len(stamps)} за {self._restart_window_seconds}с)"
                )

    async def _sleep_until_stop(self, seconds: float) -> None:
        if seconds <= 0:
            return
        try:
            async with asyncio.timeout(seconds):
                await self._stop.wait()
        except TimeoutError:
            return
