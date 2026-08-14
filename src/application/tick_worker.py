import asyncio
import time
from abc import ABC, abstractmethod

from loguru import logger

from src.application.ports.worker import FatalError

_FAILURE_LOG_INTERVAL_S = 60.0


class TickWorker(ABC):
    def __init__(
        self,
        name: str,
        interval: float,
        heartbeat_max_silence: float = 60.0,
    ) -> None:
        self.name = name
        self._interval = interval
        self._heartbeat_max_silence = heartbeat_max_silence
        self._running = False
        self._last_heartbeat: float = 0.0
        self._consecutive_failures = 0
        self._last_repeat_log_at: float = 0.0
        self._fatal_error: FatalError | None = None
        self._stop = asyncio.Event()
        self._log = logger.bind(name=name)

    @property
    def fatal_error(self) -> FatalError | None:
        return self._fatal_error

    @property
    def last_heartbeat(self) -> float:
        return self._last_heartbeat

    def _on_started(self) -> None:
        return None

    def _should_log_failure(self) -> bool:
        now = time.monotonic()
        if (
            self._last_repeat_log_at != 0.0
            and now - self._last_repeat_log_at < _FAILURE_LOG_INTERVAL_S
        ):
            return False
        self._last_repeat_log_at = now
        return True

    def _log_tick_failure(self, level: str, template: str, *args: object) -> None:
        if not self._should_log_failure():
            return
        log = getattr(self._log, level)
        if self._consecutive_failures <= 1:
            log(template, *args)
            return
        log(template + " ({} сбоев подряд)", *args, self._consecutive_failures)

    def _reset_failures(self) -> None:
        self._consecutive_failures = 0
        self._last_repeat_log_at = 0.0

    @abstractmethod
    async def _safe_tick(self) -> None: ...

    async def start(self) -> None:
        self._running = True
        self._fatal_error = None
        self._stop.clear()
        self._last_heartbeat = time.monotonic()
        self._log.info("Воркер запущен")
        self._on_started()

        try:
            while self._running:
                tick_started_at = time.monotonic()
                self._last_heartbeat = tick_started_at
                await self._safe_tick()

                if self._fatal_error is not None:
                    break

                elapsed = time.monotonic() - tick_started_at
                await self._sleep_until_stop(max(0.0, self._interval - elapsed))
        finally:
            self._running = False
            self._log.info("Воркер завершил цикл")

    async def _sleep_until_stop(self, seconds: float) -> None:
        if seconds <= 0:
            return
        try:
            async with asyncio.timeout(seconds):
                await self._stop.wait()
        except TimeoutError:
            return

    def is_alive(self, max_silence: float | None = None) -> bool:
        if not self._running:
            return False
        silence = (
            self._heartbeat_max_silence if max_silence is None else max_silence
        )
        return (time.monotonic() - self._last_heartbeat) < silence

    def stop(self) -> None:
        self._running = False
        self._stop.set()
        self._log.info("Запрошена остановка воркера")
