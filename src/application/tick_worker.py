import asyncio
import time
from abc import ABC, abstractmethod
from datetime import UTC, datetime

from loguru import logger

from src.application.constants import LOG_INTERVAL_S
from src.application.wait import sleep_until_event
from src.domain.value_objects.fatal_error import FatalError


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
        self._last_log_at: dict[str, float] = {}
        self._last_error: str | None = None
        self._last_error_at: datetime | None = None
        self._fatal_error: FatalError | None = None
        self._stop = asyncio.Event()
        self._log = logger.bind(name=name)

    @property
    def fatal_error(self) -> FatalError | None:
        return self._fatal_error

    @property
    def last_heartbeat(self) -> float:
        return self._last_heartbeat

    @property
    def last_error(self) -> str | None:
        return self._last_error

    @property
    def last_error_at(self) -> datetime | None:
        return self._last_error_at

    @property
    def storage_paused(self) -> bool:
        return False

    def _on_started(self) -> None:
        return None

    def _should_log(self, key: str, *, interval: float = LOG_INTERVAL_S) -> bool:
        now = time.monotonic()
        last = self._last_log_at.get(key, 0.0)
        if last != 0.0 and now - last < interval:
            return False
        self._last_log_at[key] = now
        return True

    def _clear_log_throttle(self, key: str) -> None:
        self._last_log_at.pop(key, None)

    def _note_error(self, message: str) -> None:
        self._last_error = message
        self._last_error_at = datetime.now(tz=UTC)

    def _set_fatal_error(self, error: FatalError) -> None:
        self._note_error(error.reason)
        self._fatal_error = error
        self._running = False

    def _stop_misconfigured(self, error: BaseException, *, fallback: str) -> None:
        self._log.critical(
            "Требуется вмешательство в конфигурацию, остановка воркера: {}", error
        )
        self._set_fatal_error(
            FatalError(reason=str(error) or fallback, restartable=False)
        )

    def _log_tick_failure(self, level: str, template: str, *args: object) -> None:
        message = template
        for arg in args:
            message = message.replace("{}", str(arg), 1)
        self._note_error(message)
        if not self._should_log("failure"):
            return
        log = getattr(self._log, level)
        if self._consecutive_failures <= 1:
            log(template, *args)
            return
        log(template + " ({} сбоев подряд)", *args, self._consecutive_failures)

    def _reset_failures(self) -> None:
        self._consecutive_failures = 0
        self._clear_log_throttle("failure")
        self._last_error = None
        self._last_error_at = None

    @abstractmethod
    async def _safe_tick(self) -> None: ...

    async def start(self) -> None:
        self._running = True
        self._fatal_error = None
        self._last_error = None
        self._last_error_at = None
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
                await sleep_until_event(
                    self._stop, max(0.0, self._interval - elapsed)
                )
        finally:
            self._running = False
            self._log.info("Воркер завершил цикл")

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
