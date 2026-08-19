import asyncio
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Literal

from loguru import logger

from src.application.failure_tracker import FailureTracker
from src.application.log_throttle import LogThrottle
from src.application.wait import sleep_until_event
from src.domain.value_objects.fatal_error import FatalError

type LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

FAILURE_LOG_KEY = "failure"


class TickWorker(ABC):
    """Цикл «отметил heartbeat → сделал работу → поспал остаток интервала».

    Наследник описывает только тело тика в `_safe_tick` и сообщает о результате
    через `_fail`/`_reset_failures`. Учёт ошибок живёт в `FailureTracker`,
    ограничение частоты логов — в `LogThrottle`.
    """

    def __init__(
        self,
        name: str,
        interval: float,
        heartbeat_max_silence: float,
    ) -> None:
        self.name = name
        self._interval = interval
        self._heartbeat_max_silence = heartbeat_max_silence
        self._running = False
        self._last_heartbeat: float = 0.0
        self._failures = FailureTracker()
        self._log_throttle = LogThrottle()
        self._stop = asyncio.Event()
        self._log = logger.bind(name=name)

    @property
    def fatal_error(self) -> FatalError | None:
        return self._failures.fatal_error

    @property
    def last_heartbeat(self) -> float:
        return self._last_heartbeat

    @property
    def last_error(self) -> str | None:
        return self._failures.last_error

    @property
    def last_error_at(self) -> datetime | None:
        return self._failures.last_error_at

    @property
    def storage_paused(self) -> bool:
        return False

    def _on_started(self) -> None:
        return None

    def _fail(self, level: LogLevel, template: str, *args: object) -> None:
        """Сбой тика: +1 к счётчику, запомнить ошибку, залогировать не чаще
        интервала троттлинга."""
        self._failures.record_failure(_render(template, *args))
        self._log_failure(level, template, *args)

    def _log_failure_only(
        self, level: LogLevel, template: str, *args: object
    ) -> None:
        """Ещё одна ошибка внутри уже посчитанного сбоя: счётчик не двигаем,
        иначе один неудачный тик даст два деления."""
        self._failures.note_error(_render(template, *args))
        self._log_failure(level, template, *args)

    def _log_failure(self, level: LogLevel, template: str, *args: object) -> None:
        if not self._log_throttle.allows(FAILURE_LOG_KEY):
            return
        failures = self._failures.consecutive
        if failures <= 1:
            self._log.log(level, template, *args)
            return
        self._log.log(level, template + " ({} сбоев подряд)", *args, failures)

    def _set_fatal_error(self, error: FatalError) -> None:
        self._failures.set_fatal(error)
        self._running = False

    def _stop_misconfigured(self, error: BaseException, *, fallback: str) -> None:
        self._log.critical(
            "Требуется вмешательство в конфигурацию, остановка воркера: {}", error
        )
        self._set_fatal_error(
            FatalError(reason=str(error) or fallback, restartable=False)
        )

    def _reset_failures(self) -> None:
        self._failures.reset()
        self._log_throttle.clear(FAILURE_LOG_KEY)

    @abstractmethod
    async def _safe_tick(self) -> None: ...

    async def start(self) -> None:
        self._running = True
        self._failures.clear_errors()
        self._stop.clear()
        self._last_heartbeat = time.monotonic()
        self._log.info("Воркер запущен")
        self._on_started()

        try:
            while self._running:
                tick_started_at = time.monotonic()
                self._last_heartbeat = tick_started_at
                await self._safe_tick()

                if self._failures.fatal_error is not None:
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


def _render(template: str, *args: object) -> str:
    """Тот же текст, что уйдёт в лог. `format` не разбирает подставленные
    значения, поэтому фигурные скобки внутри аргумента остаются текстом."""
    return template.format(*args)
