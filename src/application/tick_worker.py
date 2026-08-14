import asyncio
import time
from abc import ABC, abstractmethod

from loguru import logger

from src.application.ports.worker import FatalError


class TickWorker(ABC):
    def __init__(
        self,
        name: str,
        interval: float,
        max_consecutive_failures: int = 10,
        heartbeat_max_silence: float = 30.0,
    ) -> None:
        self.name = name
        self._interval = interval
        self._max_consecutive_failures = max_consecutive_failures
        self._heartbeat_max_silence = heartbeat_max_silence
        self._running = False
        self._last_heartbeat: float = 0.0
        self._consecutive_failures = 0
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

                if self._consecutive_failures >= self._max_consecutive_failures:
                    self._log.critical(
                        "Превышен лимит подряд идущих ошибок ({}), остановка воркера",
                        self._consecutive_failures,
                    )
                    self._fatal_error = FatalError(
                        reason=(
                            f"too many consecutive failures "
                            f"({self._consecutive_failures})"
                        ),
                        restartable=False,
                    )
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
