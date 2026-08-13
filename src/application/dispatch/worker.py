import asyncio
import time

from loguru import logger

from src.application.dispatch.service import DispatchService
from src.application.ports.worker import FatalError
from src.config.settings import DispatchSettings
from src.domain.exceptions import (
    DispatchMisconfigured,
    NetworkUnavailable,
    TelemetryStorageError,
)


class DispatchWorker:
    def __init__(
        self,
        name: str,
        service: DispatchService,
        settings: DispatchSettings,
        max_consecutive_failures: int = 10,
    ) -> None:
        self.name = name
        self._service = service
        self._settings = settings
        self._max_consecutive_failures = max_consecutive_failures
        self._running = False
        self._last_heartbeat: float = 0.0
        self._consecutive_failures = 0
        self._fatal_error: FatalError | None = None

    @property
    def fatal_error(self) -> FatalError | None:
        return self._fatal_error

    async def start(self) -> None:
        self._running = True
        self._fatal_error = None
        logger.bind(worker=self.name).info("Воркер запущен")

        while self._running:
            tick_started_at = time.monotonic()
            await self._safe_tick()
            self._last_heartbeat = time.monotonic()

            if self._fatal_error is not None:
                break

            if self._consecutive_failures >= self._max_consecutive_failures:
                logger.bind(worker=self.name).critical(
                    "Превышен лимит подряд идущих ошибок ({}), остановка воркера",
                    self._consecutive_failures,
                )
                self._fatal_error = FatalError(
                    reason=(
                        f"too many consecutive failures ({self._consecutive_failures})"
                    ),
                    restartable=False,
                )
                self._running = False
                break

            elapsed = time.monotonic() - tick_started_at
            await asyncio.sleep(max(0.0, self._settings.interval - elapsed))

    async def _safe_tick(self) -> None:
        try:
            sent = await self._service.dispatch_once()
            if sent:
                logger.bind(worker=self.name).info("Отправлено {} пакетов", sent)
            self._consecutive_failures = 0
        except NetworkUnavailable as e:
            logger.bind(worker=self.name).warning(
                "Сети нет, пакеты копятся в outbox: {}", e
            )
        except DispatchMisconfigured as e:
            logger.bind(worker=self.name).critical(
                "Требуется вмешательство в конфигурацию, остановка воркера: {}", e
            )
            self._fatal_error = FatalError(
                reason=str(e) or "dispatch misconfigured",
                restartable=False,
            )
            self._running = False
        except TelemetryStorageError as e:
            self._consecutive_failures += 1
            logger.bind(worker=self.name).error(
                "Ошибка хранилища ({}/{}): {}",
                self._consecutive_failures,
                self._max_consecutive_failures,
                e,
            )

    def is_alive(self, max_silence: float = 30.0) -> bool:
        if not self._running:
            return False
        return (time.monotonic() - self._last_heartbeat) < max_silence

    def stop(self) -> None:
        self._running = False
        logger.bind(worker=self.name).info("Воркер остановлен")
