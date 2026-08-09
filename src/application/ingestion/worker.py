import asyncio
import time

from loguru import logger

from src.application.ingestion.service import IngestionService
from src.application.ports.worker import FatalError
from src.domain.exceptions import (
    RadarDataInvalid,
    RadarSourceMisconfigured,
    RadarSourceUnavailable,
    TelemetryStorageError,
)


class IngestionWorker:
    def __init__(
        self,
        name: str,
        service: IngestionService,
        interval: float = 5.0,
        max_consecutive_failures: int = 10,
    ) -> None:
        self.name = name
        self._service = service
        self._interval = interval
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
            await asyncio.sleep(max(0.0, self._interval - elapsed))

    async def _safe_tick(self) -> None:
        try:
            await self._service.collect_once()
            self._consecutive_failures = 0
        except RadarSourceUnavailable as e:
            self._consecutive_failures += 1
            logger.bind(worker=self.name).warning(
                "Источник недоступен ({}/{}): {}",
                self._consecutive_failures,
                self._max_consecutive_failures,
                e,
            )
        except RadarSourceMisconfigured as e:
            logger.bind(worker=self.name).critical(
                "Требуется вмешательство в конфигурацию, остановка воркера: {}", e
            )
            self._fatal_error = FatalError(
                reason=str(e) or "radar source misconfigured",
                restartable=False,
            )
            self._running = False
        except TelemetryStorageError as e:
            self._consecutive_failures += 1
            logger.bind(worker=self.name).error(
                "Пакет собран, но не сохранён ({}/{}): {}",
                self._consecutive_failures,
                self._max_consecutive_failures,
                e,
            )
        except RadarDataInvalid as e:
            logger.bind(worker=self.name).warning(
                "Некорректные данные, пропуск тика: {}", e
            )

    def is_alive(self, max_silence: float = 30.0) -> bool:
        if not self._running:
            return False
        return (time.monotonic() - self._last_heartbeat) < max_silence

    def stop(self) -> None:
        self._running = False
        logger.bind(worker=self.name).info("Воркер остановлен")
