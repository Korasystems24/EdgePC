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
from src.domain.ports.storage_monitor import StorageMonitor

_MIB = 1024 * 1024
_STORAGE_LOG_INTERVAL_S = 60.0


class IngestionWorker:
    def __init__(
        self,
        name: str,
        service: IngestionService,
        storage_monitor: StorageMonitor,
        interval: float = 5.0,
        min_free_bytes: int = 200 * _MIB,
        resume_free_bytes: int = 250 * _MIB,
        max_consecutive_failures: int = 10,
    ) -> None:
        self.name = name
        self._service = service
        self._storage_monitor = storage_monitor
        self._interval = interval
        self._min_free_bytes = min_free_bytes
        self._resume_free_bytes = resume_free_bytes
        self._max_consecutive_failures = max_consecutive_failures
        self._running = False
        self._last_heartbeat: float = 0.0
        self._consecutive_failures = 0
        self._fatal_error: FatalError | None = None
        self._storage_paused = False
        self._last_storage_log_at: float = 0.0

    @property
    def fatal_error(self) -> FatalError | None:
        return self._fatal_error

    @property
    def storage_paused(self) -> bool:
        return self._storage_paused

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
            if not self._storage_allows_write():
                return
        except TelemetryStorageError as e:
            self._consecutive_failures += 1
            logger.bind(worker=self.name).error(
                "Не удалось проверить свободное место ({}/{}): {}",
                self._consecutive_failures,
                self._max_consecutive_failures,
                e,
            )
            return

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

    def _storage_allows_write(self) -> bool:
        free_bytes = self._storage_monitor.free_bytes()
        if self._storage_paused:
            if free_bytes >= self._resume_free_bytes:
                self._storage_paused = False
                self._last_storage_log_at = 0.0
                logger.bind(worker=self.name).info(
                    "Свободное место восстановлено ({} MiB), сбор возобновлён",
                    free_bytes // _MIB,
                )
                return True
            self._log_storage_paused(free_bytes)
            return False
        if free_bytes < self._min_free_bytes:
            self._storage_paused = True
            self._log_storage_paused(free_bytes)
            return False
        return True

    def _log_storage_paused(self, free_bytes: int) -> None:
        now = time.monotonic()
        if now - self._last_storage_log_at < _STORAGE_LOG_INTERVAL_S:
            return
        self._last_storage_log_at = now
        logger.bind(worker=self.name).error(
            "Недостаточно места на диске БД (свободно {} MiB, пауза ниже {} MiB), "
            "чтение пакета пропущено; pending не удаляются",
            free_bytes // _MIB,
            self._min_free_bytes // _MIB,
        )

    def is_alive(self, max_silence: float = 30.0) -> bool:
        if not self._running:
            return False
        return (time.monotonic() - self._last_heartbeat) < max_silence

    def stop(self) -> None:
        self._running = False
        logger.bind(worker=self.name).info("Воркер остановлен")
