import asyncio
import time

from src.application.ingestion.service import IngestionService
from src.application.ports.worker import FatalError
from src.application.tick_worker import TickWorker
from src.domain.exceptions import (
    RadarDataInvalid,
    RadarSourceHung,
    RadarSourceMisconfigured,
    RadarSourceUnavailable,
    TelemetryStorageError,
)
from src.domain.ports.storage_monitor import StorageMonitor

_MIB = 1024 * 1024
_STORAGE_LOG_INTERVAL_S = 60.0


class IngestionWorker(TickWorker):
    def __init__(
        self,
        name: str,
        service: IngestionService,
        storage_monitor: StorageMonitor,
        interval: float = 5.0,
        tick_deadline: float = 8.0,
        min_free_bytes: int = 200 * _MIB,
        resume_free_bytes: int = 250 * _MIB,
        max_consecutive_failures: int = 10,
        heartbeat_max_silence: float = 30.0,
    ) -> None:
        super().__init__(
            name=name,
            interval=interval,
            max_consecutive_failures=max_consecutive_failures,
            heartbeat_max_silence=heartbeat_max_silence,
        )
        self._service = service
        self._storage_monitor = storage_monitor
        self._tick_deadline = tick_deadline
        self._min_free_bytes = min_free_bytes
        self._resume_free_bytes = resume_free_bytes
        self._storage_paused = False
        self._last_storage_log_at: float = 0.0

    @property
    def storage_paused(self) -> bool:
        return self._storage_paused

    async def _safe_tick(self) -> None:
        try:
            if not self._storage_allows_write():
                return
        except TelemetryStorageError as e:
            self._consecutive_failures += 1
            self._log.error(
                "Не удалось проверить свободное место ({}/{}): {}",
                self._consecutive_failures,
                self._max_consecutive_failures,
                e,
            )
            return

        try:
            async with asyncio.timeout(self._tick_deadline):
                await self._service.collect_once()
            self._consecutive_failures = 0
        except TimeoutError:
            self._on_tick_deadline_exceeded()
        except RadarSourceUnavailable as e:
            self._consecutive_failures += 1
            self._log.warning(
                "Источник недоступен ({}/{}): {}",
                self._consecutive_failures,
                self._max_consecutive_failures,
                e,
            )
        except RadarSourceMisconfigured as e:
            self._log.critical(
                "Требуется вмешательство в конфигурацию, остановка воркера: {}", e
            )
            self._fatal_error = FatalError(
                reason=str(e) or "radar source misconfigured",
                restartable=False,
            )
            self._running = False
        except TelemetryStorageError as e:
            self._consecutive_failures += 1
            self._log.error(
                "Пакет собран, но не сохранён ({}/{}): {}",
                self._consecutive_failures,
                self._max_consecutive_failures,
                e,
            )
        except RadarDataInvalid as e:
            self._log.warning(
                "Некорректные данные, пропуск тика: {}", e
            )

    def _on_tick_deadline_exceeded(self) -> None:
        try:
            self._service.raise_if_hung()
        except RadarSourceHung as e:
            self._log.critical(
                "Чтение источника пережило дедлайн тика, остановка воркера: {}", e
            )
            self._fatal_error = FatalError(reason=str(e), restartable=True)
            self._running = False
            return

        self._consecutive_failures += 1
        self._log.error(
            "Тик превысил дедлайн ({}/{})",
            self._consecutive_failures,
            self._max_consecutive_failures,
        )

    def _storage_allows_write(self) -> bool:
        free_bytes = self._storage_monitor.free_bytes()
        if self._storage_paused:
            if free_bytes >= self._resume_free_bytes:
                self._storage_paused = False
                self._last_storage_log_at = 0.0
                self._log.info(
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
        self._log.error(
            "Недостаточно места на диске БД (свободно {} MiB, пауза ниже {} MiB), "
            "чтение пакета пропущено; pending не удаляются",
            free_bytes // _MIB,
            self._min_free_bytes // _MIB,
        )
