import asyncio
import time

from src.application.ingestion.service import IngestionService
from src.application.tick_worker import TickWorker
from src.config.settings import IngestionSettings
from src.domain.constants import BYTES_IN_MIB
from src.domain.entities.edge_event import EdgeEvent
from src.domain.exceptions import (
    RadarDataInvalid,
    RadarSourceHung,
    RadarSourceMisconfigured,
    RadarSourceUnavailable,
    StorageError,
)
from src.domain.ports.event_repository import EventRepository
from src.domain.ports.storage_monitor import StorageMonitor
from src.domain.value_objects.fatal_error import FatalError


class IngestionWorker(TickWorker):
    def __init__(
        self,
        name: str,
        service: IngestionService,
        settings: IngestionSettings,
        storage_monitor: StorageMonitor,
        event_repository: EventRepository,
        heartbeat_max_silence: float = 60.0,
    ) -> None:
        super().__init__(
            name=name,
            interval=settings.interval,
            heartbeat_max_silence=heartbeat_max_silence,
        )
        self._service = service
        self._settings = settings
        self._storage_monitor = storage_monitor
        self._events = event_repository
        self._storage_paused = False
        self._radar_down = False
        self._radar_down_since: float = 0.0

    @property
    def storage_paused(self) -> bool:
        return self._storage_paused

    async def _safe_tick(self) -> None:
        try:
            if not self._storage_allows_write():
                return
        except StorageError as e:
            self._consecutive_failures += 1
            self._log_tick_failure(
                "error",
                "Не удалось проверить свободное место: {}",
                e,
            )
            return

        try:
            async with asyncio.timeout(self._settings.tick_deadline):
                await self._service.collect_once()
                await self._on_success()
        except TimeoutError:
            self._on_tick_deadline_exceeded()
        except RadarSourceUnavailable as e:
            await self._on_source_unavailable(e)
        except RadarSourceMisconfigured as e:
            self._stop_misconfigured(e, fallback="radar source misconfigured")
        except StorageError as e:
            self._consecutive_failures += 1
            self._log_tick_failure("error", "Пакет собран, но не сохранён: {}", e)
        except RadarDataInvalid as e:
            self._log.warning("Некорректные данные, пропуск тика: {}", e)

    def _on_tick_deadline_exceeded(self) -> None:
        try:
            self._service.raise_if_hung()
        except RadarSourceHung as e:
            self._log.critical(
                "Чтение источника пережило дедлайн тика, остановка воркера: {}", e
            )
            self._set_fatal_error(FatalError(reason=str(e), restartable=True))
            return

        self._consecutive_failures += 1
        self._log_tick_failure("error", "Тик превысил дедлайн")

    async def _on_source_unavailable(self, error: RadarSourceUnavailable) -> None:
        self._consecutive_failures += 1
        self._log_tick_failure("warning", "Источник недоступен: {}", error)
        if self._radar_down:
            return
        try:
            await self._events.save(EdgeEvent.radar_down(reason=str(error)))
        except StorageError as e:
            self._log_tick_failure("error", "Не удалось записать radar_down: {}", e)
            return
        self._radar_down = True
        self._radar_down_since = time.monotonic()

    async def _on_success(self) -> None:
        if self._radar_down:
            downtime_seconds = time.monotonic() - self._radar_down_since
            try:
                await self._events.save(
                    EdgeEvent.radar_recovered(downtime_seconds=downtime_seconds)
                )
            except StorageError as e:
                self._consecutive_failures += 1
                self._log_tick_failure(
                    "error",
                    "Не удалось записать radar_recovered: {}",
                    e,
                )
                return
            self._radar_down = False
            self._radar_down_since = 0.0
        self._reset_failures()

    def _storage_allows_write(self) -> bool:
        free_bytes = self._storage_monitor.free_bytes()
        if self._storage_paused:
            if free_bytes >= self._settings.resume_free_bytes:
                self._storage_paused = False
                self._clear_log_throttle("storage")
                self._log.info(
                    "Свободное место восстановлено ({} MiB), сбор возобновлён",
                    free_bytes // BYTES_IN_MIB,
                )
                return True
            self._log_storage_paused(free_bytes)
            return False
        if free_bytes < self._settings.min_free_bytes:
            self._storage_paused = True
            self._log_storage_paused(free_bytes)
            return False
        return True

    def _log_storage_paused(self, free_bytes: int) -> None:
        if not self._should_log("storage"):
            return
        self._log.error(
            "Недостаточно места на диске БД (свободно {} MiB, пауза ниже {} MiB), "
            "чтение пакета пропущено; pending не удаляются",
            free_bytes // BYTES_IN_MIB,
            self._settings.min_free_mb,
        )
