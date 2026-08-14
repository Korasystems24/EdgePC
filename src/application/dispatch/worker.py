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

_NETWORK_LOG_INTERVAL_S = 60.0


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
        self._deadline: float = 0.0
        self._network_unavailable = False
        self._last_network_log_at: float = 0.0

    @property
    def fatal_error(self) -> FatalError | None:
        return self._fatal_error

    async def start(self) -> None:
        self._running = True
        self._fatal_error = None
        logger.bind(worker=self.name).info("Воркер запущен")
        self._set_deadline()
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
            await self._clear_ttl(self._deadline)
            if self._network_unavailable and sent:
                self._network_unavailable = False
                self._last_network_log_at = 0.0
                logger.bind(worker=self.name).info(
                    "Сеть восстановилась, отправлено {} пакетов", sent
                )
            elif sent:
                logger.bind(worker=self.name).info("Отправлено {} пакетов", sent)
            self._consecutive_failures = 0
        except NetworkUnavailable as e:
            self._network_unavailable = True
            self._log_network_unavailable(e)
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

    def _log_network_unavailable(self, error: NetworkUnavailable) -> None:
        now = time.monotonic()
        if now - self._last_network_log_at < _NETWORK_LOG_INTERVAL_S:
            return
        self._last_network_log_at = now
        logger.bind(worker=self.name).warning(
            "Сети нет, пакеты копятся в outbox: {}", error
        )

    def is_alive(self, max_silence: float = 30.0) -> bool:
        if not self._running:
            return False
        return (time.monotonic() - self._last_heartbeat) < max_silence

    async def _clear_ttl(self, deadline: float) -> None:
        if time.monotonic() > deadline:
            await self._service.delete_sent_older_than()
            logger.bind(worker=self.name).info(
                "Удалены отправленные пакеты, установлен новый дедлайн"
            )
            self._set_deadline()

    def _set_deadline(self) -> None:
        self._deadline = (
            time.monotonic() + self._settings.waiting_period_ttl.total_seconds()
        )

    def stop(self) -> None:
        self._running = False
        logger.bind(worker=self.name).info("Воркер остановлен")
