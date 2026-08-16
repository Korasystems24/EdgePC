import asyncio
import time

from src.application.dispatch.service import DispatchService
from src.application.ports.worker import FatalError
from src.application.tick_worker import TickWorker
from src.config.settings import DispatchSettings
from src.domain.exceptions import (
    DispatchMisconfigured,
    NetworkUnavailable,
    TelemetryStorageError,
)

_NETWORK_LOG_INTERVAL_S = 60.0


class DispatchWorker(TickWorker):
    def __init__(
        self,
        name: str,
        service: DispatchService,
        settings: DispatchSettings,
        heartbeat_max_silence: float = 60.0,
    ) -> None:
        super().__init__(
            name=name,
            interval=settings.interval,
            heartbeat_max_silence=heartbeat_max_silence,
        )
        self._service = service
        self._settings = settings
        self._retention_deadline: float = 0.0
        self._network_unavailable = False
        self._last_network_log_at: float = 0.0

    def _on_started(self) -> None:
        self._set_retention_deadline()

    async def _safe_tick(self) -> None:
        try:
            async with asyncio.timeout(self._settings.tick_deadline):
                sent = await self._service.dispatch_once()
                await self._clear_ttl(self._retention_deadline)
            if self._network_unavailable and sent:
                self._network_unavailable = False
                self._last_network_log_at = 0.0
                self._log.info(
                    "Сеть восстановилась, отправлено {} пакетов", sent
                )
            elif sent:
                self._log.info("Отправлено {} пакетов", sent)
            self._reset_failures()
        except TimeoutError:
            self._consecutive_failures += 1
            self._log_tick_failure("error", "Тик превысил дедлайн")
        except NetworkUnavailable as e:
            self._network_unavailable = True
            self._log_network_unavailable(e)
        except DispatchMisconfigured as e:
            self._log.critical(
                "Требуется вмешательство в конфигурацию, остановка воркера: {}", e
            )
            self._fatal_error = FatalError(
                reason=str(e) or "dispatch misconfigured",
                restartable=False,
            )
            self._running = False
        except TelemetryStorageError as e:
            self._consecutive_failures += 1
            self._log_tick_failure("error", "Ошибка хранилища: {}", e)

    def _log_network_unavailable(self, error: NetworkUnavailable) -> None:
        now = time.monotonic()
        if (
            self._last_network_log_at != 0.0
            and now - self._last_network_log_at < _NETWORK_LOG_INTERVAL_S
        ):
            return
        self._last_network_log_at = now
        self._log.warning(
            "Сети нет, пакеты копятся в outbox: {}", error
        )

    async def _clear_ttl(self, deadline: float) -> None:
        if time.monotonic() > deadline:
            await self._service.delete_sent_older_than()
            self._log.info(
                "Удалены отправленные пакеты, установлен новый дедлайн"
            )
            self._set_retention_deadline()

    def _set_retention_deadline(self) -> None:
        self._retention_deadline = (
            time.monotonic() + self._settings.waiting_period_ttl.total_seconds()
        )
