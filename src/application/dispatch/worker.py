import asyncio
import time

from src.application.constants import RETENTION_TIMEOUT_S
from src.application.dispatch.service import DispatchService
from src.application.tick_worker import TickWorker
from src.config.settings import DispatchSettings
from src.domain.exceptions import (
    DispatchMisconfigured,
    NetworkUnavailable,
    StorageError,
)


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

    def _on_started(self) -> None:
        self._set_retention_deadline()

    async def _safe_tick(self) -> None:
        sent: int | None = None
        try:
            async with asyncio.timeout(self._settings.tick_deadline):
                sent = await self._service.dispatch_once()
        except TimeoutError:
            self._on_network_unavailable(
                NetworkUnavailable("тик отправки превысил дедлайн")
            )
        except NetworkUnavailable as e:
            self._on_network_unavailable(e)
        except DispatchMisconfigured as e:
            self._stop_misconfigured(e, fallback="dispatch misconfigured")
            return
        except StorageError as e:
            self._consecutive_failures += 1
            self._log_tick_failure("error", "Ошибка хранилища: {}", e)
            return
        else:
            self._on_success(sent)

        await self._clear_ttl()

    def _on_success(self, sent: int) -> None:
        if self._network_unavailable:
            self._network_unavailable = False
            self._clear_log_throttle("network")
            if sent:
                self._log.info("Сеть восстановилась, отправлено {} пакетов", sent)
            else:
                self._log.info("Сеть восстановилась")
        elif sent:
            self._log.info("Отправлено {} пакетов", sent)
        self._reset_failures()

    def _on_network_unavailable(self, error: NetworkUnavailable) -> None:
        self._network_unavailable = True
        if not self._should_log("network"):
            return
        self._log.warning("Сети нет, пакеты копятся в outbox: {}", error)

    async def _clear_ttl(self) -> None:
        if time.monotonic() <= self._retention_deadline:
            return
        try:
            async with asyncio.timeout(RETENTION_TIMEOUT_S):
                await self._service.delete_sent_older_than()
        except TimeoutError:
            self._log.error("Ретенция превысила таймаут")
            return
        except StorageError as e:
            self._log.error("Не удалось удалить отправленные пакеты: {}", e)
            return
        self._log.info("Удалены отправленные пакеты, установлен новый дедлайн")
        self._set_retention_deadline()

    def _set_retention_deadline(self) -> None:
        self._retention_deadline = (
            time.monotonic() + self._settings.waiting_period_ttl.total_seconds()
        )
