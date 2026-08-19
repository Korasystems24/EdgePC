import asyncio
from datetime import UTC, datetime, timedelta

from loguru import logger

from src.application.dispatch.rejection import RejectionIsolator
from src.application.health_store import HealthStore
from src.domain.entities.edge_event import EdgeEvent
from src.domain.exceptions import (
    DispatchMisconfigured,
    DispatchRejected,
    NetworkUnavailable,
)
from src.domain.ports.event_repository import EventRepository
from src.domain.ports.network_gateway import NetworkGateway
from src.domain.ports.telemetry_repository import TelemetryRepository


class DispatchService:
    """Что нужно сделать за один тик отправки: health, события, телеметрия.

    Порядок внутри телеметрии — гарантия «не потеряем без следа»: пометка
    `sent_at` строго после успешной отправки. Упали между отправкой и пометкой —
    пакеты уедут повторно, дубли гасит дедупликация на сервере.
    """

    def __init__(
        self,
        repository: TelemetryRepository,
        events: EventRepository,
        gateway: NetworkGateway,
        health: HealthStore,
        batch_size: int,
        event_batch_size: int,
        retention_ttl: timedelta,
        mass_rejection_sample: int,
        quarantine_after_attempts: int,
        isolation_budget: float,
        events_timeout: float | None = None,
    ) -> None:
        self._repository = repository
        self._events = events
        self._gateway = gateway
        self._health = health
        self._batch_size = batch_size
        self._event_batch_size = event_batch_size
        self._retention_ttl = retention_ttl
        self._events_timeout = events_timeout
        self._rejection = RejectionIsolator(
            repository,
            gateway,
            sample_size=mass_rejection_sample,
            quarantine_after_attempts=quarantine_after_attempts,
            budget=isolation_budget,
        )
        self._log = logger.bind(name="dispatch")

    async def dispatch_once(self) -> int:
        await self._send_health()
        await self._send_events()
        return await self._send_telemetry()

    async def delete_sent_older_than(self) -> int:
        cutoff = datetime.now(tz=UTC) - self._retention_ttl
        return await self._repository.delete_sent_older_than(cutoff)

    async def _send_health(self) -> None:
        snapshot = self._health.current()
        if snapshot is None:
            return
        try:
            await self._gateway.send_health(snapshot)
        except DispatchRejected as exc:
            raise DispatchMisconfigured(f"сервер отверг health-снапшот: {exc}") from exc
        except NetworkUnavailable as exc:
            self._log.warning(
                "Health не ушёл, события и телеметрию всё равно отправляем: {}",
                exc,
            )

    async def _send_events(self) -> None:
        pending = await self._events.get_pending(self._event_batch_size)
        if not pending:
            return
        try:
            await self._send_events_payload([item.event for item in pending])
        except TimeoutError:
            self._log.warning(
                "Events не ушли за отведённое время, телеметрию всё равно отправляем"
            )
            return
        except NetworkUnavailable as exc:
            self._log.warning(
                "Events не ушли, телеметрию всё равно отправляем: {}",
                exc,
            )
            return
        except DispatchRejected as exc:
            raise DispatchMisconfigured(f"сервер отверг события: {exc}") from exc
        await self._events.set_sent([item.id for item in pending])

    async def _send_events_payload(self, events: list[EdgeEvent]) -> None:
        if self._events_timeout is None:
            await self._gateway.send_events(events)
            return
        async with asyncio.timeout(self._events_timeout):
            await self._gateway.send_events(events)

    async def _send_telemetry(self) -> int:
        batch = await self._repository.get_pending(self._batch_size)
        if not batch:
            return 0
        try:
            await self._gateway.send_telemetry([item.packet for item in batch])
        except DispatchRejected:
            return await self._rejection.isolate(batch)
        await self._repository.set_sent([item.id for item in batch])
        return len(batch)
