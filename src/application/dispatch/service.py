import asyncio
from datetime import UTC, datetime, timedelta

from loguru import logger

from src.application.health_store import HealthStore
from src.domain.entities.edge_event import EdgeEvent
from src.domain.exceptions import (
    DispatchMisconfigured,
    DispatchRejected,
    NetworkUnavailable,
)
from src.domain.ports.event_repository import EventRepository
from src.domain.ports.network_gateway import NetworkGateway
from src.domain.ports.telemetry_repository import StoredPacket, TelemetryRepository


class DispatchService:
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
        events_timeout: float | None = None,
    ) -> None:
        self._repository = repository
        self._events = events
        self._gateway = gateway
        self._health = health
        self._batch_size = batch_size
        self._event_batch_size = event_batch_size
        self._retention_ttl = retention_ttl
        self._mass_rejection_sample = mass_rejection_sample
        self._quarantine_after_attempts = quarantine_after_attempts
        self._events_timeout = events_timeout

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
            logger.warning(
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
            logger.warning(
                "Events не ушли за отведённое время, телеметрию всё равно отправляем"
            )
            return
        except NetworkUnavailable as exc:
            logger.warning(
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
            sample = batch[: self._mass_rejection_sample]
            return await self._send_rejected_sample_one_by_one(sample)
        await self._repository.set_sent([item.id for item in batch])
        return len(batch)

    async def _send_rejected_sample_one_by_one(self, sample: list[StoredPacket]) -> int:

        logger.warning(
            "Пачка получила 422, проверяем {} пакетов по одному",
            len(sample),
        )
        sent = 0
        rejected: list[StoredPacket] = []
        try:
            for item in sample:
                try:
                    await self._gateway.send_telemetry_once([item.packet])
                    await self._repository.set_sent([item.id])
                    sent += 1
                except DispatchRejected:
                    rejected.append(item)
        except NetworkUnavailable:
            if rejected:
                await self._repository.update_rejected(
                    [item.id for item in rejected],
                    self._quarantine_after_attempts,
                )
            raise

        if len(sample) >= self._mass_rejection_sample and len(rejected) == len(sample):
            raise DispatchMisconfigured(
                f"Сервер отверг {len(rejected)} пакетов подряд, похоже сломан контракт"
            )

        await self._repository.update_rejected(
            [item.id for item in rejected],
            self._quarantine_after_attempts,
        )
        logger.warning(
            "Сервер отверг {} пакетов, копят попытки / карантин. "
            "Отправлено только {} пакетов",
            len(rejected),
            sent,
        )
        return sent
