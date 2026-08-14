from datetime import UTC, datetime, timedelta

from loguru import logger

from src.domain.exceptions import DispatchMisconfigured, DispatchRejected
from src.domain.ports.network_gateway import NetworkGateway
from src.domain.ports.telemetry_repository import StoredPacket, TelemetryRepository


class DispatchService:
    def __init__(
        self,
        repository: TelemetryRepository,
        gateway: NetworkGateway,
        batch_size: int,
        retention_ttl: timedelta,
        mass_rejection_sample: int,
        quarantine_after_attempts: int,
    ) -> None:
        self._repository = repository
        self._gateway = gateway
        self._batch_size = batch_size
        self._retention_ttl = retention_ttl
        self._mass_rejection_sample = mass_rejection_sample
        self.quarantine_after_attempts = quarantine_after_attempts

    async def dispatch_once(self) -> int:
        # TODO не забыть добавить проверку для panding и sent пакетов
        batch = await self._repository.get_pending(self._batch_size)

        if not batch:
            return 0

        try:
            await self._gateway.send_batch([item.packet for item in batch])
            await self._repository.set_sent([item.id for item in batch])
            return len(batch)
        except DispatchRejected:
            return await self._isolate_batch(batch)

    async def delete_sent_older_than(self) -> int:
        cutoff = datetime.now(tz=UTC) - self._retention_ttl
        return await self._repository.delete_sent_older_than(cutoff)

    async def _isolate_batch(self, batch: list[StoredPacket]) -> int:
        logger.warning(
            f"Пачка получила 422, проверяем {self._mass_rejection_sample} пакетов по одному"
        )
        quarantine_after = 0
        success_batch = 0
        bad_batch: list[StoredPacket] = []
        for item in batch[: self._mass_rejection_sample]:
            try:
                await self._gateway.send_batch([item.packet])
                await self._repository.set_sent([item.id])
                success_batch += 1
            except DispatchRejected:
                quarantine_after += 1
                bad_batch.append(item)
        if quarantine_after >= self._mass_rejection_sample:
            raise DispatchMisconfigured(
                f"Сервер отверг {quarantine_after} пакетов подряд, похоже сломан контракт"
            )
        else:
            await self._repository.update_rejected(
                [item.id for item in bad_batch], self.quarantine_after_attempts
            )
        logger.warning(
            f"Сервер отверг{quarantine_after} пакетов, копят попытки / карантин. Отправлено только {success_batch} пакетов"
        )
        return success_batch
