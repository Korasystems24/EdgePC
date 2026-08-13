from src.domain.ports.network_gateway import NetworkGateway
from src.domain.ports.telemetry_repository import TelemetryRepository


class DispatchService:
    def __init__(
        self,
        repository: TelemetryRepository,
        gateway: NetworkGateway,
        batch_size: int,
    ) -> None:
        self._repository = repository
        self._gateway = gateway
        self._batch_size = batch_size

    async def dispatch_once(self) -> int:

        batch = await self._repository.fetch_pending(self._batch_size)

        if not batch:
            return 0

        await self._gateway.send_batch([item.packet for item in batch])

        await self._repository.mark_sent([item.id for item in batch])

        return len(batch)
