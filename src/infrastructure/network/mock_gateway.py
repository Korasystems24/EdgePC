import asyncio
import random

from loguru import logger

from src.domain.entities.edge_event import EdgeEvent
from src.domain.entities.health_snapshot import HealthSnapshot
from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.exceptions import NetworkUnavailable
from src.domain.ports.network_gateway import NetworkGateway


class MockNetworkGateway(NetworkGateway):
    def __init__(self, delay: float, fail: float) -> None:
        self.delay = delay
        self.fail = fail

    async def send_telemetry(self, packets: list[TelemetryPacket]) -> None:
        await self._simulate()
        logger.info(
            "Mock: 'отправлено' {} пакетов, первый ts={}",
            len(packets),
            packets[0].ts if packets else None,
        )

    async def send_events(self, events: list[EdgeEvent]) -> None:
        await self._simulate()
        logger.info(
            "Mock: 'отправлено' {} событий, первый type={}",
            len(events),
            events[0].type if events else None,
        )

    async def send_health(self, snapshot: HealthSnapshot) -> None:
        await self._simulate()
        logger.info(
            "Mock: 'отправлен' health ts={} pending={}",
            snapshot.ts,
            snapshot.pending_count,
        )

    async def _simulate(self) -> None:
        await asyncio.sleep(self.delay)
        if random.random() < self.fail:
            raise NetworkUnavailable("Mock: симулированный обрыв сети")
