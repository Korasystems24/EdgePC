import asyncio
import random

from loguru import logger

from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.exceptions import NetworkUnavailable
from src.domain.ports.network_gateway import NetworkGateway


class MockNetworkGateway(NetworkGateway):
    def __init__(self, delay: float, fail: float) -> None:
        self.delay = delay
        self.fail = fail

    async def send_batch(self, packets: list[TelemetryPacket]) -> None:
        await asyncio.sleep(self.delay)

        if random.random() <= self.fail:
            raise NetworkUnavailable("Mock: симулированный обрыв сети")

        logger.info(
            "Mock: 'отправлено' {} пакетов, первый ts={}", len(packets), packets[0].ts
        )
