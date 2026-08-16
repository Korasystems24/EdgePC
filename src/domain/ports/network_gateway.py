from abc import ABC, abstractmethod

from src.domain.entities.edge_event import EdgeEvent
from src.domain.entities.health_snapshot import HealthSnapshot
from src.domain.entities.telemetry_packet import TelemetryPacket


class NetworkGateway(ABC):
    @abstractmethod
    async def send_telemetry(self, packets: list[TelemetryPacket]) -> None: ...

    async def send_telemetry_once(self, packets: list[TelemetryPacket]) -> None:
        await self.send_telemetry(packets)

    @abstractmethod
    async def send_events(self, events: list[EdgeEvent]) -> None: ...

    @abstractmethod
    async def send_health(self, snapshot: HealthSnapshot) -> None: ...

    async def close(self) -> None:
        return None
