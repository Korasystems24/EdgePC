from abc import ABC, abstractmethod

from src.domain.entities.telemetry_packet import TelemetryPacket


class NetworkGateway(ABC):
    @abstractmethod
    async def send_batch(self, packets: list[TelemetryPacket]) -> None: ...
