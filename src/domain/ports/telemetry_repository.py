from abc import ABC, abstractmethod

from src.domain.entities.telemetry_packet import TelemetryPacket


class TelemetryRepository(ABC):
    @abstractmethod
    async def save(self, packet: TelemetryPacket) -> None: ...
