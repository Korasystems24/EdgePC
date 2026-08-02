from abc import ABC, abstractmethod

from src.domain.entities.telemetry_packet import TelemetryPacket


class RadarAdapter(ABC):
    @abstractmethod
    async def read_packet(self) -> TelemetryPacket: ...
