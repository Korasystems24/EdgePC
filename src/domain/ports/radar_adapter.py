from abc import ABC, abstractmethod

from src.domain.entities.telemetry_packet import TelemetryPacket


class RadarAdapter(ABC):
    @abstractmethod
    async def read_packet(self) -> TelemetryPacket: ...

    def raise_if_hung(self) -> None:
        """Бросить RadarSourceHung, если чтение ещё блокирует границу после отмены ожидания."""
        return None

    async def close(self) -> None:
        return None
