from loguru import logger

from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.ports.radar_adapter import RadarAdapter


class IngestionService:
    def __init__(self, radar_adapter: RadarAdapter) -> None:
        self._radar_adapter = radar_adapter

    async def collect_once(self) -> TelemetryPacket:
        packet = await self._radar_adapter.read_packet()
        logger.info(
            "Собран пакет: avg_level={} mass={} beams={}",
            packet.avg_level,
            packet.mass,
            len(packet.beams),
        )
        return packet
