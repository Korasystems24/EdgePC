from loguru import logger

from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.ports.radar_adapter import RadarAdapter
from src.domain.ports.telemetry_repository import TelemetryRepository


class IngestionService:
    def __init__(
        self,
        radar_adapter: RadarAdapter,
        repository: TelemetryRepository,
    ) -> None:
        self._radar_adapter = radar_adapter
        self._repository = repository

    async def collect_once(self) -> TelemetryPacket:
        packet = await self._radar_adapter.read_packet()
        await self._repository.save(packet)
        logger.bind(name="ingestion service: collect_once").info(
            "Собран и сохранён пакет: avg_level={} mass={} beams={}",
            packet.avg_level,
            packet.mass,
            len(packet.beams),
        )
        return packet
