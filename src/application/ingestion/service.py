from src.domain.ports.radar_adapter import RadarAdapter


class IngestionService:
    def __init__(self, radar_adapter: RadarAdapter) -> None:
        self.radar = radar_adapter

    async def handler_service(self):
        packet = await self.radar.read_packet()
        return packet
