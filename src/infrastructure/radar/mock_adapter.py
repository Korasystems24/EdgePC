import asyncio
import random
from datetime import UTC, datetime

from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.ports.radar_adapter import RadarAdapter
from src.domain.value_objects.beam import Beam


class MockRadarAdapter(RadarAdapter):
    def __init__(self, delay: int, fail: float) -> None:
        self.delay = delay
        self.fail = fail

    async def read_packet(self) -> TelemetryPacket:

        await asyncio.sleep(self.delay)

        if random.random() <= self.fail:
            raise ValueError()
        ts = datetime.now(tz=UTC).timestamp()

        beams = [
            Beam(
                index=index,
                distance=random.uniform(1000, 2000),
                raw_status=random.uniform(0, 1),
                status_flags=[],
            )
            for index in range(64)
        ]

        volume, mass, avg_level = [random.randint(1000, 2000) for _ in range(3)]

        return TelemetryPacket(
            ts=ts,
            volume=volume,
            mass=mass,
            avg_level=avg_level,
            beams=beams,
            source_state="HEALTHY",
        )
