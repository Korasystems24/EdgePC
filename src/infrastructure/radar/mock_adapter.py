import asyncio
import random
from datetime import UTC, datetime

from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.exceptions import RadarSourceUnavailable
from src.domain.ports.radar_adapter import RadarAdapter
from src.domain.value_objects.beam import Beam


class MockRadarAdapter(RadarAdapter):
    def __init__(self, delay: float, fail: float) -> None:
        self.delay = delay
        self.fail = fail

    async def read_packet(self) -> TelemetryPacket:

        await asyncio.sleep(self.delay)

        if random.random() <= self.fail:
            raise RadarSourceUnavailable("Mock: симулированный сбой источника")
        ts = datetime.now(tz=UTC)

        beams = [
            Beam(
                height=random.uniform(1000, 2000),
                raw_status=0,
                status=(),
            )
            for _ in range(64)
        ]

        volume, mass, avg_level = [random.randint(1000, 2000) for _ in range(3)]

        return TelemetryPacket(
            ts=ts,
            volume=volume,
            mass=mass,
            avg_level=avg_level,
            beams=beams,
        )
