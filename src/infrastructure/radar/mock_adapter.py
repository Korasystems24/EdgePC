import asyncio
import random
from datetime import UTC, datetime

from src.domain.constants import BEAM_COUNT
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

        if random.random() < self.fail:
            raise RadarSourceUnavailable("Mock: симулированный сбой источника")

        beams = [
            Beam(
                height=random.uniform(1000, 2000),
                raw_status=0,
                status=(),
            )
            for _ in range(BEAM_COUNT)
        ]

        return TelemetryPacket(
            ts=datetime.now(tz=UTC),
            volume=random.randint(1000, 2000),
            mass=random.randint(1000, 2000),
            avg_level=random.randint(1000, 2000),
            beams=beams,
        )
