from typing import Annotated

import pydantic

from src.domain.enum import SourceState
from src.domain.value_objects.beam import Beam

TelemetryBeams = Annotated[list[Beam], pydantic.Field(min_length=64, max_length=64)]


class TelemetryPacket(pydantic.BaseModel):
    ts: pydantic.AwareDatetime
    avg_level: float | None = None
    mass: float | None = None
    volume: float | None = None
    beams: TelemetryBeams
    source_state: SourceState
