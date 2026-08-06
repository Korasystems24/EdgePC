import pydantic

from src.domain.enum import SourceState
from src.domain.value_objects.types import Measurement, TelemetryBeams, Timestamp


class TelemetryPacket(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(frozen=True)

    ts: Timestamp
    avg_level: Measurement | None = None
    mass: Measurement | None = None
    volume: Measurement | None = None
    beams: TelemetryBeams
    source_state: SourceState
