import uuid

import pydantic

from src.domain.value_objects.types import Measurement, TelemetryBeams, Timestamp


class TelemetryPacket(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(frozen=True)
    event_id: uuid.UUID = pydantic.Field(default_factory=uuid.uuid4)
    ts: Timestamp
    avg_level: Measurement | None = None
    mass: Measurement | None = None
    volume: Measurement | None = None
    beams: TelemetryBeams
