from typing import Annotated

import pydantic

from src.domain.constants import BEAM_COUNT

from .beam import Beam
from .event_payload import (
    PacketQuarantined,
    RadarDown,
    RadarRecovered,
    WorkerFatal,
    WorkerRestarted,
)

TelemetryBeams = Annotated[
    list[Beam], pydantic.Field(min_length=BEAM_COUNT, max_length=BEAM_COUNT)
]

# Измеренная радаром величина (масса, объём, уровень):
# конечное неотрицательное число; отсутствие значения выражается None на поле.
Measurement = Annotated[float, pydantic.Field(ge=0, allow_inf_nan=False)]

type Timestamp = pydantic.AwareDatetime

type EdgeEventPayload = (
    RadarDown | RadarRecovered | PacketQuarantined | WorkerRestarted | WorkerFatal
)
