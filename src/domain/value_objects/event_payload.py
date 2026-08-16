import uuid

from pydantic import BaseModel, ConfigDict, Field


class RadarDown(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    reason: str


class RadarRecovered(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    downtime_seconds: float = Field(ge=0, allow_inf_nan=False)


class PacketQuarantined(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    telemetry_packet_id: int = Field(ge=1)
    telemetry_packet_event_id: uuid.UUID
    attempts: int = Field(ge=1)


class WorkerRestarted(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    worker: str


class WorkerFatal(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    worker: str
    reason: str
    restartable: bool
