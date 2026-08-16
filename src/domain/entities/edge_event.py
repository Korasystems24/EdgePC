import uuid
from typing import Self

import pydantic
from pydantic import model_validator

from src.domain.constants import PAYLOAD_BY_TYPE
from src.domain.enum import EdgeEventType
from src.domain.value_objects.event_payload import (
    PacketQuarantined,
    RadarDown,
    RadarRecovered,
    WorkerFatal,
    WorkerRestarted,
)
from src.domain.value_objects.types import EdgeEventPayload


class EdgeEvent(pydantic.BaseModel):
    model_config = pydantic.ConfigDict(frozen=True)

    event_id: uuid.UUID = pydantic.Field(default_factory=uuid.uuid4)
    type: EdgeEventType
    payload: EdgeEventPayload

    @model_validator(mode="after")
    def _payload_matches_type(self) -> Self:
        expected = PAYLOAD_BY_TYPE[self.type]
        if type(self.payload) is not expected:
            raise ValueError(
                f"{self.type} ожидает {expected.__name__}, "
                f"получен {type(self.payload).__name__}"
            )
        return self

    @classmethod
    def radar_down(cls, *, reason: str) -> Self:
        return cls(type=EdgeEventType.RADAR_DOWN, payload=RadarDown(reason=reason))

    @classmethod
    def radar_recovered(cls, *, downtime_seconds: float) -> Self:
        return cls(
            type=EdgeEventType.RADAR_RECOVERED,
            payload=RadarRecovered(downtime_seconds=downtime_seconds),
        )

    @classmethod
    def packet_quarantined(
        cls,
        *,
        telemetry_packet_id: int,
        telemetry_packet_event_id: uuid.UUID,
        attempts: int,
    ) -> Self:
        return cls(
            type=EdgeEventType.PACKET_QUARANTINED,
            payload=PacketQuarantined(
                telemetry_packet_id=telemetry_packet_id,
                telemetry_packet_event_id=telemetry_packet_event_id,
                attempts=attempts,
            ),
        )

    @classmethod
    def worker_restarted(cls, *, worker: str) -> Self:
        return cls(
            type=EdgeEventType.WORKER_RESTARTED,
            payload=WorkerRestarted(worker=worker),
        )

    @classmethod
    def fatal_error(cls, *, worker: str, reason: str, restartable: bool) -> Self:
        return cls(
            type=EdgeEventType.FATAL_ERROR,
            payload=WorkerFatal(
                worker=worker,
                reason=reason,
                restartable=restartable,
            ),
        )
