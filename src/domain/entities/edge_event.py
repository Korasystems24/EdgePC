import uuid
from typing import Any, Self, TypedDict

import pydantic

from src.domain.enum import EdgeEventType


class RadarDownPayload(TypedDict):
    """Радар/WCF стал недоступен (один раз на вход в DOWN)."""

    reason: str


class RadarRecoveredPayload(TypedDict):
    """Первый успешный опрос после простоя."""

    downtime_seconds: float


class PacketQuarantinedPayload(TypedDict):
    """Пакет исключён из автоматической отправки."""

    telemetry_packet_id: int
    telemetry_packet_event_id: str
    attempts: int


class WorkerRestartedPayload(TypedDict):
    """Watchdog перезапустил воркер."""

    worker: str


class FatalErrorPayload(TypedDict):
    """Воркер признал, что не может продолжать."""

    worker: str
    reason: str
    restartable: bool


type EdgeEventPayload = (
    RadarDownPayload
    | RadarRecoveredPayload
    | PacketQuarantinedPayload
    | WorkerRestartedPayload
    | FatalErrorPayload
)


class EdgeEvent(pydantic.BaseModel):
    """Операционное событие для event_outbox → POST /events."""

    model_config = pydantic.ConfigDict(frozen=True)

    event_id: uuid.UUID = pydantic.Field(default_factory=uuid.uuid4)
    type: EdgeEventType
    payload: EdgeEventPayload

    @classmethod
    def radar_down(cls, *, reason: str) -> Self:
        payload: RadarDownPayload = {"reason": reason}
        return cls(type=EdgeEventType.RADAR_DOWN, payload=payload)

    @classmethod
    def radar_recovered(cls, *, downtime_seconds: float) -> Self:
        payload: RadarRecoveredPayload = {"downtime_seconds": downtime_seconds}
        return cls(type=EdgeEventType.RADAR_RECOVERED, payload=payload)

    @classmethod
    def packet_quarantined(
        cls,
        *,
        telemetry_packet_id: int,
        telemetry_packet_event_id: uuid.UUID,
        attempts: int,
    ) -> Self:
        payload: PacketQuarantinedPayload = {
            "telemetry_packet_id": telemetry_packet_id,
            "telemetry_packet_event_id": str(telemetry_packet_event_id),
            "attempts": attempts,
        }
        return cls(type=EdgeEventType.PACKET_QUARANTINED, payload=payload)

    @classmethod
    def worker_restarted(cls, *, worker: str) -> Self:
        payload: WorkerRestartedPayload = {"worker": worker}
        return cls(type=EdgeEventType.WORKER_RESTARTED, payload=payload)

    @classmethod
    def fatal_error(cls, *, worker: str, reason: str, restartable: bool) -> Self:
        payload: FatalErrorPayload = {
            "worker": worker,
            "reason": reason,
            "restartable": restartable,
        }
        return cls(type=EdgeEventType.FATAL_ERROR, payload=payload)

    def payload_as_dict(self) -> dict[str, Any]:
        """Для ORM/JSON-колонки, где нужен обычный dict."""
        return dict(self.payload)
