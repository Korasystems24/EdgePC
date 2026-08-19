"""`created_at` строки и `ts` события — одна и та же величина: когда событие
произошло на edge. Отдельной колонки под `ts` нет, чтобы у времени события не
появилось два источника истины.
"""

from src.domain.entities.edge_event import EdgeEvent
from src.infrastructure.database.models import EventOutboxModel


def to_domain(model: EventOutboxModel) -> EdgeEvent:
    return EdgeEvent.model_validate(
        {
            "event_id": model.event_id,
            "ts": model.created_at,
            "type": model.type,
            "payload": model.payload,
        }
    )


def to_model(event: EdgeEvent) -> EventOutboxModel:
    return EventOutboxModel(
        event_id=event.event_id,
        created_at=event.ts,
        type=event.type,
        payload=event.payload.model_dump(mode="json"),
    )
