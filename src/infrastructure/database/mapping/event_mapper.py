from src.domain.entities.edge_event import EdgeEvent
from src.infrastructure.database.models import EventOutboxModel


class EventOutboxMapper:
    @staticmethod
    def to_domain(model: EventOutboxModel) -> EdgeEvent:
        return EdgeEvent.model_validate(
            {
                "event_id": model.event_id,
                "type": model.type,
                "payload": model.payload,
            }
        )

    @staticmethod
    def to_model(event: EdgeEvent) -> EventOutboxModel:
        return EventOutboxModel(
            event_id=event.event_id,
            type=event.type,
            payload=event.payload.model_dump(mode="json"),
        )
