from datetime import UTC, datetime

from loguru import logger
from sqlalchemy import ColumnElement, select, update

from src.domain.entities.edge_event import EdgeEvent
from src.domain.ports.event_repository import EventRepository, StoredEvent
from src.infrastructure.database.base_client import DatabaseClient
from src.infrastructure.database.mapping import event_mapper
from src.infrastructure.database.models import EventOutboxModel
from src.infrastructure.database.repositories.pending import (
    read_pending,
    storage_errors,
)


class SqlAlchemyEventRepository(EventRepository):
    def __init__(self, db_client: DatabaseClient) -> None:
        self._db_client = db_client

    async def save(self, event: EdgeEvent) -> None:
        model = event_mapper.to_model(event)
        with storage_errors("Не удалось сохранить событие"):
            async with self._db_client.session() as session:
                session.add(model)
                await session.commit()

    async def get_pending(self, limit: int) -> list[StoredEvent]:
        query = (
            select(EventOutboxModel)
            .where(*self._pending_filter())
            .order_by(EventOutboxModel.id)
            .limit(limit)
        )
        with storage_errors("Не удалось получить ожидающие события"):
            async with self._db_client.session() as session:
                return await read_pending(
                    session,
                    query,
                    build=self._to_stored_event,
                    on_corrupt=self._quarantine_corrupt,
                )

    @staticmethod
    def _to_stored_event(model: EventOutboxModel) -> StoredEvent:
        return StoredEvent(id=model.id, event=event_mapper.to_domain(model))

    @staticmethod
    def _quarantine_corrupt(model: EventOutboxModel, error: Exception) -> None:
        logger.error("Карантин битого события id={}: {}", model.id, error)
        model.quarantined_at = datetime.now(UTC)

    @staticmethod
    def _pending_filter() -> tuple[ColumnElement[bool], ColumnElement[bool]]:
        """Ещё не уехало и не в карантине."""
        return (
            EventOutboxModel.sent_at.is_(None),
            EventOutboxModel.quarantined_at.is_(None),
        )

    async def set_sent(self, ids: list[int]) -> None:
        if not ids:
            return
        with storage_errors("Не удалось пометить события отправленными"):
            async with self._db_client.session() as session:
                await session.execute(
                    update(EventOutboxModel)
                    .where(EventOutboxModel.id.in_(ids))
                    .values(sent_at=datetime.now(UTC))
                )
                await session.commit()
