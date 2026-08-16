from datetime import UTC, datetime

from loguru import logger
from pydantic import ValidationError
from sqlalchemy import ColumnElement, select, update
from sqlalchemy.exc import SQLAlchemyError

from src.domain.entities.edge_event import EdgeEvent
from src.domain.exceptions import StorageError
from src.domain.ports.event_repository import EventRepository, StoredEvent
from src.infrastructure.database.base_client import DatabaseClient
from src.infrastructure.database.mapping.event_mapper import EventOutboxMapper
from src.infrastructure.database.models import EventOutboxModel


class SqlAlchemyEventRepository(EventRepository):
    def __init__(self, db_client: DatabaseClient) -> None:
        self._db_client = db_client
        self._mapper = EventOutboxMapper()

    async def save(self, event: EdgeEvent) -> None:
        model = self._mapper.to_model(event)
        try:
            async with self._db_client.session() as session:
                session.add(model)
                await session.commit()
        except SQLAlchemyError as e:
            raise StorageError(f"Не удалось сохранить событие: {e}") from e

    async def get_pending(self, limit: int) -> list[StoredEvent]:
        try:
            async with self._db_client.session() as session:
                pending: list[StoredEvent] = []
                processed_ids: list[int] = []
                quarantined = False
                while len(pending) < limit:
                    query = (
                        select(EventOutboxModel)
                        .where(*self._pending_filter())
                        .order_by(EventOutboxModel.id)
                        .limit(limit - len(pending))
                    )
                    if processed_ids:
                        query = query.where(EventOutboxModel.id.notin_(processed_ids))
                    result = await session.execute(query)
                    models = result.scalars().all()
                    if not models:
                        break
                    quarantined_this_batch = False
                    for model in models:
                        processed_ids.append(model.id)
                        try:
                            pending.append(
                                StoredEvent(
                                    id=model.id,
                                    event=self._mapper.to_domain(model),
                                    created_at=model.created_at,
                                )
                            )
                        except (ValidationError, TypeError, ValueError) as e:
                            logger.error(
                                "Карантин битого события id={}: {}",
                                model.id,
                                e,
                            )
                            model.quarantined_at = datetime.now(UTC)
                            quarantined_this_batch = True
                            quarantined = True
                    if quarantined_this_batch:
                        await session.flush()
                    else:
                        break
                if quarantined:
                    await session.commit()
                return pending
        except SQLAlchemyError as e:
            raise StorageError(f"Не удалось получить ожидающие события: {e}") from e

    @staticmethod
    def _pending_filter() -> tuple[ColumnElement[bool], ColumnElement[bool]]:
        return (
            EventOutboxModel.sent_at.is_(None),
            EventOutboxModel.quarantined_at.is_(None),
        )

    async def set_sent(self, ids: list[int]) -> None:
        if not ids:
            return
        try:
            async with self._db_client.session() as session:
                await session.execute(
                    update(EventOutboxModel)
                    .where(EventOutboxModel.id.in_(ids))
                    .values(sent_at=datetime.now(UTC))
                )
                await session.commit()
        except SQLAlchemyError as e:
            raise StorageError(
                f"Не удалось пометить события отправленными: {e}"
            ) from e
