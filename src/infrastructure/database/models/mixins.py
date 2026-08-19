import uuid
from datetime import datetime

from sqlalchemy import Uuid
from sqlalchemy.orm import Mapped, declared_attr, mapped_column

from src.infrastructure.database.types import UTCDateTime, utc_now


class IntIdPkMixin:
    id: Mapped[int] = mapped_column(primary_key=True)


class EventIdMixin:
    """Идентичность записи, по которой сервер отличает повтор от новых данных.
    Таблица включает уникальность через `__event_id_unique__`."""

    __event_id_unique__ = False
    __event_id_index__ = True

    @declared_attr
    def event_id(cls) -> Mapped[uuid.UUID]:
        return mapped_column(
            Uuid(as_uuid=True),
            unique=cls.__event_id_unique__,
            index=cls.__event_id_index__,
            nullable=False,
        )


class CreatedAtMixin:
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(),
        default=utc_now,
        nullable=False,
    )


class SentAtMixin:
    """NULL означает «ещё не отправлено»: одна колонка отвечает и за факт
    отправки, и за её время (по нему работает ретенция)."""

    sent_at: Mapped[datetime | None] = mapped_column(
        UTCDateTime(),
        nullable=True,
        index=True,
        default=None,
    )
