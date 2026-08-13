import uuid

from sqlalchemy import Uuid
from sqlalchemy.orm import Mapped, declared_attr, mapped_column


class EventIdMixin:
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
