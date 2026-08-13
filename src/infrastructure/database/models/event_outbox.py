from typing import Any

from sqlalchemy import JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from src.infrastructure.database.models.base import Base
from src.infrastructure.database.models.mixin.created_at import CreatedAtMixin
from src.infrastructure.database.models.mixin.event_id import EventIdMixin
from src.infrastructure.database.models.mixin.int_id_pk import IntIdPkMixin
from src.infrastructure.database.models.mixin.sent_at import SentAtMixin


class EventOutbox(
    IntIdPkMixin,
    EventIdMixin,
    SentAtMixin,
    CreatedAtMixin,
    Base,
):
    __tablename__ = "event_outbox"

    __event_id_unique__ = True

    type: Mapped[str] = mapped_column(
        String(64),
        index=True,
    )

    payload: Mapped[dict[str, Any]] = mapped_column(
        JSON,
        default=dict,
    )
