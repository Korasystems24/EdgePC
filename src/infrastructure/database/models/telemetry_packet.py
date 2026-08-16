from datetime import datetime
from typing import Any

from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, mapped_column

from src.infrastructure.database.models.base import Base
from src.infrastructure.database.models.mixin.event_id import EventIdMixin
from src.infrastructure.database.models.mixin.sent_at import SentAtMixin
from src.infrastructure.database.types import UTCDateTime

from .mixin.int_id_pk import IntIdPkMixin


class TelemetryPacketModel(
    IntIdPkMixin,
    EventIdMixin,
    SentAtMixin,
    Base,
):
    __tablename__ = "telemetry_packet"

    __event_id_unique__ = True

    ts: Mapped[datetime] = mapped_column(
        UTCDateTime(),
        index=True,
    )

    avg_level: Mapped[float | None]
    mass: Mapped[float | None]
    volume: Mapped[float | None]

    beams: Mapped[list[dict[str, Any]]] = mapped_column(JSON)

    attempts: Mapped[int] = mapped_column(default=0)

    quarantined_at: Mapped[datetime | None] = mapped_column(
        UTCDateTime(),
        nullable=True,
        index=True,
        default=None,
    )
