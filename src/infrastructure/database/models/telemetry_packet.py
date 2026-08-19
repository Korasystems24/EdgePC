from datetime import datetime
from typing import Any

from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, mapped_column

from src.infrastructure.database.models.base import Base
from src.infrastructure.database.models.mixins import (
    EventIdMixin,
    IntIdPkMixin,
    SentAtMixin,
)
from src.infrastructure.database.types import UTCDateTime


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

    # Радар может не отдать величину: в БД это NULL, а не ноль.
    avg_level: Mapped[float | None] = mapped_column(nullable=True)
    mass: Mapped[float | None] = mapped_column(nullable=True)
    volume: Mapped[float | None] = mapped_column(nullable=True)

    beams: Mapped[list[dict[str, Any]]] = mapped_column(JSON)

    attempts: Mapped[int] = mapped_column(default=0)

    quarantined_at: Mapped[datetime | None] = mapped_column(
        UTCDateTime(),
        nullable=True,
        index=True,
        default=None,
    )
