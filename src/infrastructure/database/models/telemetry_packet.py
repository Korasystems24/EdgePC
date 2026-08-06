from datetime import datetime
from typing import Any

from sqlalchemy import JSON, DateTime
from sqlalchemy.orm import Mapped, mapped_column

from src.infrastructure.database.models.base import Base

from .mixin.int_id_pk import IntIdPkMixin


class TelemetryPacketModel(IntIdPkMixin, Base):
    __tablename__ = "telemetry_packet"

    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)

    avg_level: Mapped[float | None]
    mass: Mapped[float | None]
    volume: Mapped[float | None]

    source_state: Mapped[str]

    beams: Mapped[list[dict[str, Any]]] = mapped_column(JSON)
