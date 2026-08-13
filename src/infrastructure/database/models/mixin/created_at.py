from datetime import datetime

from sqlalchemy.orm import Mapped, mapped_column

from src.infrastructure.database.types import UTCDateTime, utc_now


class CreatedAtMixin:
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(),
        default=utc_now,
        nullable=False,
    )
