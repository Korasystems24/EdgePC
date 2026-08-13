from datetime import datetime

from sqlalchemy.orm import Mapped, mapped_column

from src.infrastructure.database.types import UTCDateTime


class SentAtMixin:
    sent_at: Mapped[datetime | None] = mapped_column(
        UTCDateTime(),
        nullable=True,
        index=True,
        default=None,
    )
