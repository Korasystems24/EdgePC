from datetime import UTC, datetime

from sqlalchemy.types import DateTime, TypeDecorator


def utc_now() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


class UTCDateTime(TypeDecorator[datetime]):
    """SQLite хранит datetime без tz; на границе БД считаем UTC."""

    impl = DateTime
    cache_ok = True

    def process_bind_param(
        self, value: datetime | None, dialect: object
    ) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is not None:
            return value.astimezone(UTC).replace(tzinfo=None)
        return value

    def process_result_value(
        self, value: datetime | None, dialect: object
    ) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
