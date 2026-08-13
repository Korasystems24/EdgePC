from datetime import UTC, datetime
from typing import Any, cast

from sqlalchemy import ColumnElement, CursorResult, delete, func, select, update
from sqlalchemy.exc import SQLAlchemyError

from src.domain.entities.edge_event import EdgeEvent
from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.exceptions import TelemetryStorageError
from src.domain.ports.telemetry_repository import StoredPacket, TelemetryRepository
from src.infrastructure.database.base_client import DatabaseClient
from src.infrastructure.database.mapping.telemetry_mapper import TelemetryPacketMapper
from src.infrastructure.database.models import EventOutbox, TelemetryPacketModel


class SqlAlchemyTelemetryRepository(TelemetryRepository):
    def __init__(self, db_client: DatabaseClient) -> None:
        self._db_client = db_client
        self._mapper = TelemetryPacketMapper()

    async def save(self, packet: TelemetryPacket) -> None:
        model = self._mapper.to_model(packet)
        try:
            async with self._db_client.session() as session:
                session.add(model)
                await session.commit()
        except SQLAlchemyError as e:
            raise TelemetryStorageError(
                f"Не удалось сохранить пакет телеметрии: {e}"
            ) from e

    @staticmethod
    def _pending_filter() -> tuple[ColumnElement[bool], ColumnElement[bool]]:
        return (
            TelemetryPacketModel.sent_at.is_(None),
            TelemetryPacketModel.quarantined_at.is_(None),
        )

    async def fetch_pending(self, limit: int) -> list[StoredPacket]:
        try:
            async with self._db_client.session() as session:
                result = await session.execute(
                    select(TelemetryPacketModel)
                    .where(*self._pending_filter())
                    .order_by(TelemetryPacketModel.id)
                    .limit(limit)
                )
                models = result.scalars().all()
                return [
                    StoredPacket(id=model.id, packet=self._mapper.to_domain(model))
                    for model in models
                ]
        except SQLAlchemyError as e:
            raise TelemetryStorageError(
                f"Не удалось получить ожидающие пакеты: {e}"
            ) from e

    async def count_pending(self) -> int:
        try:
            async with self._db_client.session() as session:
                result = await session.execute(
                    select(func.count())
                    .select_from(TelemetryPacketModel)
                    .where(*self._pending_filter())
                )
                return result.scalar_one()
        except SQLAlchemyError as e:
            raise TelemetryStorageError(
                f"Не удалось посчитать ожидающие пакеты: {e}"
            ) from e

    async def count_quarantined(self) -> int:
        try:
            async with self._db_client.session() as session:
                result = await session.execute(
                    select(func.count())
                    .select_from(TelemetryPacketModel)
                    .where(TelemetryPacketModel.quarantined_at.is_not(None))
                )
                return result.scalar_one()
        except SQLAlchemyError as e:
            raise TelemetryStorageError(
                f"Не удалось посчитать пакеты в карантине: {e}"
            ) from e

    async def mark_sent(self, ids: list[int]) -> None:
        if not ids:
            return
        try:
            async with self._db_client.session() as session:
                await session.execute(
                    update(TelemetryPacketModel)
                    .where(TelemetryPacketModel.id.in_(ids))
                    .values(sent_at=datetime.now(UTC))
                )
                await session.commit()
        except SQLAlchemyError as e:
            raise TelemetryStorageError(
                f"Не удалось пометить пакеты отправленными: {e}"
            ) from e

    async def record_rejection(self, ids: list[int], quarantine_after: int) -> None:
        if not ids:
            return
        try:
            async with self._db_client.session() as session:
                result = await session.execute(
                    select(TelemetryPacketModel).where(TelemetryPacketModel.id.in_(ids))
                )
                models = result.scalars().all()
                now = datetime.now(UTC)

                for model in models:
                    model.attempts += 1

                    newly_quarantined = (
                        model.attempts >= quarantine_after
                        and model.quarantined_at is None
                    )
                    if newly_quarantined:
                        model.quarantined_at = now
                        quarantine_event: EdgeEvent = EdgeEvent.packet_quarantined(
                            telemetry_packet_id=model.id,
                            telemetry_packet_event_id=model.event_id,
                            attempts=model.attempts,
                        )
                        session.add(
                            EventOutbox(
                                event_id=quarantine_event.event_id,
                                type=quarantine_event.type,
                                payload=quarantine_event.payload_as_dict(),
                            )
                        )

                await session.commit()
        except SQLAlchemyError as e:
            raise TelemetryStorageError(
                f"Не удалось обработать отклонение пакетов: {e}"
            ) from e

    async def purge_sent_older_than(self, cutoff: datetime) -> int:
        try:
            async with self._db_client.session() as session:
                result = await session.execute(
                    delete(TelemetryPacketModel).where(
                        TelemetryPacketModel.sent_at.is_not(None),
                        TelemetryPacketModel.sent_at < cutoff,
                    )
                )
                await session.commit()
                return cast(CursorResult[Any], result).rowcount
        except SQLAlchemyError as e:
            raise TelemetryStorageError(
                f"Не удалось удалить отправленные пакеты: {e}"
            ) from e
