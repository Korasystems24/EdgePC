from datetime import UTC, datetime
from typing import Any, cast

from loguru import logger
from pydantic import ValidationError
from sqlalchemy import ColumnElement, CursorResult, delete, func, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from src.domain.entities.edge_event import EdgeEvent
from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.exceptions import TelemetryStorageError
from src.domain.ports.telemetry_repository import StoredPacket, TelemetryRepository
from src.infrastructure.database.base_client import DatabaseClient
from src.infrastructure.database.mapping.event_mapper import EventOutboxMapper
from src.infrastructure.database.mapping.telemetry_mapper import TelemetryPacketMapper
from src.infrastructure.database.models import TelemetryPacketModel


class SqlAlchemyTelemetryRepository(TelemetryRepository):
    def __init__(self, db_client: DatabaseClient) -> None:
        self._db_client = db_client
        self._mapper = TelemetryPacketMapper()

    async def save(self, packet: TelemetryPacket) -> None:
        """Записать новый пакет."""
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
        """Ещё не уехало и не в карантине."""
        return (
            TelemetryPacketModel.sent_at.is_(None),
            TelemetryPacketModel.quarantined_at.is_(None),
        )

    async def get_pending(self, limit: int) -> list[StoredPacket]:
        """Взять пачку ещё не отправленных."""
        try:
            async with self._db_client.session() as session:
                pending: list[StoredPacket] = []
                processed_ids: list[int] = []
                quarantined = False
                while len(pending) < limit:
                    query = (
                        select(TelemetryPacketModel)
                        .where(*self._pending_filter())
                        .order_by(TelemetryPacketModel.id)
                        .limit(limit - len(pending))
                    )
                    if processed_ids:
                        query = query.where(
                            TelemetryPacketModel.id.notin_(processed_ids)
                        )
                    result = await session.execute(query)
                    models = result.scalars().all()
                    if not models:
                        break
                    quarantined_this_batch = False
                    for model in models:
                        processed_ids.append(model.id)
                        try:
                            pending.append(
                                StoredPacket(
                                    id=model.id,
                                    packet=self._mapper.to_domain(model),
                                )
                            )
                        except (ValidationError, TypeError, ValueError) as e:
                            logger.error(
                                "Карантин битого пакета id={}: {}",
                                model.id,
                                e,
                            )
                            self._quarantine_corrupt(session, model)
                            quarantined_this_batch = True
                            quarantined = True
                    if quarantined_this_batch:
                        await session.flush()
                    else:
                        break
                if quarantined:
                    await session.commit()
                return pending
        except SQLAlchemyError as e:
            raise TelemetryStorageError(
                f"Не удалось получить ожидающие пакеты: {e}"
            ) from e

    @staticmethod
    def _quarantine_corrupt(
        session: AsyncSession, model: TelemetryPacketModel
    ) -> None:
        now = datetime.now(UTC)
        model.attempts += 1
        model.quarantined_at = now
        quarantine_event: EdgeEvent = EdgeEvent.packet_quarantined(
            telemetry_packet_id=model.id,
            telemetry_packet_event_id=model.event_id,
            attempts=model.attempts,
        )
        session.add(EventOutboxMapper.to_model(quarantine_event))

    async def get_pending_count(self) -> int:
        """Сколько ещё ждёт отправки."""
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

    async def get_quarantined_count(self) -> int:
        """Сколько лежит в карантине."""
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

    async def set_sent(self, ids: list[int]) -> None:
        """Пометить: сервер принял."""
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

    async def update_rejected(self, ids: list[int], quarantine_after: int) -> None:
        """Сервер отверг: +1 попытка, после порога убрать в карантин."""
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
                        session.add(EventOutboxMapper.to_model(quarantine_event))

                await session.commit()
        except SQLAlchemyError as e:
            raise TelemetryStorageError(
                f"Не удалось обработать отклонение пакетов: {e}"
            ) from e

    async def delete_sent_older_than(self, cutoff: datetime) -> int:
        """Стереть уже отправленные старше даты."""
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
