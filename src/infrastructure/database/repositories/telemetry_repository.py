from datetime import UTC, datetime
from functools import partial
from typing import Any, cast

from loguru import logger
from sqlalchemy import ColumnElement, CursorResult, delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from src.domain.entities.edge_event import EdgeEvent
from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.exceptions import TelemetryStorageError
from src.domain.ports.telemetry_repository import StoredPacket, TelemetryRepository
from src.infrastructure.database.base_client import DatabaseClient
from src.infrastructure.database.mapping import event_mapper, telemetry_mapper
from src.infrastructure.database.models import TelemetryPacketModel
from src.infrastructure.database.repositories.pending import (
    read_pending,
    storage_errors,
)

storage_error = partial(storage_errors, error=TelemetryStorageError)


class SqlAlchemyTelemetryRepository(TelemetryRepository):
    def __init__(self, db_client: DatabaseClient) -> None:
        self._db_client = db_client

    async def save(self, packet: TelemetryPacket) -> None:
        """Записать новый пакет."""
        model = telemetry_mapper.to_model(packet)
        with storage_error("Не удалось сохранить пакет телеметрии"):
            async with self._db_client.session() as session:
                session.add(model)
                await session.commit()

    @staticmethod
    def _pending_filter() -> tuple[ColumnElement[bool], ColumnElement[bool]]:
        """Ещё не уехало и не в карантине."""
        return (
            TelemetryPacketModel.sent_at.is_(None),
            TelemetryPacketModel.quarantined_at.is_(None),
        )

    async def get_pending(self, limit: int) -> list[StoredPacket]:
        """Взять пачку ещё не отправленных."""
        query = (
            select(TelemetryPacketModel)
            .where(*self._pending_filter())
            .order_by(TelemetryPacketModel.id)
            .limit(limit)
        )
        with storage_error("Не удалось получить ожидающие пакеты"):
            async with self._db_client.session() as session:
                return await read_pending(
                    session,
                    query,
                    build=self._to_stored_packet,
                    on_corrupt=partial(self._quarantine_corrupt, session),
                )

    @staticmethod
    def _to_stored_packet(model: TelemetryPacketModel) -> StoredPacket:
        return StoredPacket(id=model.id, packet=telemetry_mapper.to_domain(model))

    @staticmethod
    def _quarantine_corrupt(
        session: AsyncSession, model: TelemetryPacketModel, error: Exception
    ) -> None:
        logger.error("Карантин битого пакета id={}: {}", model.id, error)
        model.attempts += 1
        model.quarantined_at = datetime.now(UTC)
        session.add(
            event_mapper.to_model(
                EdgeEvent.packet_quarantined(
                    telemetry_packet_id=model.id,
                    telemetry_packet_event_id=model.event_id,
                    attempts=model.attempts,
                )
            )
        )

    async def get_pending_count(self) -> int:
        """Сколько ещё ждёт отправки."""
        with storage_error("Не удалось посчитать ожидающие пакеты"):
            async with self._db_client.session() as session:
                result = await session.execute(
                    select(func.count())
                    .select_from(TelemetryPacketModel)
                    .where(*self._pending_filter())
                )
                return result.scalar_one()

    async def get_quarantined_count(self) -> int:
        """Сколько лежит в карантине."""
        with storage_error("Не удалось посчитать пакеты в карантине"):
            async with self._db_client.session() as session:
                result = await session.execute(
                    select(func.count())
                    .select_from(TelemetryPacketModel)
                    .where(TelemetryPacketModel.quarantined_at.is_not(None))
                )
                return result.scalar_one()

    async def set_sent(self, ids: list[int]) -> None:
        """Пометить: сервер принял."""
        if not ids:
            return
        with storage_error("Не удалось пометить пакеты отправленными"):
            async with self._db_client.session() as session:
                await session.execute(
                    update(TelemetryPacketModel)
                    .where(TelemetryPacketModel.id.in_(ids))
                    .values(sent_at=datetime.now(UTC))
                )
                await session.commit()

    async def update_rejected(self, ids: list[int], quarantine_after: int) -> None:
        """Сервер отверг: +1 попытка, после порога убрать в карантин.

        Событие карантина пишется той же транзакцией, что и сам карантин: краш
        между ними потерял бы историю, по которой человек разбирает отказы.
        """
        if not ids:
            return
        with storage_error("Не удалось обработать отклонение пакетов"):
            async with self._db_client.session() as session:
                result = await session.execute(
                    select(TelemetryPacketModel).where(
                        TelemetryPacketModel.id.in_(ids)
                    )
                )
                now = datetime.now(UTC)

                for model in result.scalars().all():
                    model.attempts += 1
                    newly_quarantined = (
                        model.attempts >= quarantine_after
                        and model.quarantined_at is None
                    )
                    if newly_quarantined:
                        model.quarantined_at = now
                        session.add(
                            event_mapper.to_model(
                                EdgeEvent.packet_quarantined(
                                    telemetry_packet_id=model.id,
                                    telemetry_packet_event_id=model.event_id,
                                    attempts=model.attempts,
                                )
                            )
                        )

                await session.commit()

    async def delete_sent_older_than(self, cutoff: datetime) -> int:
        """Стереть уже отправленные старше даты."""
        with storage_error("Не удалось удалить отправленные пакеты"):
            async with self._db_client.session() as session:
                result = await session.execute(
                    delete(TelemetryPacketModel).where(
                        TelemetryPacketModel.sent_at.is_not(None),
                        TelemetryPacketModel.sent_at < cutoff,
                    )
                )
                await session.commit()
                return cast(CursorResult[Any], result).rowcount
