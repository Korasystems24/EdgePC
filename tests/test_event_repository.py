from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import select

from src.domain.entities.edge_event import EdgeEvent
from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.enum import EdgeEventType
from src.domain.exceptions import TelemetryStorageError
from src.domain.value_objects.beam import Beam
from src.infrastructure.database.models import EventOutboxModel, TelemetryPacketModel
from src.infrastructure.database.repositories.event_repository import (
    SqlAlchemyEventRepository,
)
from src.infrastructure.database.repositories.telemetry_repository import (
    SqlAlchemyTelemetryRepository,
)
from src.infrastructure.database.sqlite_client import SqliteClient


@pytest.fixture
async def db_client() -> AsyncIterator[SqliteClient]:
    client = SqliteClient("sqlite+aiosqlite:///:memory:")
    await client.init_database()
    try:
        yield client
    finally:
        await client.close()


@pytest.fixture
def events(db_client: SqliteClient) -> SqlAlchemyEventRepository:
    return SqlAlchemyEventRepository(db_client)


def _packet() -> TelemetryPacket:
    beams = [Beam(height=1.0, raw_status=0, status=()) for _ in range(64)]
    return TelemetryPacket(
        ts=datetime.now(tz=UTC),
        avg_level=1.0,
        mass=1.0,
        volume=1.0,
        beams=beams,
    )


async def test_save_then_get_pending_roundtrip(
    events: SqlAlchemyEventRepository,
) -> None:
    event = EdgeEvent.radar_down(reason="WCF timeout")
    await events.save(event)

    pending = await events.get_pending(10)
    assert len(pending) == 1
    stored = pending[0]
    assert stored.id > 0
    assert stored.event == event
    assert stored.created_at.tzinfo is not None
    assert stored.event.type is EdgeEventType.RADAR_DOWN
    assert stored.event.payload.reason == "WCF timeout"


async def test_get_pending_orders_by_id_and_respects_limit(
    events: SqlAlchemyEventRepository,
) -> None:
    first = EdgeEvent.radar_down(reason="down")
    second = EdgeEvent.radar_recovered(downtime_seconds=12.5)
    await events.save(first)
    await events.save(second)

    pending = await events.get_pending(1)
    assert [item.event.event_id for item in pending] == [first.event_id]


async def test_set_sent_removes_from_pending(
    events: SqlAlchemyEventRepository,
) -> None:
    kept = EdgeEvent.worker_restarted(worker="ingestion")
    sent = EdgeEvent.fatal_error(
        worker="dispatch",
        reason="misconfigured",
        restartable=False,
    )
    await events.save(kept)
    await events.save(sent)

    pending = await events.get_pending(10)
    sent_row = next(item for item in pending if item.event.event_id == sent.event_id)
    await events.set_sent([sent_row.id])

    remaining = await events.get_pending(10)
    assert [item.event.event_id for item in remaining] == [kept.event_id]


async def test_set_sent_empty_ids_is_noop(
    events: SqlAlchemyEventRepository,
) -> None:
    await events.set_sent([])
    assert await events.get_pending(10) == []


async def test_quarantine_row_is_visible_through_event_repository(
    db_client: SqliteClient,
    events: SqlAlchemyEventRepository,
) -> None:
    telemetry = SqlAlchemyTelemetryRepository(db_client)
    packet = _packet()
    await telemetry.save(packet)

    stored = (await telemetry.get_pending(1))[0]
    await telemetry.update_rejected([stored.id], quarantine_after=1)

    pending = await events.get_pending(10)
    assert len(pending) == 1
    event = pending[0].event
    assert event.type is EdgeEventType.PACKET_QUARANTINED
    assert event.payload.telemetry_packet_id == stored.id
    assert event.payload.telemetry_packet_event_id == packet.event_id
    assert event.payload.attempts == 1


async def test_corrupt_pending_row_is_quarantined(
    db_client: SqliteClient,
    events: SqlAlchemyEventRepository,
) -> None:
    async with db_client.session() as session:
        session.add(
            EventOutboxModel(
                event_id=uuid4(),
                type="radar_down",
                payload={"garbage": True},
            )
        )
        await session.commit()
    good = EdgeEvent.radar_recovered(downtime_seconds=3.0)
    await events.save(good)

    pending = await events.get_pending(10)
    assert [item.event.event_id for item in pending] == [good.event_id]

    async with db_client.session() as session:
        result = await session.execute(select(EventOutboxModel))
        rows = result.scalars().all()
    corrupt = next(row for row in rows if row.payload == {"garbage": True})
    assert corrupt.quarantined_at is not None
    assert corrupt.sent_at is None


async def test_corrupt_only_event_limit_does_not_stall(
    events: SqlAlchemyEventRepository,
    db_client: SqliteClient,
) -> None:
    async with db_client.session() as session:
        session.add(
            EventOutboxModel(
                event_id=uuid4(),
                type="radar_down",
                payload={"garbage": True},
            )
        )
        await session.commit()
    good = EdgeEvent.radar_down(reason="later")
    await events.save(good)

    pending = await events.get_pending(1)
    assert [item.event.event_id for item in pending] == [good.event_id]


async def test_corrupt_pending_telemetry_row_is_quarantined(
    db_client: SqliteClient,
) -> None:
    telemetry = SqlAlchemyTelemetryRepository(db_client)
    async with db_client.session() as session:
        session.add(
            TelemetryPacketModel(
                event_id=uuid4(),
                ts=datetime.now(tz=UTC),
                avg_level=1.0,
                mass=1.0,
                volume=1.0,
                beams=[{"garbage": True}],
            )
        )
        await session.commit()
    await telemetry.save(_packet())

    pending = await telemetry.get_pending(10)
    assert len(pending) == 1
    assert len(pending[0].packet.beams) == 64
    assert await telemetry.get_pending_count() == 1
    assert await telemetry.get_quarantined_count() == 1


async def test_corrupt_only_telemetry_limit_does_not_stall(
    db_client: SqliteClient,
) -> None:
    telemetry = SqlAlchemyTelemetryRepository(db_client)
    async with db_client.session() as session:
        session.add(
            TelemetryPacketModel(
                event_id=uuid4(),
                ts=datetime.now(tz=UTC),
                avg_level=1.0,
                mass=1.0,
                volume=1.0,
                beams=[{"garbage": True}],
            )
        )
        await session.commit()
    await telemetry.save(_packet())

    pending = await telemetry.get_pending(1)
    assert len(pending) == 1
    assert len(pending[0].packet.beams) == 64


async def test_telemetry_event_id_is_unique(db_client: SqliteClient) -> None:
    telemetry = SqlAlchemyTelemetryRepository(db_client)
    packet = _packet()
    await telemetry.save(packet)
    duplicate = TelemetryPacket(
        event_id=packet.event_id,
        ts=packet.ts,
        avg_level=packet.avg_level,
        mass=packet.mass,
        volume=packet.volume,
        beams=packet.beams,
    )
    with pytest.raises(TelemetryStorageError):
        await telemetry.save(duplicate)
