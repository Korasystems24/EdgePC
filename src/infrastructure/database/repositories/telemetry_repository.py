from sqlalchemy.exc import SQLAlchemyError

from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.exceptions import TelemetryStorageError
from src.domain.ports.telemetry_repository import TelemetryRepository
from src.infrastructure.database.base_client import DatabaseClient
from src.infrastructure.database.models import TelemetryPacketModel


class SqlAlchemyTelemetryRepository(TelemetryRepository):
    def __init__(self, db_client: DatabaseClient) -> None:
        self._db_client = db_client

    async def save(self, packet: TelemetryPacket) -> None:
        model = TelemetryPacketModel(
            ts=packet.ts,
            avg_level=packet.avg_level,
            mass=packet.mass,
            volume=packet.volume,
            source_state=packet.source_state.value,
            beams=[beam.model_dump(mode="json") for beam in packet.beams],
        )
        try:
            async with self._db_client.session() as session:
                session.add(model)
                await session.commit()
        except SQLAlchemyError as e:
            raise TelemetryStorageError(
                f"Не удалось сохранить пакет телеметрии: {e}"
            ) from e
