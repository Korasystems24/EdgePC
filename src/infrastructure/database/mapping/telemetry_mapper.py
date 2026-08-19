from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.value_objects.beam import Beam
from src.infrastructure.database.models import TelemetryPacketModel


def to_domain(model: TelemetryPacketModel) -> TelemetryPacket:
    return TelemetryPacket(
        event_id=model.event_id,
        ts=model.ts,
        avg_level=model.avg_level,
        mass=model.mass,
        volume=model.volume,
        beams=[Beam.model_validate(raw) for raw in model.beams],
    )


def to_model(packet: TelemetryPacket) -> TelemetryPacketModel:
    return TelemetryPacketModel(
        event_id=packet.event_id,
        ts=packet.ts,
        avg_level=packet.avg_level,
        mass=packet.mass,
        volume=packet.volume,
        beams=[beam.model_dump(mode="json") for beam in packet.beams],
    )
