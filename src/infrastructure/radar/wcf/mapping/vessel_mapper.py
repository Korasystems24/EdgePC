from datetime import UTC, datetime

from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.enum import BeamStatus
from src.domain.value_objects.beam import Beam
from src.infrastructure.radar.wcf.dto import WCFBeamDTO, WCFDataDTO, WCFVesselDTO


class RadarMapper:
    def to_entity(self, vessel: WCFVesselDTO, tank_data: WCFDataDTO) -> TelemetryPacket:
        return TelemetryPacket(
            ts=datetime.now(UTC),
            avg_level=vessel.average_level,
            mass=tank_data.mass,
            volume=None,  # TODO: уточнить формулу расчёта объёма
            beams=self._map_beams(vessel),
        )

    @staticmethod
    def _calculate_volume(tank_data: WCFDataDTO) -> float | None:
        volume = tank_data.volume / 100 if tank_data.volume is not None else None
        return volume

    @staticmethod
    def _map_beams(vessel: WCFVesselDTO) -> list[Beam]:
        return [RadarMapper._to_beam(raw) for raw in vessel.first_sensor_beams()]

    @staticmethod
    def _to_beam(raw: WCFBeamDTO) -> Beam:
        return Beam(
            height=raw.end_point_height,
            raw_status=raw.raw_status,
            status=tuple(BeamStatus(s) for s in (raw.status or [])),
        )
