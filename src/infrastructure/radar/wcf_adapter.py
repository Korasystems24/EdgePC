from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.ports.radar_adapter import RadarAdapter
from src.infrastructure.radar.wcf.dto import WCFDataDTO, WCFVesselDTO
from src.infrastructure.radar.wcf.mapping.vessel_mapper import RadarMapper
from src.infrastructure.radar.wcf.wcf_client import WCFClient


class WCFRadarAdapter(RadarAdapter):
    def __init__(
        self,
        client: WCFClient,
        mapper: RadarMapper,
        park_name: str,
        vessel_name: str,
    ) -> None:
        self._client = client
        self._mapper = mapper
        self._park_name = park_name
        self._vessel_name = vessel_name

    async def read_packet(self) -> TelemetryPacket:
        raw_vessel = self._client.get_vessel(self._park_name, self._vessel_name)
        vessel = WCFVesselDTO.from_zeep(raw_vessel)

        raw_data = self._client.get_data()
        tank_data = WCFDataDTO.from_raw(raw_data)

        return self._mapper.to_entity(vessel, tank_data)
