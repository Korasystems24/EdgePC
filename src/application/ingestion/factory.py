from src.config.settings import Settings
from src.domain.enum import RadarSourceType
from src.domain.ports.radar_adapter import RadarAdapter
from src.infrastructure.radar.mock_adapter import MockRadarAdapter
from src.infrastructure.radar.wcf.mapping.vessel_mapper import RadarMapper
from src.infrastructure.radar.wcf.selector import WCFResourceSelector
from src.infrastructure.radar.wcf.wcf_client import WCFClient
from src.infrastructure.radar.wcf_adapter import WCFRadarAdapter


class RadarAdapterFactory:
    @staticmethod
    def create_adapter(settings: Settings) -> RadarAdapter:

        if settings.radar_source == RadarSourceType.MOCK:
            return MockRadarAdapter(
                delay=settings.mock_delay,
                fail=settings.mock_fail,
            )

        if settings.radar_source == RadarSourceType.WCF:
            client = WCFClient(wsdl_url=settings.wcf_wsdl_url)
            client.initialize()

            selector = WCFResourceSelector(client)
            park_name, vessel_name = selector.select()

            return WCFRadarAdapter(
                client=client,
                mapper=RadarMapper(),
                park_name=park_name,
                vessel_name=vessel_name,
            )

        raise ValueError(f"Неподдерживаемый источник данных: {settings.radar_source}")
