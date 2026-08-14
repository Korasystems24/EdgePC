from src.config.settings import Settings
from src.domain.enum import RadarSourceType
from src.domain.exceptions import RadarSourceMisconfigured
from src.domain.ports.radar_adapter import RadarAdapter
from src.infrastructure.radar.mock_adapter import MockRadarAdapter
from src.infrastructure.radar.wcf.mapping.vessel_mapper import RadarMapper
from src.infrastructure.radar.wcf.wcf_client import WCFClient
from src.infrastructure.radar.wcf_adapter import WCFRadarAdapter


def create_radar_adapter(settings: Settings) -> RadarAdapter:
    if settings.radar_source == RadarSourceType.MOCK:
        return MockRadarAdapter(
            delay=settings.mock.delay,
            fail=settings.mock.fail_rate,
        )

    if settings.radar_source == RadarSourceType.WCF:
        return WCFRadarAdapter(
            client=WCFClient(
                wsdl_url=settings.wcf.wsdl_url,
                timeout=settings.wcf.timeout,
            ),
            mapper=RadarMapper(),
        )

    raise RadarSourceMisconfigured(
        f"Неподдерживаемый источник данных: {settings.radar_source}"
    )
