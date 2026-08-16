from src.config.settings import MockIOSettings, WcfSettings
from src.domain.enum import RadarSourceType
from src.domain.exceptions import RadarSourceMisconfigured
from src.domain.ports.radar_adapter import RadarAdapter
from src.infrastructure.radar.mock_adapter import MockRadarAdapter
from src.infrastructure.radar.wcf.mapping.vessel_mapper import RadarMapper
from src.infrastructure.radar.wcf.wcf_client import WCFClient
from src.infrastructure.radar.wcf_adapter import WCFRadarAdapter


def create_radar_adapter(
    source: RadarSourceType,
    *,
    wcf: WcfSettings,
    mock: MockIOSettings,
) -> RadarAdapter:
    if source == RadarSourceType.MOCK:
        return MockRadarAdapter(delay=mock.delay, fail=mock.fail_rate)

    if source == RadarSourceType.WCF:
        if not wcf.wsdl_url or not wcf.wsdl_url.strip():
            raise RadarSourceMisconfigured("WCF__WSDL_URL не задан")
        return WCFRadarAdapter(
            client=WCFClient(
                wsdl_url=wcf.wsdl_url,
                timeout=wcf.timeout,
            ),
            mapper=RadarMapper(),
        )

    raise RadarSourceMisconfigured(
        f"Неподдерживаемый источник данных: {source}"
    )
