from requests import RequestException
from zeep.exceptions import Error as ZeepError

from src.config.settings import Settings
from src.domain.enum import RadarSourceType
from src.domain.exceptions import RadarSourceMisconfigured, RadarSourceUnavailable
from src.domain.ports.radar_adapter import RadarAdapter
from src.infrastructure.radar.mock_adapter import MockRadarAdapter
from src.infrastructure.radar.wcf.exceptions import RadarSelectionError
from src.infrastructure.radar.wcf.mapping.vessel_mapper import RadarMapper
from src.infrastructure.radar.wcf.selector import WCFResourceSelector
from src.infrastructure.radar.wcf.wcf_client import WCFClient
from src.infrastructure.radar.wcf_adapter import WCFRadarAdapter


def create_radar_adapter(settings: Settings) -> RadarAdapter:
    if settings.radar_source == RadarSourceType.MOCK:
        return MockRadarAdapter(
            delay=settings.mock.delay,
            fail=settings.mock.fail_rate,
        )

    if settings.radar_source == RadarSourceType.WCF:
        try:
            client = WCFClient(wsdl_url=settings.wcf.wsdl_url)
            client.initialize()

            selector = WCFResourceSelector(client)
            park_name, vessel_name = selector.select()
        except (RequestException, ZeepError) as e:
            raise RadarSourceUnavailable(
                f"WCF недоступен при инициализации: {e}"
            ) from e
        except RadarSelectionError as e:
            raise RadarSourceMisconfigured(str(e)) from e

        return WCFRadarAdapter(
            client=client,
            mapper=RadarMapper(),
            park_name=park_name,
            vessel_name=vessel_name,
        )

    raise RadarSourceMisconfigured(
        f"Неподдерживаемый источник данных: {settings.radar_source}"
    )
