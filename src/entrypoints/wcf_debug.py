from zeep.helpers import serialize_object  # pyright: ignore[reportUnknownVariableType]

from src.config.settings import settings
from src.infrastructure.radar.wcf.dto import WCFDataDTO
from src.infrastructure.radar.wcf.selector import WCFResourceSelector
from src.infrastructure.radar.wcf.wcf_client import WCFClient


async def run() -> None:
    client = WCFClient(wsdl_url=settings.wcf_wsdl_url)
    client.initialize()
    selector = WCFResourceSelector(client)
    park, vessel = selector.select()
    raw = serialize_object(client.get_vessel(park, vessel))

    print(raw.get("Sensors").get("SensorCalculatedData"))
