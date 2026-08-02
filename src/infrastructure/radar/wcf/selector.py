from src.infrastructure.radar.wcf.exceptions import RadarSelectionError
from src.infrastructure.radar.wcf.wcf_client import WCFClient


class WCFResourceSelector:
    def __init__(self, client: WCFClient) -> None:
        self._client = client

    def select(self) -> tuple[str, str]:
        parks = self._client.get_park_names()

        if len(parks) != 1:
            raise RadarSelectionError(f"Найдено parks: {len(parks)}")

        park = parks[0]
        vessels = self._client.get_vessel_names(park)

        if len(vessels) != 1:
            raise RadarSelectionError(f"Найдено vessels: {len(vessels)}")

        return park, vessels[0]
