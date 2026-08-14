from typing import Protocol

from src.infrastructure.radar.wcf.exceptions import (
    RadarCatalogNotReady,
    RadarSelectionError,
)


class WCFCatalog(Protocol):
    def get_park_names(self) -> list[str]: ...
    def get_vessel_names(self, park_name: str) -> list[str]: ...


class WCFResourceSelector:
    def __init__(self, client: WCFCatalog) -> None:
        self._client = client

    def select(self) -> tuple[str, str]:
        park = self._exactly_one(self._client.get_park_names(), "parks")
        vessel = self._exactly_one(
            self._client.get_vessel_names(park),
            "vessels",
        )
        return park, vessel

    @staticmethod
    def _exactly_one(items: list[str], kind: str) -> str:
        if len(items) == 0:
            raise RadarCatalogNotReady(f"каталог LIMACO ещё пуст: {kind}=0")
        if len(items) != 1:
            raise RadarSelectionError(f"Найдено {kind}: {len(items)}")
        return items[0]
