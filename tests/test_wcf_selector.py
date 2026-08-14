import pytest

from src.infrastructure.radar.wcf.exceptions import (
    RadarCatalogNotReady,
    RadarSelectionError,
)
from src.infrastructure.radar.wcf.selector import WCFResourceSelector


class _FakeClient:
    def __init__(self, parks: list[str], vessels: list[str]) -> None:
        self._parks = parks
        self._vessels = vessels

    def get_park_names(self) -> list[str]:
        return list(self._parks)

    def get_vessel_names(self, park_name: str) -> list[str]:
        assert park_name == self._parks[0]
        return list(self._vessels)


def test_empty_parks_is_not_ready() -> None:
    selector = WCFResourceSelector(_FakeClient(parks=[], vessels=[]))
    with pytest.raises(RadarCatalogNotReady, match="parks=0"):
        selector.select()


def test_empty_vessels_is_not_ready() -> None:
    selector = WCFResourceSelector(_FakeClient(parks=["park-a"], vessels=[]))
    with pytest.raises(RadarCatalogNotReady, match="vessels=0"):
        selector.select()


def test_two_parks_is_selection_error() -> None:
    selector = WCFResourceSelector(_FakeClient(parks=["a", "b"], vessels=["v"]))
    with pytest.raises(RadarSelectionError, match="parks: 2"):
        selector.select()


def test_exactly_one_park_and_vessel() -> None:
    selector = WCFResourceSelector(
        _FakeClient(parks=["park-a"], vessels=["tank-1"])
    )
    assert selector.select() == ("park-a", "tank-1")
