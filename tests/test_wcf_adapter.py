import asyncio
from concurrent.futures import CancelledError as FuturesCancelledError
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from src.config.settings import MockIOSettings, WcfSettings
from src.domain.enum import RadarSourceType
from src.domain.exceptions import RadarSourceMisconfigured, RadarSourceUnavailable
from src.infrastructure.radar.factory import create_radar_adapter
from src.infrastructure.radar.wcf_adapter import WCFRadarAdapter


def _adapter() -> WCFRadarAdapter:
    return WCFRadarAdapter(client=MagicMock(), mapper=MagicMock())


async def test_unexpected_read_error_becomes_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _adapter()

    async def boom() -> None:
        raise RuntimeError("xml parse")

    monkeypatch.setattr(adapter, "_read_packet_in_executor", boom)
    try:
        with pytest.raises(
            RadarSourceUnavailable, match="неожиданная ошибка источника"
        ):
            await adapter.read_packet()
    finally:
        await adapter.close()


async def test_domain_radar_error_is_not_rewrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _adapter()

    async def boom() -> None:
        raise RadarSourceMisconfigured("найдено 2 vessel")

    monkeypatch.setattr(adapter, "_read_packet_in_executor", boom)
    try:
        with pytest.raises(RadarSourceMisconfigured, match="найдено 2 vessel"):
            await adapter.read_packet()
    finally:
        await adapter.close()


async def test_asyncio_cancelled_error_is_not_wrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _adapter()

    async def boom() -> None:
        raise asyncio.CancelledError()

    monkeypatch.setattr(adapter, "_read_packet_in_executor", boom)
    try:
        with pytest.raises(asyncio.CancelledError):
            await adapter.read_packet()
    finally:
        await adapter.close()


async def test_futures_cancelled_error_is_not_wrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    adapter = _adapter()

    async def boom() -> None:
        raise FuturesCancelledError()

    monkeypatch.setattr(adapter, "_read_packet_in_executor", boom)
    try:
        with pytest.raises(FuturesCancelledError):
            await adapter.read_packet()
    finally:
        await adapter.close()


class _FakeCatalogClient:
    def __init__(self) -> None:
        self.initialize_calls = 0
        self._initialized = False

    @property
    def is_initialized(self) -> bool:
        return self._initialized

    def initialize(self) -> None:
        self.initialize_calls += 1
        self._initialized = True

    def get_park_names(self) -> list[str]:
        return []

    def get_vessel_names(self, park_name: str) -> list[str]:
        return []

    def close(self) -> None:
        return None


async def test_catalog_not_ready_does_not_reinitialize_client() -> None:
    client = _FakeCatalogClient()
    adapter = WCFRadarAdapter(client=client, mapper=MagicMock())
    try:
        with pytest.raises(RadarSourceUnavailable, match="каталог LIMACO ещё пуст"):
            await adapter.read_packet()
        with pytest.raises(RadarSourceUnavailable, match="каталог LIMACO ещё пуст"):
            await adapter.read_packet()
        assert client.initialize_calls == 1
        assert client.is_initialized is True
    finally:
        await adapter.close()


def test_empty_wsdl_is_misconfigured() -> None:
    mock = MockIOSettings()
    with pytest.raises(RadarSourceMisconfigured, match="WCF__WSDL_URL"):
        create_radar_adapter(
            RadarSourceType.WCF,
            wcf=WcfSettings(wsdl_url=None),
            mock=mock,
        )


def test_blank_wsdl_is_rejected_by_settings() -> None:
    with pytest.raises(ValidationError, match="wcf.wsdl_url must not be blank"):
        WcfSettings(wsdl_url="   ")
    with pytest.raises(ValidationError, match="wcf.wsdl_url must not be blank"):
        WcfSettings(wsdl_url="")
