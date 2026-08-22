from typing import cast

import requests
from zeep import Client

from .exceptions import ClientNotInitialized
from .loopback_transport import LoopbackRewriteTransport
from .types import WCFServiceProtocol


class WCFClient:
    def __init__(
        self,
        wsdl_url: str | None,
        timeout: int,
        loopback_alias: str | None = None,
    ) -> None:
        self.wsdl_url = wsdl_url
        self._client: Client | None = None

        self._session = requests.Session()
        self._transport = LoopbackRewriteTransport(
            session=self._session,
            timeout=timeout,
            operation_timeout=timeout,
            loopback_alias=loopback_alias,
        )

    def close(self) -> None:
        self._session.close()

    def initialize(self) -> None:
        if self._client is not None:
            return
        self._client = Client(
            wsdl=self.wsdl_url,
            transport=self._transport,
        )

    @property
    def is_initialized(self) -> bool:
        return self._client is not None

    @property
    def service(self) -> WCFServiceProtocol:
        """zeep собирает методы сервиса из WSDL в рантайме, поэтому статически
        они выглядят как `Any`: протокол объявляет то, чем мы пользуемся."""
        if self._client is None:
            raise ClientNotInitialized("WCF клиент не инициализирован")
        return cast(WCFServiceProtocol, self._client.service)

    def get_park_names(self) -> list[str]:
        response = self.service.GetParkNames()
        return list(response or [])

    def get_vessel_names(self, park_name: str) -> list[str]:
        response = self.service.GetVesselNames(park_name)
        return list(response or [])

    def get_vessel(self, park_name: str, vessel_name: str) -> object:
        return self.service.GetVessel(park_name, vessel_name)

    def get_data(self) -> str:
        response = self.service.GetData()
        return response or ""
