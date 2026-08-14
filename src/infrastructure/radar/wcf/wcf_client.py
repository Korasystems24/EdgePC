from typing import cast

import requests
from zeep import Client, Transport

from .exceptions import ClientNotInitialized
from .types import WCFServiceProtocol, ZeepClientProtocol


class WCFClient:
    def __init__(
        self,
        wsdl_url: str | None,
        timeout: int,
    ) -> None:

        self.wsdl_url = wsdl_url
        self._client: Client | None = None

        self._session = requests.Session()
        self._transport = Transport(
            session=self._session,
            timeout=timeout,
            operation_timeout=timeout,
        )

    def close(self) -> None:
        self._session.close()

    def initialize(self) -> None:

        self._client = Client(
            wsdl=self.wsdl_url,
            transport=self._transport,
        )

    @property
    def client(self) -> ZeepClientProtocol:
        if self._client is None:
            raise ClientNotInitialized("WCF клиент не инициализирован")
        return cast(ZeepClientProtocol, self._client)

    @property
    def service(self) -> WCFServiceProtocol:
        return self.client.service

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

    def get_wsdl_info(self) -> None:
        return self.client.wsdl.dump()
