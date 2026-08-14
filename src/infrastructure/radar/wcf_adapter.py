import asyncio
from concurrent.futures import Future, ThreadPoolExecutor

from pydantic_core import ValidationError
from requests import RequestException
from zeep.exceptions import Error as ZeepError

import src.domain.exceptions
from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.exceptions import RadarSourceHung
from src.domain.ports.radar_adapter import RadarAdapter
from src.infrastructure.radar.wcf.dto import WCFDataDTO, WCFVesselDTO
from src.infrastructure.radar.wcf.exceptions import (
    ClientNotInitialized,
    RadarCatalogNotReady,
    RadarSelectionError,
    ValueRadarError,
)
from src.infrastructure.radar.wcf.mapping.vessel_mapper import RadarMapper
from src.infrastructure.radar.wcf.selector import WCFResourceSelector
from src.infrastructure.radar.wcf.wcf_client import WCFClient


class WCFRadarAdapter(RadarAdapter):
    def __init__(self, client: WCFClient, mapper: RadarMapper) -> None:
        self._client = client
        self._mapper = mapper
        self._park_name: str | None = None
        self._vessel_name: str | None = None
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="wcf")
        self._in_flight: Future[TelemetryPacket] | None = None

    def raise_if_hung(self) -> None:
        if self._in_flight is not None and not self._in_flight.done():
            raise RadarSourceHung(
                "источник не вернул управление после дедлайна тика"
            )

    def _ensure_ready(self) -> None:
        if self._park_name is not None and self._vessel_name is not None:
            return

        self._client.initialize()
        park_name, vessel_name = WCFResourceSelector(self._client).select()
        self._park_name = park_name
        self._vessel_name = vessel_name

    def _read_packet_sync(self) -> TelemetryPacket:
        self._ensure_ready()
        assert self._park_name is not None
        assert self._vessel_name is not None

        raw_vessel = self._client.get_vessel(self._park_name, self._vessel_name)
        vessel = WCFVesselDTO.from_zeep(raw_vessel)

        raw_data = self._client.get_data()
        tank_data = WCFDataDTO.from_raw(raw_data)

        return self._mapper.to_entity(vessel, tank_data)

    async def read_packet(self) -> TelemetryPacket:
        try:
            return await self._read_packet_in_executor()
        except ClientNotInitialized as e:
            raise src.domain.exceptions.RadarSourceUnavailable(str(e)) from e
        except (RequestException, ZeepError) as e:
            raise src.domain.exceptions.RadarSourceUnavailable(
                f"WCF недоступен: {e}"
            ) from e
        except RadarCatalogNotReady as e:
            raise src.domain.exceptions.RadarSourceUnavailable(str(e)) from e
        except RadarSelectionError as e:
            raise src.domain.exceptions.RadarSourceMisconfigured(str(e)) from e
        except (ValidationError, ValueRadarError) as e:
            raise src.domain.exceptions.RadarDataInvalid(
                f"Некорректные данные от WCF: {e}"
            ) from e

    async def _read_packet_in_executor(self) -> TelemetryPacket:
        self._in_flight = self._executor.submit(self._read_packet_sync)
        try:
            return await asyncio.wrap_future(self._in_flight)
        finally:
            if self._in_flight.done():
                self._in_flight = None

    async def close(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)
        self._client.close()
