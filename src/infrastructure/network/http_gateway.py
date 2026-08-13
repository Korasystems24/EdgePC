from typing import Any

import httpx
from loguru import logger
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from src.config.settings import DispatchSettings
from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.exceptions import (
    DispatchMisconfigured,
    DispatchRejected,
    NetworkUnavailable,
)
from src.domain.ports.network_gateway import NetworkGateway


class HttpGateway(NetworkGateway):
    def __init__(self, settings: DispatchSettings, bunker_id: str) -> None:
        self._settings = settings
        self._bunker_id = bunker_id
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=settings.connect_timeout,
                read=settings.read_timeout,
                write=settings.write_timeout,
                pool=settings.pool_timeout,
            )
        )

    async def send_batch(self, packets: list[TelemetryPacket]) -> None:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self._settings.retry_attempts),
            wait=wait_random_exponential(
                multiplier=1,
                max=self._settings.retry_wait_max,
            ),
            retry=retry_if_exception_type(NetworkUnavailable),
            before_sleep=self._log_retry,
            reraise=True,
        ):
            with attempt:
                await self._post_once(packets)

    async def close(self) -> None:
        await self._client.aclose()

    async def _post_once(self, packets: list[TelemetryPacket]) -> None:
        payload: dict[str, str | list[dict[str, Any]]] = {
            "bunker_id": self._bunker_id,
            "packets": [packet.model_dump(mode="json") for packet in packets],
        }
        headers = {
            "Authorization": (f"Bearer {self._settings.auth_token.get_secret_value()}"),
        }

        try:
            response = await self._client.post(
                self._settings.server_url,
                json=payload,
                headers=headers,
            )
        except httpx.RequestError as exc:
            raise NetworkUnavailable(f"{type(exc).__name__}: {exc}") from exc
        except httpx.InvalidURL as exc:
            raise DispatchMisconfigured(f"{type(exc).__name__}: {exc}") from exc

        self._raise_from_status(response)

    def _raise_from_status(self, response: httpx.Response) -> None:
        status = response.status_code
        body = response.text[:300]

        if status == 200:
            return

        if status == 422:
            logger.error("Сервер отверг данные (HTTP 422): {}", body)
            raise DispatchRejected(f"HTTP 422: {body}")

        if status in (400, 401, 403):
            logger.error(
                "Конфигурация отправки отвергнута (HTTP {}): {}",
                status,
                body,
            )
            raise DispatchMisconfigured(f"HTTP {status}: {body}")

        if status == 429 or status >= 500:
            raise NetworkUnavailable(f"HTTP {status}: {body}")

        logger.error("Неожиданный HTTP {}: {}", status, body)
        raise DispatchMisconfigured(f"HTTP {status}: {body}")

    def _log_retry(self, retry_state: RetryCallState) -> None:
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        logger.warning(
            "Сеть моргнула, повтор {}/{}: {}",
            retry_state.attempt_number,
            self._settings.retry_attempts,
            exc,
        )
