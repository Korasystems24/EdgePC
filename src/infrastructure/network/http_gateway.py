from http import HTTPStatus
from typing import Any

import httpx
from loguru import logger
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    retry_if_exception_type,
    stop_after_attempt,
    stop_after_delay,
    wait_random_exponential,
)

from src.config.settings import HttpTransportSettings
from src.domain.entities.edge_event import EdgeEvent
from src.domain.entities.health_snapshot import HealthSnapshot
from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.exceptions import (
    DispatchMisconfigured,
    DispatchRejected,
    NetworkUnavailable,
)
from src.domain.ports.network_gateway import NetworkGateway

TELEMETRY_PATH = "telemetry"
EVENTS_PATH = "events"
HEALTH_PATH = "health"

MAX_ERROR_BODY_CHARS = 300

# HTTPStatus, а не httpx.codes: члены httpx объявлены парами (код, фраза), и для
# статических проверок сравнение int с таким членом выглядит всегда ложным.
RETRYABLE_STATUSES = frozenset({HTTPStatus.TOO_MANY_REQUESTS})
MISCONFIGURED_STATUSES = frozenset(
    {
        HTTPStatus.BAD_REQUEST,
        HTTPStatus.UNAUTHORIZED,
        HTTPStatus.FORBIDDEN,
    }
)


class HttpGateway(NetworkGateway):
    def __init__(
        self,
        settings: HttpTransportSettings,
        bunker_id: str,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._settings = settings
        self._bunker_id = bunker_id
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(
                connect=settings.connect_timeout,
                read=settings.read_timeout,
                write=settings.write_timeout,
                pool=settings.pool_timeout,
            )
        )

    async def send_telemetry(self, packets: list[TelemetryPacket]) -> None:
        await self._post_with_retry(TELEMETRY_PATH, self._telemetry_payload(packets))

    async def send_telemetry_once(self, packets: list[TelemetryPacket]) -> None:
        """Без ретраев: изоляция 422 шлёт пакеты по одному и должна уложиться в
        тот же дедлайн тика, что и обычная пачка."""
        await self._post_json(TELEMETRY_PATH, self._telemetry_payload(packets))

    async def send_events(self, events: list[EdgeEvent]) -> None:
        await self._post_with_retry(
            EVENTS_PATH,
            {
                "bunker_id": self._bunker_id,
                "events": [event.model_dump(mode="json") for event in events],
            },
        )

    async def send_health(self, snapshot: HealthSnapshot) -> None:
        await self._post_json(
            HEALTH_PATH,
            {
                "bunker_id": self._bunker_id,
                **snapshot.model_dump(mode="json"),
            },
        )

    async def close(self) -> None:
        await self._client.aclose()

    def _telemetry_payload(self, packets: list[TelemetryPacket]) -> dict[str, Any]:
        return {
            "bunker_id": self._bunker_id,
            "packets": [packet.model_dump(mode="json") for packet in packets],
        }

    def _url(self, path: str) -> str:
        return f"{self._settings.server_url.rstrip('/')}/{path.lstrip('/')}"

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._settings.auth_token.get_secret_value()}",
        }

    async def _post_with_retry(self, path: str, payload: dict[str, Any]) -> None:
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(self._settings.retry_attempts)
            | stop_after_delay(self._settings.retrying_send_budget),
            wait=wait_random_exponential(
                multiplier=1,
                max=self._settings.retry_wait_max,
            ),
            retry=retry_if_exception_type(NetworkUnavailable),
            before_sleep=self._log_retry,
            reraise=True,
        ):
            with attempt:
                await self._post_json(path, payload)

    async def _post_json(self, path: str, payload: dict[str, Any]) -> None:
        try:
            response = await self._client.post(
                self._url(path),
                json=payload,
                headers=self._headers(),
            )
        except (httpx.InvalidURL, httpx.UnsupportedProtocol) as exc:
            raise DispatchMisconfigured(f"{type(exc).__name__}: {exc}") from exc
        except httpx.RequestError as exc:
            raise NetworkUnavailable(f"{type(exc).__name__}: {exc}") from exc

        self._raise_from_status(response)

    def _raise_from_status(self, response: httpx.Response) -> None:
        """Три класса ошибок сервера: повторить позже, данные плохие, нужен
        человек. Всё, что не разобрано явно, — последний класс: молча считать
        неизвестный ответ успехом опаснее, чем остановить отправку."""
        if response.is_success:
            return

        status = response.status_code
        body = response.text[:MAX_ERROR_BODY_CHARS]

        if status == HTTPStatus.UNPROCESSABLE_ENTITY:
            logger.error("Сервер отверг данные (HTTP 422): {}", body)
            raise DispatchRejected(f"HTTP {status}: {body}")

        if status == HTTPStatus.REQUEST_ENTITY_TOO_LARGE:
            logger.error(
                "Приёмник считает тело запроса слишком большим (HTTP 413): {}. "
                "Повтор тем же размером не поможет: уменьшить "
                "DISPATCH__BATCH_SIZE или поднять лимит тела на прокси",
                body,
            )
            raise DispatchMisconfigured(f"HTTP {status}: {body}")

        if status in MISCONFIGURED_STATUSES:
            logger.error(
                "Конфигурация отправки отвергнута (HTTP {}): {}",
                status,
                body,
            )
            raise DispatchMisconfigured(f"HTTP {status}: {body}")

        if status in RETRYABLE_STATUSES or response.is_server_error:
            raise NetworkUnavailable(f"HTTP {status}: {body}")

        logger.error("Неожиданный HTTP {}: {}", status, body)
        raise DispatchMisconfigured(f"HTTP {status}: {body}")

    def _log_retry(self, retry_state: RetryCallState) -> None:
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        logger.debug(
            "Сеть моргнула, повтор {}/{}: {}",
            retry_state.attempt_number,
            self._settings.retry_attempts,
            exc,
        )
