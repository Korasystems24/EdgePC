from src.config.enums import NetworkGatewayType
from src.config.settings import DispatchSettings, MockIOSettings
from src.domain.exceptions import DispatchMisconfigured
from src.domain.ports.network_gateway import NetworkGateway
from src.infrastructure.network.http_gateway import HttpGateway
from src.infrastructure.network.mock_gateway import MockNetworkGateway

SERVER_URL_SCHEMES = ("http://", "https://")


def create_network_gateway(
    kind: NetworkGatewayType,
    *,
    dispatch: DispatchSettings,
    mock: MockIOSettings,
    bunker_id: str,
) -> NetworkGateway:
    if kind == NetworkGatewayType.MOCK:
        return MockNetworkGateway(delay=mock.delay, fail=mock.fail_rate)

    if kind == NetworkGatewayType.HTTP:
        _check_server_url(dispatch.http.server_url)
        return HttpGateway(settings=dispatch.http, bunker_id=bunker_id)

    raise DispatchMisconfigured(f"Неподдерживаемый сетевой шлюз: {kind}")


def _check_server_url(url: str) -> None:
    """Адрес без схемы httpx отвергает как UnsupportedProtocol, а это подкласс
    ошибок транспорта: опечатка выглядела бы как вечное отсутствие сети."""
    if not url:
        raise DispatchMisconfigured("DISPATCH__HTTP__SERVER_URL не задан")
    if not url.startswith(SERVER_URL_SCHEMES):
        raise DispatchMisconfigured(
            f"DISPATCH__HTTP__SERVER_URL должен начинаться с http:// или https://, "
            f"получено: {url!r}"
        )
