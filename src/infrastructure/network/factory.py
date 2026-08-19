from src.config.enums import NetworkGatewayType
from src.config.settings import DispatchSettings, MockIOSettings
from src.domain.exceptions import DispatchMisconfigured
from src.domain.ports.network_gateway import NetworkGateway
from src.infrastructure.network.http_gateway import HttpGateway
from src.infrastructure.network.mock_gateway import MockNetworkGateway


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
        if not dispatch.http.server_url:
            raise DispatchMisconfigured("DISPATCH__HTTP__SERVER_URL не задан")
        return HttpGateway(settings=dispatch.http, bunker_id=bunker_id)

    raise DispatchMisconfigured(f"Неподдерживаемый сетевой шлюз: {kind}")
