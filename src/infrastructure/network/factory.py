from src.config.settings import Settings
from src.domain.enum import NetworkGatewayType
from src.domain.exceptions import DispatchMisconfigured
from src.domain.ports.network_gateway import NetworkGateway
from src.infrastructure.network.http_gateway import HttpGateway
from src.infrastructure.network.mock_gateway import MockNetworkGateway


def create_network_gateway(settings: Settings) -> NetworkGateway:
    if settings.network_gateway == NetworkGatewayType.MOCK:
        return MockNetworkGateway(
            delay=settings.mock.delay,
            fail=settings.mock.fail_rate,
        )

    if settings.network_gateway == NetworkGatewayType.HTTP:
        if not settings.dispatch.server_url:
            raise DispatchMisconfigured("DISPATCH__SERVER_URL не задан")
        return HttpGateway(
            settings=settings.dispatch,
            bunker_id=settings.bunker_id,
        )

    raise DispatchMisconfigured(
        f"Неподдерживаемый сетевой шлюз: {settings.network_gateway}"
    )
