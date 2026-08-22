from typing import Any
from urllib.parse import urlparse, urlunparse

import requests
from zeep import Transport

_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1"})
_HTTP_SCHEMES = frozenset({"http", "https"})

# zeep.Transport без аннотаций: basedpyright выводит частично Unknown.
_transport_cls: Any = Transport


def rewrite_loopback_url(url: str, alias: str) -> str:
    """Подменить host в URL, если это loopback. Путь, порт и query не трогаем."""
    parsed = urlparse(url)
    if parsed.scheme not in _HTTP_SCHEMES:
        return url
    if parsed.hostname not in _LOOPBACK_HOSTS:
        return url
    try:
        port = parsed.port
    except ValueError:
        return url
    netloc = alias if port is None else f"{alias}:{port}"
    return urlunparse(parsed._replace(netloc=netloc))


class LoopbackRewriteTransport(Transport):
    """zeep.Transport: localhost/127.0.0.1 из WSDL идут на alias за контейнером.

    post_xml вызывает self.post, отдельно его перехватывать не нужно.
    """

    def __init__(
        self,
        session: requests.Session,
        timeout: int,
        operation_timeout: int,
        loopback_alias: str | None = None,
    ) -> None:
        _transport_cls.__init__(
            self,
            session=session,
            timeout=timeout,
            operation_timeout=operation_timeout,
        )
        self._loopback_alias = loopback_alias

    def load(self, url: str) -> bytes:
        content = _transport_cls.load(self, self._rewrite(url))
        if not isinstance(content, bytes):
            raise TypeError("zeep.Transport.load вернул не bytes")
        return content

    def post(
        self,
        address: str,
        message: object,
        headers: object,
    ) -> requests.Response:
        response = _transport_cls.post(
            self,
            self._rewrite(address),
            message,
            headers,
        )
        if not isinstance(response, requests.Response):
            raise TypeError("zeep.Transport.post вернул не Response")
        return response

    def _rewrite(self, url: str) -> str:
        if not self._loopback_alias:
            return url
        return rewrite_loopback_url(url, self._loopback_alias)
