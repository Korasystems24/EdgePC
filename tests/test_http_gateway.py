from collections.abc import Callable
from datetime import UTC, datetime

import httpx
from pydantic import SecretStr

from src.config.settings import DispatchSettings
from src.domain.entities.edge_event import EdgeEvent
from src.domain.entities.health_snapshot import HealthSnapshot
from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.exceptions import NetworkUnavailable
from src.domain.value_objects.beam import Beam
from src.domain.value_objects.worker_health import WorkerHealth
from src.infrastructure.network.http_gateway import HttpGateway


def _settings() -> DispatchSettings:
    return DispatchSettings(
        server_url="https://telemetry.example.com",
        auth_token=SecretStr("secret-token"),
        retry_attempts=3,
        retry_wait_max=0.01,
    )


def _packet() -> TelemetryPacket:
    beams = [Beam(height=1.0, raw_status=0, status=()) for _ in range(64)]
    return TelemetryPacket(
        ts=datetime.now(tz=UTC),
        avg_level=1.0,
        mass=1.0,
        volume=1.0,
        beams=beams,
    )


def _snapshot() -> HealthSnapshot:
    return HealthSnapshot(
        ts=datetime(2026, 8, 16, 9, 29, tzinfo=UTC),
        workers={"ingestion": WorkerHealth(alive=True)},
        pending_count=3,
        quarantined_count=0,
        free_bytes=1,
        storage_paused=False,
        cpu_percent=1.0,
        memory_percent=2.0,
        disk_percent=3.0,
    )


def _gateway(handler: Callable[[httpx.Request], httpx.Response]) -> HttpGateway:
    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    return HttpGateway(_settings(), "bunker-001", client=client)


async def test_send_telemetry_posts_telemetry_path() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200)

    gateway = _gateway(handler)
    try:
        await gateway.send_telemetry([_packet()])
    finally:
        await gateway.close()

    assert seen[0].url.path == "/telemetry"
    assert seen[0].headers["Authorization"] == "Bearer secret-token"


async def test_send_events_posts_events_path_with_type() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200)

    event = EdgeEvent.radar_down(reason="WCF timeout")
    gateway = _gateway(handler)
    try:
        await gateway.send_events([event])
    finally:
        await gateway.close()

    assert seen[0].url.path == "/events"
    body = seen[0].content
    assert b"radar_down" in body
    assert b"bunker-001" in body


async def test_send_health_posts_once_even_on_server_error() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        assert request.url.path == "/health"
        return httpx.Response(500)

    gateway = _gateway(handler)
    try:
        try:
            await gateway.send_health(_snapshot())
        except NetworkUnavailable:
            pass
        else:
            raise AssertionError("ожидали NetworkUnavailable")
    finally:
        await gateway.close()

    assert calls == 1


async def test_send_events_retries_transient_errors() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503)

    gateway = _gateway(handler)
    try:
        try:
            await gateway.send_events([EdgeEvent.worker_restarted(worker="ingestion")])
        except NetworkUnavailable:
            pass
        else:
            raise AssertionError("ожидали NetworkUnavailable")
    finally:
        await gateway.close()

    assert calls == 3


async def test_isolation_send_does_not_retry_transient_errors() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503)

    gateway = _gateway(handler)
    try:
        try:
            await gateway.send_telemetry_once([_packet()])
        except NetworkUnavailable:
            pass
        else:
            raise AssertionError("ожидали NetworkUnavailable")
    finally:
        await gateway.close()

    assert calls == 1
