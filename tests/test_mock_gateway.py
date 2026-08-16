from datetime import UTC, datetime

import pytest

from src.domain.entities.edge_event import EdgeEvent
from src.domain.entities.health_snapshot import HealthSnapshot
from src.domain.value_objects.worker_health import WorkerHealth
from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.exceptions import NetworkUnavailable
from src.domain.value_objects.beam import Beam
from src.infrastructure.network.mock_gateway import MockNetworkGateway


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
        pending_count=0,
        quarantined_count=0,
        free_bytes=1,
        storage_paused=False,
        cpu_percent=1.0,
        memory_percent=2.0,
        disk_percent=3.0,
    )


async def test_mock_gateway_fail_rate_applies_to_all_methods() -> None:
    gateway = MockNetworkGateway(delay=0, fail=1.0)
    with pytest.raises(NetworkUnavailable):
        await gateway.send_telemetry([_packet()])
    with pytest.raises(NetworkUnavailable):
        await gateway.send_events([EdgeEvent.radar_down(reason="down")])
    with pytest.raises(NetworkUnavailable):
        await gateway.send_health(_snapshot())


async def test_mock_gateway_success_path() -> None:
    gateway = MockNetworkGateway(delay=0, fail=0.0)
    await gateway.send_telemetry([_packet()])
    await gateway.send_events([EdgeEvent.worker_restarted(worker="dispatch")])
    await gateway.send_health(_snapshot())
