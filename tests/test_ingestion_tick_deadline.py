import asyncio
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime

from src.application.ingestion.service import IngestionService
from src.application.ingestion.worker import IngestionWorker
from src.domain.entities.telemetry_packet import TelemetryPacket
from src.domain.exceptions import RadarSourceHung
from src.domain.ports.radar_adapter import RadarAdapter
from src.domain.ports.storage_monitor import StorageMonitor
from src.domain.ports.telemetry_repository import StoredPacket, TelemetryRepository
from src.infrastructure.radar.mock_adapter import MockRadarAdapter


class _HungRadarAdapter(RadarAdapter):
    def __init__(self) -> None:
        self.calls = 0
        self._release = threading.Event()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="test-hung")
        self._in_flight: Future[TelemetryPacket] | None = None

    def raise_if_hung(self) -> None:
        if self._in_flight is not None and not self._in_flight.done():
            raise RadarSourceHung("источник не вернул управление после дедлайна тика")

    def _block(self) -> TelemetryPacket:
        self.calls += 1
        self._release.wait(timeout=60)
        raise AssertionError("блокирующий вызов не должен завершиться в этом тесте")

    async def read_packet(self) -> TelemetryPacket:
        self._in_flight = self._executor.submit(self._block)
        try:
            return await asyncio.wrap_future(self._in_flight)
        finally:
            if self._in_flight.done():
                self._in_flight = None

    async def close(self) -> None:
        self._release.set()
        self._executor.shutdown(wait=True, cancel_futures=True)


class _FakeStorage(StorageMonitor):
    def free_bytes(self) -> int:
        return 10**12


class _FakeRepository(TelemetryRepository):
    async def save(self, packet: TelemetryPacket) -> None:
        return None

    async def get_pending(self, limit: int) -> list[StoredPacket]:
        return []

    async def set_sent(self, ids: list[int]) -> None:
        return None

    async def update_rejected(self, ids: list[int], quarantine_after: int) -> None:
        return None

    async def delete_sent_older_than(self, cutoff: datetime) -> int:
        return 0

    async def get_pending_count(self) -> int:
        return 0

    async def get_quarantined_count(self) -> int:
        return 0


async def test_hung_blocking_read_sets_restartable_fatal_and_does_not_call_twice() -> None:
    adapter = _HungRadarAdapter()
    worker = IngestionWorker(
        name="ingestion",
        service=IngestionService(radar_adapter=adapter, repository=_FakeRepository()),
        storage_monitor=_FakeStorage(),
        interval=0.05,
        tick_deadline=0.2,
        max_consecutive_failures=10,
    )
    task = asyncio.create_task(worker.start())
    try:
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline and worker.fatal_error is None:
            await asyncio.sleep(0.05)

        assert worker.fatal_error is not None
        assert worker.fatal_error.restartable is True
        assert adapter.calls == 1
    finally:
        worker.stop()
        await asyncio.wait_for(task, timeout=1.0)
        await adapter.close()


class _CountingMockRadarAdapter(RadarAdapter):
    def __init__(self) -> None:
        self.calls = 0
        self._inner = MockRadarAdapter(delay=10, fail=0.0)

    async def read_packet(self) -> TelemetryPacket:
        self.calls += 1
        return await self._inner.read_packet()


async def test_cooperative_async_timeout_is_ordinary_failure_not_fatal() -> None:
    adapter = _CountingMockRadarAdapter()
    worker = IngestionWorker(
        name="ingestion",
        service=IngestionService(radar_adapter=adapter, repository=_FakeRepository()),
        storage_monitor=_FakeStorage(),
        interval=0.05,
        tick_deadline=0.15,
        max_consecutive_failures=10,
    )
    task = asyncio.create_task(worker.start())
    try:
        await asyncio.sleep(0.5)
        assert worker.fatal_error is None
        assert adapter.calls >= 2
        assert worker.is_alive()
    finally:
        worker.stop()
        await asyncio.wait_for(task, timeout=1.0)
