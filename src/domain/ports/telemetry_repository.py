from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

from src.domain.entities.telemetry_packet import TelemetryPacket


@dataclass(frozen=True)
class StoredPacket:
    id: int
    packet: TelemetryPacket


class TelemetryRepository(ABC):
    @abstractmethod
    async def save(self, packet: TelemetryPacket) -> None: ...

    @abstractmethod
    async def fetch_pending(self, limit: int) -> list[StoredPacket]: ...

    @abstractmethod
    async def mark_sent(self, ids: list[int]) -> None: ...

    @abstractmethod
    async def record_rejection(self, ids: list[int], quarantine_after: int) -> None: ...

    @abstractmethod
    async def purge_sent_older_than(self, cutoff: datetime) -> int: ...

    @abstractmethod
    async def count_pending(self) -> int: ...

    @abstractmethod
    async def count_quarantined(self) -> int: ...
