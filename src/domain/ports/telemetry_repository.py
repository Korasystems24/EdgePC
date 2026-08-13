from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

from src.domain.entities.telemetry_packet import TelemetryPacket


@dataclass(frozen=True)
class StoredPacket:
    """Пакет из таблицы плюс его id, чтобы потом пометить именно эту строку."""

    id: int
    packet: TelemetryPacket


class TelemetryRepository(ABC):
    @abstractmethod
    async def save(self, packet: TelemetryPacket) -> None:
        """Записать новый пакет."""

    @abstractmethod
    async def get_pending(self, limit: int) -> list[StoredPacket]:
        """Взять пачку ещё не отправленных (не уехавших и не в карантине)."""

    @abstractmethod
    async def set_sent(self, ids: list[int]) -> None:
        """Пометить: сервер принял."""

    @abstractmethod
    async def update_rejected(self, ids: list[int], quarantine_after: int) -> None:
        """Сервер отверг: +1 попытка, после порога убрать в карантин."""

    @abstractmethod
    async def delete_sent_older_than(self, cutoff: datetime) -> int:
        """Стереть уже отправленные старше даты. Неотправленные не трогать."""

    @abstractmethod
    async def get_pending_count(self) -> int:
        """Сколько ещё ждёт отправки."""

    @abstractmethod
    async def get_quarantined_count(self) -> int:
        """Сколько лежит в карантине."""
