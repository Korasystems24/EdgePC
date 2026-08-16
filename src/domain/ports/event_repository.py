from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

from src.domain.entities.edge_event import EdgeEvent


@dataclass(frozen=True)
class StoredEvent:
    """Событие из outbox плюс id строки, чтобы пометить именно её."""

    id: int
    event: EdgeEvent
    created_at: datetime


class EventRepository(ABC):
    @abstractmethod
    async def save(self, event: EdgeEvent) -> None:
        """Записать новое событие. sent_at остаётся NULL."""

    @abstractmethod
    async def get_pending(self, limit: int) -> list[StoredEvent]:
        """Пачка ещё не отправленных, в порядке вставки."""

    @abstractmethod
    async def set_sent(self, ids: list[int]) -> None:
        """Пометить: сервер принял."""
