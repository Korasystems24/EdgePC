from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from src.domain.value_objects.fatal_error import FatalError


@dataclass(frozen=True)
class TaskState:
    done: bool
    exception: BaseException | None


class Worker(Protocol):
    name: str

    async def start(self) -> None: ...
    def stop(self) -> None: ...

    @property
    def fatal_error(self) -> FatalError | None: ...

    @property
    def last_heartbeat(self) -> float: ...

    @property
    def last_error(self) -> str | None: ...

    @property
    def last_error_at(self) -> datetime | None: ...

    @property
    def storage_paused(self) -> bool: ...

    def is_alive(self, max_silence: float | None = None) -> bool: ...


type WorkerFactory = Callable[[], Worker]
"""Рестарт создаёт новый экземпляр воркера, а не запускает остановленный
повторно, поэтому менеджер хранит фабрики, а не сами объекты."""
