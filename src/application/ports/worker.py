from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class FatalError:
    reason: str
    restartable: bool


class Worker(Protocol):
    name: str

    async def start(self) -> None: ...
    def stop(self) -> None: ...

    @property
    def fatal_error(self) -> FatalError | None: ...

    @property
    def last_heartbeat(self) -> float: ...

    def is_alive(self, max_silence: float | None = None) -> bool: ...
