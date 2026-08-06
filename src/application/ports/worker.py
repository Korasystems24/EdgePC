from typing import Protocol


class Worker(Protocol):
    name: str

    async def start(self) -> None: ...

    def stop(self) -> None: ...

    def is_alive(self, max_silence: float = 30.0) -> bool: ...
