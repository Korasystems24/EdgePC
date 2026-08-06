from abc import ABC, abstractmethod
from contextlib import AbstractAsyncContextManager

from sqlalchemy.ext.asyncio import AsyncSession


class DatabaseClient(ABC):
    @abstractmethod
    async def init_database(self) -> None: ...

    @abstractmethod
    def session(self) -> AbstractAsyncContextManager[AsyncSession]: ...

    @abstractmethod
    async def close(self) -> None: ...
