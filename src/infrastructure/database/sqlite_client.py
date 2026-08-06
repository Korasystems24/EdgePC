from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    async_sessionmaker,
    create_async_engine,
)

from src.infrastructure.database.base_client import DatabaseClient
from src.infrastructure.database.models import Base

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class SqliteClient(DatabaseClient):
    def __init__(
        self,
        url: str,
        echo: bool = False,
        echo_pool: bool = False,
    ):
        self._ensure_db_dir_exists(url)

        # pool_size/max_overflow не передаём: для SQLite они бессмысленны,
        # а с ":memory:" (StaticPool) вызывают TypeError.
        self.engine: AsyncEngine = create_async_engine(
            url=url,
            echo=echo,
            echo_pool=echo_pool,
        )
        self.session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )

    @staticmethod
    def _ensure_db_dir_exists(url: str) -> None:
        prefix = "sqlite+aiosqlite:///"
        if url.startswith(prefix) and ":memory:" not in url:
            db_path = Path(url.removeprefix(prefix))
            db_path.parent.mkdir(parents=True, exist_ok=True)

    async def init_database(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession]:
        async with self.session_factory() as session:
            yield session

    async def close(self) -> None:
        await self.engine.dispose()
