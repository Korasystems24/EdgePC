from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import event, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

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
        is_memory = ":memory:" in url
        self._ensure_db_dir_exists(url, is_memory)

        if is_memory:
            self.engine = create_async_engine(
                url=url,
                echo=echo,
                echo_pool=echo_pool,
                poolclass=StaticPool,
                connect_args={"check_same_thread": False},
            )
        else:
            self.engine = create_async_engine(
                url=url,
                echo=echo,
                echo_pool=echo_pool,
            )

        self._register_per_connection_pragmas()
        self.session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
            bind=self.engine,
            autoflush=False,
            autocommit=False,
            expire_on_commit=False,
        )

    @staticmethod
    def _ensure_db_dir_exists(url: str, is_memory: bool) -> None:
        if is_memory:
            return
        database = make_url(url).database
        if database:
            Path(database).parent.mkdir(parents=True, exist_ok=True)

    def _register_per_connection_pragmas(self) -> None:

        from sqlite3 import Connection as SQLite3Connection

        from sqlalchemy.pool import ConnectionPoolEntry

        @event.listens_for(self.engine.sync_engine, "connect")
        def set_sqlite_pragma(
            dbapi_connection: SQLite3Connection,
            connection_record: ConnectionPoolEntry,
        ) -> None:
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute("PRAGMA synchronous=FULL;")
                cursor.execute("PRAGMA busy_timeout=5000;")
                cursor.execute("PRAGMA foreign_keys=ON;")
            finally:
                cursor.close()

    async def init_database(self) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(text("PRAGMA journal_mode=WAL;"))
            await conn.run_sync(Base.metadata.create_all)

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        async with self.session_factory() as session:
            yield session

    async def close(self) -> None:
        await self.engine.dispose()
