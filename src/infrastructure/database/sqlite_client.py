from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from sqlite3 import Connection as SQLite3Connection
from typing import TYPE_CHECKING

from sqlalchemy import event, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import (
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import ConnectionPoolEntry, StaticPool

from src.domain.exceptions import StorageError
from src.infrastructure.database.base_client import DatabaseClient
from src.infrastructure.database.models import Base

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

# Две записи SQLite всё равно сериализует, поэтому второй транзакции нужно дать
# дождаться писателя вместо немедленного "database is locked".
SQLITE_BUSY_TIMEOUT_MS = 5000

# Durability дороже скорости: один INSERT в 5 секунд позволяет синхронизировать
# WAL на каждом commit.
SQLITE_SYNCHRONOUS = "FULL"


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
        self._session_lock = asyncio.Lock()
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
        if not database:
            return
        try:
            Path(database).parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise StorageError(
                f"Не удалось создать каталог БД {Path(database).parent}: {e}"
            ) from e

    def _register_per_connection_pragmas(self) -> None:
        event.listen(self.engine.sync_engine, "connect", self._apply_sqlite_pragmas)

    @staticmethod
    def _apply_sqlite_pragmas(
        dbapi_connection: SQLite3Connection,
        _connection_record: ConnectionPoolEntry,
    ) -> None:
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(f"PRAGMA synchronous={SQLITE_SYNCHRONOUS};")
            cursor.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS};")
            cursor.execute("PRAGMA foreign_keys=ON;")
        finally:
            cursor.close()

    async def init_database(self) -> None:
        try:
            async with self.engine.begin() as conn:
                await conn.execute(text("PRAGMA journal_mode=WAL;"))
                await conn.run_sync(Base.metadata.create_all)
        except SQLAlchemyError as e:
            raise StorageError(f"Не удалось инициализировать SQLite: {e}") from e

    @asynccontextmanager
    async def session(self) -> AsyncGenerator[AsyncSession, None]:
        async with self._session_lock, self.session_factory() as session:
            yield session

    async def close(self) -> None:
        await self.engine.dispose()
