from __future__ import annotations

import asyncio
import sys
from logging.config import fileConfig
from pathlib import Path
from typing import Protocol

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection, make_url
from sqlalchemy.ext.asyncio import async_engine_from_config

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.config.settings import get_settings  # noqa: E402
from src.infrastructure.database.models import Base  # noqa: E402
from src.infrastructure.database.types import UTCDateTime  # noqa: E402

config = context.config
target_metadata = Base.metadata


class _AutogenContext(Protocol):
    imports: set[str]


def _database_url() -> str:
    overrides = context.get_x_argument(as_dictionary=True)
    return overrides.get("url") or get_settings().db.url


def _apply_database_url() -> None:
    # ConfigParser считает % интерполяцией, поэтому в URL его нужно удвоить.
    config.set_main_option("sqlalchemy.url", _database_url().replace("%", "%%"))


def _is_sqlite(url: str | None) -> bool:
    return url is not None and "sqlite" in url.split(":", 1)[0]


def _ensure_db_dir(url: str) -> None:
    if ":memory:" in url:
        return
    database = make_url(url).database
    if not database:
        return
    Path(database).parent.mkdir(parents=True, exist_ok=True)


def render_item(
    type_: str, obj: object, autogen_context: _AutogenContext
) -> str | bool:
    """Печатать UTCDateTime как импорт, а не как src.infrastructure..."""
    if type_ != "type" or not isinstance(obj, UTCDateTime):
        return False
    autogen_context.imports.add(
        "from src.infrastructure.database.types import UTCDateTime"
    )
    return "UTCDateTime()"


_apply_database_url()

# fileConfig перезаписывает уже настроенный loguru, поэтому внутри приложения
# (когда соединение передано через Config.attributes) его не вызываем.
if config.attributes.get("connection") is None and config.config_file_name is not None:
    fileConfig(config.config_file_name)


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        render_as_batch=_is_sqlite(url),
        compare_type=True,
        render_item=render_item,
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        render_as_batch=_is_sqlite(url),
        compare_type=True,
        render_item=render_item,
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    url = config.get_main_option("sqlalchemy.url")
    if url is None:
        raise RuntimeError("sqlalchemy.url не задан")
    _ensure_db_dir(url)

    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    connection = config.attributes.get("connection")
    if connection is None:
        asyncio.run(run_async_migrations())
        return
    do_run_migrations(connection)


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
