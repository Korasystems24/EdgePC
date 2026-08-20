from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import inspect
from sqlalchemy.engine import Connection

_ALEMBIC_INI = Path(__file__).resolve().parents[3] / "alembic.ini"

# Таблицы, которые create_all уже мог создать до появления Alembic.
_LEGACY_TABLES = frozenset({"event_outbox", "telemetry_packet"})


def upgrade_schema(connection: Connection) -> None:
    """Довести схему до head. БД без alembic_version, но с таблицами, штампуем."""
    cfg = Config(str(_ALEMBIC_INI))
    cfg.attributes["connection"] = connection

    tables = set(inspect(connection).get_table_names())
    if "alembic_version" not in tables and tables & _LEGACY_TABLES:
        command.stamp(cfg, "head")
        return

    command.upgrade(cfg, "head")
