"""SQLAlchemy 2.0 (async) integration.

::

    from pgdispatch import Webhooks
    from pgdispatch.sqlalchemy import SQLAlchemyBackend

    webhooks = Webhooks(SQLAlchemyBackend(engine))

    async with session.begin():
        session.add(order)
        await webhooks.publish(session, "order.created", {"id": order.id})

Any async PostgreSQL driver works for publishing (``asyncpg``, ``psycopg``).
The worker always uses asyncpg with connection settings derived from the
engine URL; pass ``worker_dsn`` to override them.

Alembic users add the pgdispatch schema to their own migration chain::

    from alembic import op
    from pgdispatch.sqlalchemy import run_migrations

    def upgrade() -> None:
        run_migrations(op.get_bind())
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from functools import lru_cache
from typing import Any, cast

import asyncpg

try:
    from sqlalchemy import Connection, CursorResult, TextClause, text
    from sqlalchemy.engine import URL
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession
except ImportError as exc:  # pragma: no cover - depends on the environment
    msg = 'pgdispatch.sqlalchemy requires SQLAlchemy: pip install "pgdispatch[sqlalchemy]"'
    raise ImportError(msg) from exc

from pgdispatch.backends.asyncpg import WORKER_POOL_SIZE
from pgdispatch.backends.base import Row, run_sync
from pgdispatch.migrations import migrate

__all__ = ["SQLAlchemyBackend", "SQLAlchemyExecutor", "run_migrations"]


@lru_cache(maxsize=512)
def _compiled(sql: str) -> TextClause:
    return text(sql)


class SQLAlchemyExecutor:
    """Runs statements on an ``AsyncSession`` or ``AsyncConnection``, inside its transaction."""

    def __init__(self, connection: AsyncSession | AsyncConnection) -> None:
        self._connection = connection

    async def fetch(self, sql: str, params: Mapping[str, Any]) -> list[Row]:
        result = cast(
            "CursorResult[Any]", await self._connection.execute(_compiled(sql), dict(params))
        )
        if not result.returns_rows:
            return []
        return [dict(row) for row in result.mappings()]


class _SyncExecutor:
    def __init__(self, connection: Connection) -> None:
        self._connection = connection

    def fetch(self, sql: str, params: Mapping[str, Any]) -> list[Row]:
        result = cast("CursorResult[Any]", self._connection.execute(_compiled(sql), dict(params)))
        if not result.returns_rows:
            return []
        return [dict(row) for row in result.mappings()]


class SQLAlchemyBackend:
    """Use pgdispatch with an SQLAlchemy ``AsyncEngine``."""

    def __init__(self, engine: AsyncEngine, *, worker_dsn: str | None = None) -> None:
        self.engine = engine
        self._worker_dsn = worker_dsn or asyncpg_dsn(engine.url)

    def executor(self, connection: Any) -> SQLAlchemyExecutor:
        if not isinstance(connection, (AsyncSession, AsyncConnection)):
            msg = (
                "SQLAlchemyBackend.publish() expects an AsyncSession or AsyncConnection, "
                f"got {type(connection).__name__}"
            )
            raise TypeError(msg)
        return SQLAlchemyExecutor(connection)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[SQLAlchemyExecutor]:
        async with self.engine.begin() as connection:
            yield SQLAlchemyExecutor(connection)

    async def create_worker_pool(self) -> asyncpg.Pool:
        pool: asyncpg.Pool = await asyncpg.create_pool(
            self._worker_dsn, min_size=1, max_size=WORKER_POOL_SIZE
        )
        return pool

    async def close(self) -> None:
        """The engine belongs to the application, so there is nothing to close."""


def run_migrations(connection: Connection, *, schema: str = "pgdispatch") -> list[int]:
    """Apply pgdispatch migrations on a synchronous connection.

    In an Alembic revision pass ``op.get_bind()``.
    """
    return run_sync(migrate(), _SyncExecutor(connection), schema=schema)


def asyncpg_dsn(url: URL) -> str:
    """Turn an SQLAlchemy URL into a DSN that asyncpg understands.

    Driver-specific query parameters are dropped; ``sslmode``/``ssl`` are kept.
    """
    query = {key: value for key, value in url.query.items() if key in ("ssl", "sslmode")}
    if "ssl" in query and "sslmode" not in query:
        query["sslmode"] = query.pop("ssl")
    plain = url.set(drivername="postgresql", query=query)
    return plain.render_as_string(hide_password=False)
