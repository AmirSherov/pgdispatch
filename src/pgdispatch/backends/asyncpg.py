"""Backend for applications that talk to PostgreSQL with plain asyncpg."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any

import asyncpg
from asyncpg.pool import PoolConnectionProxy

from pgdispatch.backends.base import Row, to_numeric_style
from pgdispatch.errors import ConfigurationError

__all__ = ["AsyncpgBackend", "AsyncpgConnection", "AsyncpgExecutor", "normalize_dsn"]

AsyncpgConnection = asyncpg.Connection | PoolConnectionProxy
"""A plain connection or one acquired from a pool."""

WORKER_POOL_SIZE = 5
"""Connections used by a worker: LISTEN, claiming, recording results and housekeeping."""


class AsyncpgExecutor:
    """Runs statements on an asyncpg connection (or pool)."""

    def __init__(self, connection: AsyncpgConnection | asyncpg.Pool) -> None:
        self._connection = connection

    async def fetch(self, sql: str, params: Mapping[str, Any]) -> list[Row]:
        statement, names = to_numeric_style(sql)
        records = await self._connection.fetch(statement, *(params[name] for name in names))
        return [dict(record) for record in records]


class AsyncpgBackend:
    """Use pgdispatch with asyncpg.

    ``AsyncpgBackend("postgresql://...")`` creates and owns a pool. Pass an
    existing pool with ``AsyncpgBackend(pool=pool)`` to share yours; the worker
    always opens a small pool of its own because it keeps a connection busy
    with ``LISTEN``.
    """

    def __init__(
        self,
        dsn: str | None = None,
        *,
        pool: asyncpg.Pool | None = None,
        **connect_kwargs: Any,
    ) -> None:
        if dsn is None and pool is None and not connect_kwargs:
            msg = "pass a DSN, an asyncpg pool or asyncpg connection arguments"
            raise ValueError(msg)
        self._dsn = normalize_dsn(dsn) if dsn else None
        self._connect_kwargs = connect_kwargs
        self._pool = pool
        self._owns_pool = pool is None

    def executor(self, connection: Any) -> AsyncpgExecutor:
        if not isinstance(connection, (asyncpg.Connection, PoolConnectionProxy, asyncpg.Pool)):
            kind = type(connection).__name__
            msg = f"AsyncpgBackend.publish() expects an asyncpg connection, got {kind}"
            raise TypeError(msg)
        return AsyncpgExecutor(connection)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncpgExecutor]:
        pool = await self._get_pool()
        async with pool.acquire() as connection, connection.transaction():
            yield AsyncpgExecutor(connection)

    async def create_worker_pool(self) -> asyncpg.Pool:
        if self._dsn is None and not self._connect_kwargs:
            msg = (
                "the worker needs its own connections: pass the DSN as well, "
                "e.g. AsyncpgBackend(dsn, pool=pool)"
            )
            raise ConfigurationError(msg)
        kwargs = {"min_size": 1, "max_size": WORKER_POOL_SIZE, **self._connect_kwargs}
        pool: asyncpg.Pool = await asyncpg.create_pool(self._dsn, **kwargs)
        return pool

    async def close(self) -> None:
        if self._owns_pool and self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def _get_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self._dsn, **self._connect_kwargs)
        return self._pool


def normalize_dsn(dsn: str) -> str:
    """Accept SQLAlchemy-style URLs such as ``postgresql+asyncpg://`` as well."""
    scheme, separator, rest = dsn.partition("://")
    if not separator:
        return dsn
    base = scheme.split("+", 1)[0]
    if base == "postgres":
        base = "postgresql"
    return f"{base}://{rest}"
