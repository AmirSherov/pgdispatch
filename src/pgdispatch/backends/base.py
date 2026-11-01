"""Driver-independent database access.

pgdispatch has to run the same SQL through asyncpg, SQLAlchemy (any async
driver) and Django (psycopg, synchronously). Instead of writing every operation
three times, operations are written once as *generators* that yield
:class:`Query` objects and receive the resulting rows::

    def get_endpoint(endpoint_id: UUID) -> Operation[Endpoint]:
        rows = yield Query("SELECT ... WHERE id = CAST(:id AS uuid)", {"id": str(endpoint_id)})
        return Endpoint.from_row(rows[0])

A small runner (:func:`run_async` or :func:`run_sync`) drives the generator
against an executor. Executors only know how to send one statement to their
driver; they never contain business logic.

SQL conventions:

* Parameters use the ``:name`` style and are always wrapped in ``CAST`` so that
  every driver binds them with the same PostgreSQL type.
* ``{schema}`` is replaced with the configured, quoted schema name.
* ``jsonb`` values are selected as ``text`` and decoded in Python.
"""

from __future__ import annotations

import re
from collections.abc import Generator, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

if TYPE_CHECKING:
    import asyncpg

__all__ = [
    "AsyncExecutor",
    "Backend",
    "Operation",
    "Query",
    "Row",
    "SyncExecutor",
    "run_async",
    "run_sync",
    "to_numeric_style",
    "to_pyformat_style",
]

T = TypeVar("T")
Row = dict[str, Any]


@dataclass(frozen=True, slots=True)
class Query:
    sql: str
    params: Mapping[str, Any] = field(default_factory=dict)


Operation = Generator[Query, list[Row], T]
"""A database operation: yields queries, receives their rows, returns a result."""


class AsyncExecutor(Protocol):
    async def fetch(self, sql: str, params: Mapping[str, Any]) -> list[Row]:
        """Execute one statement and return its rows (an empty list if it returns none)."""


class SyncExecutor(Protocol):
    def fetch(self, sql: str, params: Mapping[str, Any]) -> list[Row]:
        """Execute one statement and return its rows (an empty list if it returns none)."""


class Backend(Protocol):
    """Connects :class:`~pgdispatch.Webhooks` to the application's database layer."""

    def executor(self, connection: Any) -> AsyncExecutor:
        """Wrap a connection or session owned by the application (used by ``publish``)."""

    def transaction(self) -> AbstractAsyncContextManager[AsyncExecutor]:
        """Run management operations in a new transaction."""

    async def create_worker_pool(self) -> asyncpg.Pool:
        """Create the asyncpg pool used by the delivery worker."""

    async def close(self) -> None:
        """Release resources owned by the backend."""


async def run_async(operation: Operation[T], executor: AsyncExecutor, *, schema: str) -> T:
    """Drive ``operation`` to completion against an async executor."""
    try:
        query = next(operation)
        while True:
            rows = await executor.fetch(render_schema(query.sql, schema), query.params)
            query = operation.send(rows)
    except StopIteration as stop:
        result: T = stop.value
        return result


def run_sync(operation: Operation[T], executor: SyncExecutor, *, schema: str) -> T:
    """Drive ``operation`` to completion against a blocking executor."""
    try:
        query = next(operation)
        while True:
            rows = executor.fetch(render_schema(query.sql, schema), query.params)
            query = operation.send(rows)
    except StopIteration as stop:
        result: T = stop.value
        return result


# ``:name`` placeholders; ``::type`` casts are left alone.
_PARAMETER = re.compile(r"(?<![:\w]):(\w+)(?!:)")


@lru_cache(maxsize=512)
def render_schema(sql: str, schema: str) -> str:
    return sql.replace("{schema}", f'"{schema}"')


@lru_cache(maxsize=512)
def to_numeric_style(sql: str) -> tuple[str, tuple[str, ...]]:
    """Convert ``:name`` placeholders to asyncpg's ``$1`` style.

    Returns the new SQL and the parameter names in positional order.
    """
    names: list[str] = []

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in names:
            names.append(name)
        return f"${names.index(name) + 1}"

    return _PARAMETER.sub(replace, sql), tuple(names)


@lru_cache(maxsize=512)
def to_pyformat_style(sql: str) -> str:
    """Convert ``:name`` placeholders to the DB-API ``%(name)s`` style used by psycopg."""
    return _PARAMETER.sub(r"%(\1)s", sql.replace("%", "%%"))
