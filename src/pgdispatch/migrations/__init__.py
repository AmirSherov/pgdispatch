"""Versioned SQL migrations for the pgdispatch schema.

Migrations are plain SQL files named ``NNNN_description.sql``. They are applied
in order, each in the caller's transaction, and recorded in
``{schema}.schema_migrations``. A transaction-scoped advisory lock makes
concurrent ``migrate`` calls (several pods starting at once) safe.

The same migrator powers ``pgdispatch migrate``, ``Webhooks.migrate()``, the
Django app and the Alembic helper.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import cache
from importlib import resources

from pgdispatch.backends.base import Operation, Query

__all__ = ["Migration", "drop_schema", "load_migrations", "migrate"]

_FILE_NAME = re.compile(r"^(\d{4})_(\w+)\.sql$")
_STATEMENT_END = re.compile(r";[ \t]*$", re.MULTILINE)


@dataclass(frozen=True, slots=True)
class Migration:
    version: int
    name: str
    statements: tuple[str, ...]


@cache
def load_migrations() -> tuple[Migration, ...]:
    """All migrations shipped with the package, oldest first."""
    migrations = []
    for entry in resources.files(__package__).iterdir():
        match = _FILE_NAME.match(entry.name)
        if match is None:
            continue
        migrations.append(
            Migration(
                version=int(match.group(1)),
                name=match.group(2),
                statements=_split_statements(entry.read_text(encoding="utf-8")),
            )
        )
    return tuple(sorted(migrations, key=lambda migration: migration.version))


def migrate(*, target: int | None = None) -> Operation[list[int]]:
    """Apply pending migrations up to ``target`` (all by default); return the applied versions."""
    yield Query("SELECT pg_advisory_xact_lock(hashtext('{schema}.migrate'))")
    yield Query("CREATE SCHEMA IF NOT EXISTS {schema}")
    yield Query(
        """
        CREATE TABLE IF NOT EXISTS {schema}.schema_migrations (
            version    integer PRIMARY KEY,
            name       text NOT NULL,
            applied_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    rows = yield Query("SELECT version FROM {schema}.schema_migrations")
    applied = {row["version"] for row in rows}

    newly_applied = []
    for migration in load_migrations():
        if migration.version in applied or (target is not None and migration.version > target):
            continue
        for statement in migration.statements:
            yield Query(statement)
        yield Query(
            "INSERT INTO {schema}.schema_migrations (version, name) "
            "VALUES (CAST(:version AS integer), CAST(:name AS text))",
            {"version": migration.version, "name": migration.name},
        )
        newly_applied.append(migration.version)
    return newly_applied


def drop_schema() -> Operation[None]:
    """Remove the schema with all data. Used by ``migrate zero`` in Django and by tests."""
    yield Query("DROP SCHEMA IF EXISTS {schema} CASCADE")


def _split_statements(sql: str) -> tuple[str, ...]:
    without_comments = "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )
    parts = _STATEMENT_END.split(without_comments)
    return tuple(part.strip() for part in parts if part.strip())
