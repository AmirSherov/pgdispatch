from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from urllib.parse import urlsplit

import pytest

from tests.conftest import DSN_ENV

SCHEMA = f"pgd_django_{uuid.uuid4().hex[:8]}"


def pytest_configure() -> None:
    dsn = os.environ.get(DSN_ENV)
    if not dsn:
        return
    from django.conf import settings

    import django

    url = urlsplit(dsn)
    settings.configure(
        DATABASES={
            "default": {
                "ENGINE": "django.db.backends.postgresql",
                "NAME": url.path.lstrip("/"),
                "USER": url.username or "",
                "PASSWORD": url.password or "",
                "HOST": url.hostname or "",
                "PORT": str(url.port or ""),
            }
        },
        INSTALLED_APPS=["pgdispatch.django"],
        USE_TZ=True,
        PGDISPATCH={
            "db_schema": SCHEMA,
            "allow_http": True,
            "allow_private_ips": True,
            "poll_interval": 0.1,
            "heartbeat_interval": 0.2,
            "request_timeout": 5,
            "lease_duration": 10,
            "shutdown_timeout": 2,
        },
    )
    django.setup()


@pytest.fixture(scope="module")
def migrated() -> Iterator[None]:
    if not os.environ.get(DSN_ENV):
        pytest.skip(f"set {DSN_ENV} to run tests against PostgreSQL")
    from django.core.management import call_command
    from django.db import NotSupportedError, connection

    try:
        connection.ensure_connection()
    except NotSupportedError as exc:
        # Recent Django releases drop old PostgreSQL versions; pgdispatch itself
        # supports them, so only the Django tests are skipped.
        pytest.skip(str(exc))
    call_command("migrate", "pgdispatch", verbosity=0)
    yield
    call_command("migrate", "pgdispatch", "zero", verbosity=0)
