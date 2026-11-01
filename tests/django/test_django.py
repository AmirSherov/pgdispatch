from __future__ import annotations

import asyncio

import pytest
from aiohttp import web

pytestmark = pytest.mark.usefixtures("migrated")


def schema_exists(name: str) -> bool:
    from django.db import connection

    with connection.cursor() as cursor:
        cursor.execute("SELECT 1 FROM pg_namespace WHERE nspname = %s", [name])
        return cursor.fetchone() is not None


def test_migrate_creates_the_schema() -> None:
    from tests.django.conftest import SCHEMA

    assert schema_exists(SCHEMA)


def test_publish_follows_transaction_atomic() -> None:
    from django.db import transaction

    from pgdispatch.django import webhooks

    endpoint = webhooks.endpoints.create("http://127.0.0.1:1/hooks", owner_id="shop-rollback")
    with pytest.raises(RuntimeError), transaction.atomic():
        webhooks.publish("order.shipped", {"id": 1}, owner_id="shop-rollback")
        raise RuntimeError

    assert webhooks.events.list(owner_id="shop-rollback") == []
    with transaction.atomic():
        result = webhooks.publish("order.shipped", {"id": 2}, owner_id="shop-rollback")
    assert result.deliveries == 1
    [delivery] = webhooks.deliveries.list(endpoint_id=endpoint.id)
    assert delivery.event_id == result.event_id
    webhooks.endpoints.delete(endpoint.id)


def test_worker_delivers_django_events() -> None:
    from pgdispatch import verify_signature
    from pgdispatch.django import webhooks

    received: list[tuple[dict[str, str], bytes]] = []

    async def scenario() -> None:
        async def handle(request: web.Request) -> web.Response:
            received.append((dict(request.headers), await request.read()))
            return web.Response(text="ok")

        app = web.Application()
        app.router.add_post("/hooks", handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]

        endpoint = await asyncio.to_thread(
            webhooks.endpoints.create, f"http://127.0.0.1:{port}/hooks", owner_id="shop-worker"
        )
        await asyncio.to_thread(
            webhooks.publish, "order.shipped", {"id": 3}, owner_id="shop-worker"
        )
        worker = webhooks.worker()
        task = asyncio.create_task(worker.run())
        for _ in range(100):
            if received:
                break
            await asyncio.sleep(0.05)
        worker.stop()
        await task
        await runner.cleanup()
        assert endpoint.secret is not None
        headers, body = received[0]
        verify_signature(endpoint.secret, headers, body)

    asyncio.run(scenario())
    assert len(received) == 1


def test_sync_management_api() -> None:
    from pgdispatch import NotFoundError
    from pgdispatch.django import webhooks

    endpoint = webhooks.endpoints.create("https://example.com/hooks", owner_id="shop-api")
    assert webhooks.endpoints.get(endpoint.id).url == "https://example.com/hooks"
    assert webhooks.endpoints.get_secret(endpoint.id) == endpoint.secret
    rotated = webhooks.endpoints.rotate_secret(endpoint.id)
    assert rotated.secret != endpoint.secret
    assert webhooks.endpoints.update(endpoint.id, description="x").description == "x"
    assert not webhooks.endpoints.disable(endpoint.id).is_enabled
    assert webhooks.endpoints.enable(endpoint.id).is_enabled
    result = webhooks.endpoints.send_test(endpoint.id)
    [delivery] = webhooks.deliveries.list(event_id=result.event_id)
    assert webhooks.deliveries.cancel(delivery.id).status.value == "cancelled"
    assert webhooks.deliveries.retry(delivery.id).status.value == "pending"
    assert webhooks.deliveries.attempts(delivery.id) == []
    assert webhooks.deliveries.retry_all(endpoint_id=endpoint.id) == 0
    assert webhooks.events.get(result.event_id).event_type == "webhook.test"
    assert webhooks.events.replay(result.event_id, endpoint_id=endpoint.id)
    assert webhooks.status().pending >= 1
    webhooks.endpoints.delete(endpoint.id)
    with pytest.raises(NotFoundError):
        webhooks.endpoints.get(endpoint.id)


def test_asyncpg_connect_kwargs() -> None:
    from pgdispatch import ConfigurationError
    from pgdispatch.django.client import asyncpg_connect_kwargs

    kwargs = asyncpg_connect_kwargs(
        {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": "app",
            "USER": "u",
            "PASSWORD": "",
            "HOST": "db",
            "PORT": "5433",
            "OPTIONS": {"sslmode": "require"},
        }
    )
    assert kwargs == {"host": "db", "port": 5433, "user": "u", "database": "app", "ssl": "require"}
    with pytest.raises(ConfigurationError):
        asyncpg_connect_kwargs({"ENGINE": "django.db.backends.sqlite3"})
