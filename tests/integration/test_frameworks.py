from __future__ import annotations

import io
import sys
import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from pgdispatch import Webhooks
from pgdispatch.cli import main as cli
from pgdispatch.sqlalchemy import SQLAlchemyBackend, asyncpg_dsn, run_migrations
from tests.conftest import Receiver, make_config, wait_until

DRIVERS = ["asyncpg"] + ([] if sys.platform == "win32" else ["psycopg"])


@pytest.fixture(params=DRIVERS)
async def engine(
    request: pytest.FixtureRequest, dsn: str, schema: str
) -> AsyncIterator[AsyncEngine]:
    url = dsn.replace("postgresql://", f"postgresql+{request.param}://", 1)
    engine = create_async_engine(url)
    async with engine.begin() as connection:
        await connection.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        await connection.execute(
            text(f'CREATE TABLE "{schema}".orders (id integer PRIMARY KEY, status text)')
        )
    yield engine
    await engine.dispose()


async def test_sqlalchemy_publish_joins_the_session_transaction(
    engine: AsyncEngine, schema: str, receiver: Receiver
) -> None:
    webhooks = Webhooks(SQLAlchemyBackend(engine), make_config(schema))
    await webhooks.migrate()
    await webhooks.endpoints.create(receiver.url(), event_types=["order.*"])
    sessions = async_sessionmaker(engine, expire_on_commit=False)

    insert = text(f'INSERT INTO "{schema}".orders (id, status) VALUES (:id, :status)')

    async with sessions() as session:
        with pytest.raises(RuntimeError):
            async with session.begin():
                await session.execute(insert, {"id": 1, "status": "paid"})
                await webhooks.publish(session, "order.paid", {"id": 1})
                raise RuntimeError("rolled back")

        async with session.begin():
            await session.execute(insert, {"id": 2, "status": "paid"})
            result = await webhooks.publish(session, "order.paid", {"id": 2})
            assert result.deliveries == 1

    async with webhooks.running_worker():
        await wait_until(lambda: len(receiver.requests) == 1)
    assert receiver.requests[0].json["data"] == {"id": 2}
    assert len(await webhooks.events.list()) == 1


async def test_sqlalchemy_publish_rejects_foreign_objects(engine: AsyncEngine, schema: str) -> None:
    webhooks = Webhooks(SQLAlchemyBackend(engine), make_config(schema))
    with pytest.raises(TypeError, match="AsyncSession"):
        await webhooks.publish(object(), "x", {})


async def test_alembic_style_migrations(engine: AsyncEngine, schema: str) -> None:
    async with engine.begin() as connection:
        applied = await connection.run_sync(lambda sync: run_migrations(sync, schema=schema))
        again = await connection.run_sync(lambda sync: run_migrations(sync, schema=schema))
    assert applied == [1]
    assert again == []


def test_asyncpg_dsn_conversion() -> None:
    from sqlalchemy.engine import make_url

    url = make_url(
        "postgresql+asyncpg://u:p%40ss@db:5433/app?ssl=require&prepared_statement_cache_size=0"
    )
    assert asyncpg_dsn(url) == "postgresql://u:p%40ss@db:5433/app?sslmode=require"


async def test_fastapi_router_scopes_everything_to_the_owner(
    webhooks: Webhooks, receiver: Receiver
) -> None:
    from fastapi import FastAPI, Header

    from pgdispatch.fastapi import router

    async def owner(x_merchant: str = Header()) -> str:
        return x_merchant

    app = FastAPI()
    app.include_router(router(webhooks, owner=owner), prefix="/webhooks")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        alice = {"X-Merchant": "alice"}
        response = await client.post(
            "/webhooks/endpoints",
            json={"url": receiver.url(), "event_types": ["order.*"]},
            headers=alice,
        )
        assert response.status_code == 201, response.text
        created = response.json()
        assert created["secret"].startswith("whsec_")
        endpoint_id = created["id"]

        assert len((await client.get("/webhooks/endpoints", headers=alice)).json()) == 1
        bob = {"X-Merchant": "bob"}
        assert (await client.get("/webhooks/endpoints", headers=bob)).json() == []
        assert (
            await client.get(f"/webhooks/endpoints/{endpoint_id}", headers=bob)
        ).status_code == 404

        response = await client.patch(
            f"/webhooks/endpoints/{endpoint_id}", json={"description": "shop"}, headers=alice
        )
        assert response.json()["description"] == "shop"
        assert response.json()["event_types"] == ["order.*"]

        bad = await client.post("/webhooks/endpoints", json={"url": "ftp://x"}, headers=alice)
        assert bad.status_code == 422

        secret = (
            await client.get(f"/webhooks/endpoints/{endpoint_id}/secret", headers=alice)
        ).json()
        assert secret["secret"] == created["secret"]
        rotated = await client.post(
            f"/webhooks/endpoints/{endpoint_id}/rotate-secret", headers=alice
        )
        assert rotated.json()["secret"] != created["secret"]

        test = await client.post(f"/webhooks/endpoints/{endpoint_id}/test", headers=alice)
        assert test.status_code == 202
        async with webhooks.running_worker():
            await wait_until(lambda: len(receiver.requests) == 1)

        [delivery] = (await client.get("/webhooks/deliveries", headers=alice)).json()
        assert (await client.get("/webhooks/deliveries", headers=bob)).json() == []
        detail = (await client.get(f"/webhooks/deliveries/{delivery['id']}", headers=alice)).json()
        assert detail["attempts"][0]["response_status"] == 200
        retried = await client.post(f"/webhooks/deliveries/{delivery['id']}/retry", headers=alice)
        assert retried.json()["status"] == "pending"
        missing = await client.post("/webhooks/deliveries/999999/retry", headers=alice)
        assert missing.status_code == 404

        disabled = await client.post(f"/webhooks/endpoints/{endpoint_id}/disable", headers=alice)
        assert disabled.json()["is_enabled"] is False
        conflict = await client.post(f"/webhooks/endpoints/{endpoint_id}/test", headers=alice)
        assert conflict.status_code == 409
        enabled = await client.post(f"/webhooks/endpoints/{endpoint_id}/enable", headers=alice)
        assert enabled.json()["is_enabled"] is True

        deleted = await client.delete(f"/webhooks/endpoints/{endpoint_id}", headers=alice)
        assert deleted.status_code == 204


def run_cli(*args: str) -> tuple[int, str]:
    out = io.StringIO()
    original = sys.stdout
    sys.stdout = out
    try:
        code = cli(list(args))
    finally:
        sys.stdout = original
    return code, out.getvalue()


async def test_cli_commands(webhooks: Webhooks, dsn: str, schema: str) -> None:
    import asyncio

    endpoint = await webhooks.endpoints.create("https://example.com/hooks", owner_id="shop")
    event = await webhooks.publish(None, "order.created", {"id": 1}, owner_id="shop")
    common = ["--dsn", dsn, "--schema", schema, "--log-level", "error"]

    def call(*args: str) -> tuple[int, str]:
        return run_cli(*common, *args)

    async def in_thread(*args: str) -> tuple[int, str]:
        return await asyncio.to_thread(call, *args)

    code, output = await in_thread("migrate")
    assert code == 0 and "up to date" in output
    code, output = await in_thread("status")
    assert code == 0 and "pending" in output
    code, output = await in_thread("endpoints", "list", "--owner", "shop")
    assert str(endpoint.id) in output
    code, output = await in_thread("deliveries", "--status", "pending")
    assert "order.created" in output
    [delivery] = await webhooks.deliveries.list()
    code, output = await in_thread("deliveries", "show", str(delivery.id), "--json")
    assert code == 0 and '"attempts": []' in output
    code, output = await in_thread("replay", "--event", str(event.event_id))
    assert "Created 1 deliveries" in output
    code, output = await in_thread("retry", str(delivery.id))
    assert code == 0
    code, output = await in_thread("retry", "--endpoint", str(endpoint.id))
    assert "Scheduled 0 deliveries" in output
    code, output = await in_thread("endpoints", "disable", str(endpoint.id), "--reason", "test")
    assert code == 0
    code, output = await in_thread("endpoints", "enable", str(endpoint.id))
    assert "enabled" in output
    code, output = await in_thread("cleanup", "--older-than", "1d")
    assert "Deleted 0 events" in output
    code, _ = await in_thread("endpoints", "enable", str(uuid.uuid4()))
    assert code == 1
