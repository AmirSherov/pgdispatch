from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import asyncpg
import pytest

from pgdispatch import PayloadTooLargeError, ValidationError, Webhooks
from tests.conftest import Receiver, WebhooksFactory, wait_until


async def count(pool: asyncpg.Pool, schema: str, table: str) -> int:
    value: int = await pool.fetchval(f'SELECT count(*) FROM "{schema}".{table}')
    return value


async def test_rollback_leaves_no_trace(
    webhooks: Webhooks, receiver: Receiver, pool: asyncpg.Pool, schema: str
) -> None:
    await webhooks.endpoints.create(receiver.url())

    async with pool.acquire() as connection:
        transaction = connection.transaction()
        await transaction.start()
        result = await webhooks.publish(connection, "order.created", {"id": 1})
        assert result.deliveries == 1
        await transaction.rollback()

    assert await count(pool, schema, "events") == 0
    assert await count(pool, schema, "deliveries") == 0
    async with webhooks.running_worker():
        await asyncio.sleep(0.5)
    assert receiver.requests == []


async def test_commit_wakes_the_worker_immediately(
    make_webhooks: WebhooksFactory, receiver: Receiver, pool: asyncpg.Pool
) -> None:
    # A long polling interval proves the wake-up comes from NOTIFY.
    webhooks = await make_webhooks(poll_interval=30)
    await webhooks.endpoints.create(receiver.url())

    async with webhooks.running_worker():
        await asyncio.sleep(0.3)  # let the worker go to sleep
        async with pool.acquire() as connection, connection.transaction():
            await webhooks.publish(connection, "order.created", {"id": 1})
        await wait_until(lambda: len(receiver.requests) == 1, timeout=3)


async def test_idempotency_key_deduplicates(
    webhooks: Webhooks, receiver: Receiver, pool: asyncpg.Pool, schema: str
) -> None:
    await webhooks.endpoints.create(receiver.url())
    first = await webhooks.publish(None, "order.created", {"id": 1}, idempotency_key="order:1")
    second = await webhooks.publish(None, "order.created", {"id": 1}, idempotency_key="order:1")

    assert not first.duplicate
    assert second.duplicate
    assert second.event_id == first.event_id
    assert second.deliveries == 0
    assert await count(pool, schema, "deliveries") == 1


async def test_payload_types_are_serialized(webhooks: Webhooks) -> None:
    moment = datetime(2026, 11, 1, 12, 30, tzinfo=timezone.utc)
    identifier = uuid.uuid4()
    result = await webhooks.publish(
        None, "order.created", {"at": moment, "id": identifier, "total": Decimal("10.50")}
    )
    event = await webhooks.events.get(result.event_id)
    assert event.payload == {"at": moment.isoformat(), "id": str(identifier), "total": "10.50"}


@pytest.mark.parametrize(
    ("event_type", "payload", "error"),
    [
        ("bad type!", {}, ValidationError),
        ("", {}, ValidationError),
        ("order.created", {"x": object()}, ValidationError),
        ("order.created", {"x": float("nan")}, ValidationError),
        ("order.created", {"x": "nul\x00"}, ValidationError),
        ("order.created", {"x": "a" * 300_000}, PayloadTooLargeError),
    ],
)
async def test_invalid_events_are_rejected(
    webhooks: Webhooks, event_type: str, payload: object, error: type[Exception]
) -> None:
    with pytest.raises(error):
        await webhooks.publish(None, event_type, payload)


async def test_ordering_key_delivers_strictly_in_order(
    make_webhooks: WebhooksFactory, receiver: Receiver
) -> None:
    webhooks = await make_webhooks(concurrency=20, endpoint_concurrency=20)
    await webhooks.endpoints.create(receiver.url())
    for number in range(50):
        await webhooks.publish(None, "order.updated", {"n": number}, ordering_key="order:1")

    async with webhooks.running_worker():
        await wait_until(lambda: len(receiver.requests) == 50, timeout=20)

    assert [r.json["data"]["n"] for r in receiver.requests] == list(range(50))


async def test_failed_head_blocks_only_its_ordering_key(
    make_webhooks: WebhooksFactory, receiver: Receiver
) -> None:
    from aiohttp import web

    webhooks = await make_webhooks(retry_schedule=(0, 60))
    receiver.responder = lambda request: web.Response(
        status=500 if request.json["data"] == {"key": "a", "n": 0} else 200
    )
    await webhooks.endpoints.create(receiver.url())
    for number in range(3):
        await webhooks.publish(None, "tick", {"key": "a", "n": number}, ordering_key="a")
        await webhooks.publish(None, "tick", {"key": "b", "n": number}, ordering_key="b")

    async with webhooks.running_worker():
        await wait_until(lambda: len(receiver.requests) == 4)
        await asyncio.sleep(0.5)

    received = [r.json["data"] for r in receiver.requests]
    assert [d["n"] for d in received if d["key"] == "b"] == [0, 1, 2]
    assert [d["n"] for d in received if d["key"] == "a"] == [0]
