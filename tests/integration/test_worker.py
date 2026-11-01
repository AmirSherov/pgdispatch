from __future__ import annotations

import asyncio
from collections import Counter

import asyncpg
import pytest
from aiohttp import web

from pgdispatch import DeliveryStatus, Webhooks
from tests.conftest import Receiver, WebhooksFactory, wait_until


async def test_graceful_shutdown_returns_unfinished_deliveries(
    make_webhooks: WebhooksFactory, receiver: Receiver
) -> None:
    webhooks = await make_webhooks(shutdown_timeout=0.3)

    async def hang(_: object) -> web.Response:
        await asyncio.sleep(30)
        return web.Response()

    receiver.responder = hang  # type: ignore[assignment]
    await webhooks.endpoints.create(receiver.url())
    result = await webhooks.publish(None, "tick", {})

    async with webhooks.running_worker():
        await wait_until(lambda: len(receiver.requests) == 1)

    [delivery] = await webhooks.deliveries.list(event_id=result.event_id)
    assert delivery.status is DeliveryStatus.PENDING
    assert delivery.attempt_count == 0
    assert await webhooks.deliveries.attempts(delivery.id) == []


async def test_expired_lease_is_picked_up_again(
    webhooks: Webhooks, receiver: Receiver, pool: asyncpg.Pool, schema: str
) -> None:
    await webhooks.endpoints.create(receiver.url())
    result = await webhooks.publish(None, "tick", {})
    # Simulate a worker that claimed the delivery and died.
    await pool.execute(
        f"""UPDATE "{schema}".deliveries
            SET status = 'in_flight', attempt_count = 1,
                locked_until = now() - interval '1 second'"""
    )

    async with webhooks.running_worker():
        await wait_until(lambda: len(receiver.requests) == 1)
        [delivery] = await webhooks.deliveries.list(event_id=result.event_id)
        await wait_until(lambda: _succeeded(webhooks, delivery.id))

    [delivery] = await webhooks.deliveries.list(event_id=result.event_id)
    assert delivery.attempt_count == 2


async def test_active_lease_is_respected(
    webhooks: Webhooks, receiver: Receiver, pool: asyncpg.Pool, schema: str
) -> None:
    await webhooks.endpoints.create(receiver.url())
    await webhooks.publish(None, "tick", {})
    await pool.execute(
        f"""UPDATE "{schema}".deliveries
            SET status = 'in_flight', locked_until = now() + interval '1 minute'"""
    )
    async with webhooks.running_worker():
        await asyncio.sleep(0.5)
    assert receiver.requests == []


async def test_concurrent_workers_deliver_each_event_once(
    make_webhooks: WebhooksFactory, receiver: Receiver
) -> None:
    webhooks = await make_webhooks(batch_size=20, concurrency=20, endpoint_concurrency=20)
    for path in ("/a", "/b"):
        await webhooks.endpoints.create(receiver.url(path))
    for number in range(300):
        await webhooks.publish(None, "tick", {"n": number})

    async with webhooks.running_worker(), webhooks.running_worker(), webhooks.running_worker():
        await wait_until(lambda: len(receiver.requests) >= 600, timeout=30)
        await asyncio.sleep(0.3)

    seen = Counter((r.path, r.headers["webhook-id"]) for r in receiver.requests)
    assert len(seen) == 600
    assert set(seen.values()) == {1}


async def test_worker_blocks_private_addresses(
    make_webhooks: WebhooksFactory, receiver: Receiver
) -> None:
    permissive = await make_webhooks()
    strict = await make_webhooks(allow_private_ips=False)
    await permissive.endpoints.create(receiver.url())
    result = await strict.publish(None, "tick", {})

    async with strict.running_worker():
        [delivery] = await strict.deliveries.list(event_id=result.event_id)
        await wait_until(lambda: _attempted(strict, delivery.id))

    [attempt, *_] = await strict.deliveries.attempts(delivery.id)
    assert attempt.response_status is None
    assert attempt.error is not None
    assert attempt.error.startswith("blocked:")
    assert receiver.requests == []


async def test_timeout_is_recorded(make_webhooks: WebhooksFactory, receiver: Receiver) -> None:
    webhooks = await make_webhooks(request_timeout=0.3, retry_schedule=(0,))

    async def slow(_: object) -> web.Response:
        await asyncio.sleep(2)
        return web.Response()

    receiver.responder = slow  # type: ignore[assignment]
    await webhooks.endpoints.create(receiver.url())
    result = await webhooks.publish(None, "tick", {})

    async with webhooks.running_worker():
        [delivery] = await webhooks.deliveries.list(event_id=result.event_id)
        await wait_until(lambda: _attempted(webhooks, delivery.id))

    [attempt] = await webhooks.deliveries.attempts(delivery.id)
    assert attempt.error == "timed out after 0.3s"
    assert (await webhooks.deliveries.get(delivery.id)).status is DeliveryStatus.FAILED


async def test_unreachable_endpoint_is_a_network_error(
    make_webhooks: WebhooksFactory,
) -> None:
    webhooks = await make_webhooks(retry_schedule=(0,))
    await webhooks.endpoints.create("http://127.0.0.1:9/")  # discard port: nothing listens
    result = await webhooks.publish(None, "tick", {})

    async with webhooks.running_worker():
        [delivery] = await webhooks.deliveries.list(event_id=result.event_id)
        await wait_until(lambda: _attempted(webhooks, delivery.id))

    [attempt] = await webhooks.deliveries.attempts(delivery.id)
    assert attempt.response_status is None
    assert attempt.error and attempt.error.startswith("ClientConnectorError")


async def test_worker_registers_and_deregisters(
    webhooks: Webhooks, pool: asyncpg.Pool, schema: str
) -> None:
    async with webhooks.running_worker() as worker:
        await wait_until(lambda: worker.running)
        await wait_until(lambda: _workers(pool, schema, 1))
        assert (await webhooks.status()).active_workers == 1
    assert await _workers(pool, schema, 0)


async def test_lifespan_runs_worker(webhooks: Webhooks, receiver: Receiver) -> None:
    await webhooks.endpoints.create(receiver.url())
    async with webhooks.lifespan(run_worker=True, migrate=True)(object()):
        await webhooks.publish(None, "tick", {})
        await wait_until(lambda: len(receiver.requests) == 1)


async def _succeeded(webhooks: Webhooks, delivery_id: int) -> bool:
    return (await webhooks.deliveries.get(delivery_id)).status is DeliveryStatus.SUCCEEDED


async def _attempted(webhooks: Webhooks, delivery_id: int) -> bool:
    return bool(await webhooks.deliveries.attempts(delivery_id))


async def _workers(pool: asyncpg.Pool, schema: str, expected: int) -> bool:
    count: int = await pool.fetchval(f'SELECT count(*) FROM "{schema}".workers')
    return count == expected


async def test_worker_purges_expired_events(
    make_webhooks: WebhooksFactory, pool: asyncpg.Pool, schema: str
) -> None:
    webhooks = await make_webhooks(cleanup_interval=0.5, retention_days=7)
    await webhooks.endpoints.create("https://example.com/hooks", enabled=False)
    old = await webhooks.publish(None, "tick", {"old": True})
    fresh = await webhooks.publish(None, "tick", {"old": False})
    await pool.execute(
        f"UPDATE \"{schema}\".events SET created_at = now() - interval '8 days' WHERE id = $1",
        old.event_id,
    )

    async with webhooks.running_worker():
        await wait_until(lambda: _event_count(pool, schema, 1), timeout=5)

    assert [event.id for event in await webhooks.events.list()] == [fresh.event_id]


async def test_metrics_are_exported(make_webhooks: WebhooksFactory, receiver: Receiver) -> None:
    from prometheus_client import CollectorRegistry

    registry = CollectorRegistry()
    webhooks = await make_webhooks()
    webhooks = Webhooks(webhooks.backend, webhooks.config, metrics_registry=registry)
    await webhooks.endpoints.create(receiver.url())
    await webhooks.publish(None, "order.created", {})

    async with webhooks.running_worker():
        await wait_until(lambda: len(receiver.requests) == 1)
        await wait_until(
            lambda: (
                registry.get_sample_value(
                    "pgdispatch_delivery_attempts_total", {"result": "success"}
                )
                == 1
            )
        )

    assert (
        registry.get_sample_value(
            "pgdispatch_events_published_total", {"event_type": "order.created"}
        )
        == 1
    )
    assert registry.get_sample_value("pgdispatch_deliveries_pending") is not None


async def test_failing_hook_does_not_break_delivery(
    make_webhooks: WebhooksFactory, receiver: Receiver, caplog: pytest.LogCaptureFixture
) -> None:
    webhooks = await make_webhooks(retry_schedule=(0,))
    calls = []

    @webhooks.hooks.on_delivery_failed
    async def explode(delivery: object) -> None:
        calls.append(delivery)
        raise RuntimeError("hook bug")

    receiver.responder = lambda _: web.Response(status=500)
    await webhooks.endpoints.create(receiver.url())
    await webhooks.publish(None, "tick", {})
    await webhooks.publish(None, "tick", {})

    async with webhooks.running_worker():
        await wait_until(lambda: len(calls) == 2)

    assert "hook bug" in caplog.text or "raised an exception" in caplog.text


async def _event_count(pool: asyncpg.Pool, schema: str, expected: int) -> bool:
    count: int = await pool.fetchval(f'SELECT count(*) FROM "{schema}".events')
    return count == expected


async def test_stop_before_the_worker_starts(webhooks: Webhooks) -> None:
    async with webhooks.running_worker():
        pass  # stop() runs before the task had a chance to start


async def test_worker_fails_fast_when_the_database_is_unreachable() -> None:
    from pgdispatch import Worker

    worker = Worker.from_dsn("postgresql://nobody:wrong@127.0.0.1:1/none")
    with pytest.raises(OSError):
        await worker.run()
    assert not worker.running
