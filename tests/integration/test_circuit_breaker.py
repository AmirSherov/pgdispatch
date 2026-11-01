from __future__ import annotations

import asyncio
from datetime import timedelta

import asyncpg
from aiohttp import web

from pgdispatch import CircuitState, DeliveryStatus, Endpoint
from tests.conftest import Receiver, WebhooksFactory, wait_until


async def test_breaker_opens_probes_and_recovers(
    make_webhooks: WebhooksFactory, receiver: Receiver
) -> None:
    webhooks = await make_webhooks(
        failure_threshold=3,
        circuit_cooldown=timedelta(seconds=1),
        retry_schedule=(timedelta(0),) + (timedelta(milliseconds=50),) * 20,
        concurrency=1,
    )
    opened: list[Endpoint] = []
    webhooks.hooks.on_circuit_opened(opened.append)
    healthy = False
    receiver.responder = lambda _: web.Response(status=200 if healthy else 503)
    endpoint = await webhooks.endpoints.create(receiver.url())
    for number in range(5):
        await webhooks.publish(None, "tick", {"n": number})

    async with webhooks.running_worker():
        await wait_until(lambda: len(opened) == 1)
        state = await webhooks.endpoints.get(endpoint.id)
        assert state.circuit_state is CircuitState.OPEN
        attempts_when_opened = len(receiver.requests)
        assert attempts_when_opened == 3

        # While open, nothing is sent.
        await asyncio.sleep(0.5)
        assert len(receiver.requests) == attempts_when_opened

        # After the cooldown a single probe goes out; it succeeds and closes the breaker.
        healthy = True
        await wait_until(
            lambda: _deliveries_with(webhooks, DeliveryStatus.SUCCEEDED, 5), timeout=10
        )

    state = await webhooks.endpoints.get(endpoint.id)
    assert state.circuit_state is CircuitState.CLOSED
    assert state.consecutive_failures == 0


async def test_failed_probe_reopens_with_longer_cooldown(
    make_webhooks: WebhooksFactory, receiver: Receiver, pool: asyncpg.Pool, schema: str
) -> None:
    webhooks = await make_webhooks(
        failure_threshold=1,
        circuit_cooldown=timedelta(milliseconds=300),
        retry_schedule=(timedelta(0),) + (timedelta(milliseconds=10),) * 20,
    )
    receiver.responder = lambda _: web.Response(status=500)
    endpoint = await webhooks.endpoints.create(receiver.url())
    await webhooks.publish(None, "tick", {})

    async with webhooks.running_worker():
        await wait_until(lambda: len(receiver.requests) >= 2)  # first failure + one probe
        await wait_until(lambda: _has_trips(pool, schema, endpoint, 2))

    row = await pool.fetchrow(
        f'SELECT circuit_opened_at, circuit_retry_at FROM "{schema}".endpoints WHERE id = $1',
        endpoint.id,
    )
    assert row["circuit_retry_at"] - row["circuit_opened_at"] >= timedelta(milliseconds=600)


async def test_endpoint_is_disabled_after_failing_for_too_long(
    make_webhooks: WebhooksFactory, receiver: Receiver, pool: asyncpg.Pool, schema: str
) -> None:
    webhooks = await make_webhooks(
        auto_disable_after=timedelta(days=5),
        retry_schedule=(timedelta(0), timedelta(milliseconds=10)),
    )
    reasons: list[str] = []
    webhooks.hooks.on_endpoint_disabled(lambda _, reason: reasons.append(reason))
    receiver.responder = lambda _: web.Response(status=500)
    endpoint = await webhooks.endpoints.create(receiver.url())
    await pool.execute(
        f'UPDATE "{schema}".endpoints '
        "SET failing_since = now() - interval '6 days' WHERE id = $1",
        endpoint.id,
    )
    await webhooks.publish(None, "tick", {})

    async with webhooks.running_worker():
        await wait_until(lambda: len(reasons) == 1)

    state = await webhooks.endpoints.get(endpoint.id)
    assert not state.is_enabled
    assert "failing continuously" in (state.disabled_reason or "")

    # Enabling resets the breaker.
    state = await webhooks.endpoints.enable(endpoint.id)
    assert state.is_enabled
    assert state.failing_since is None
    assert state.consecutive_failures == 0


async def test_slow_endpoint_does_not_starve_others(
    make_webhooks: WebhooksFactory, receiver: Receiver
) -> None:
    webhooks = await make_webhooks(concurrency=10, endpoint_concurrency=2)

    async def respond(request: object) -> web.Response:
        if getattr(request, "path", "") == "/slow":
            await asyncio.sleep(2)
        return web.Response(text="ok")

    receiver.responder = respond  # type: ignore[assignment]
    await webhooks.endpoints.create(receiver.url("/slow"), event_types=["slow"])
    await webhooks.endpoints.create(receiver.url("/fast"), event_types=["fast"])
    for _ in range(20):
        await webhooks.publish(None, "slow", {})
    for _ in range(20):
        await webhooks.publish(None, "fast", {})

    async with webhooks.running_worker():
        await wait_until(lambda: len(receiver.for_path("/fast")) == 20, timeout=3)
        assert len(receiver.for_path("/slow")) <= 4


async def _deliveries_with(webhooks: object, status: DeliveryStatus, count: int) -> bool:
    deliveries = await webhooks.deliveries.list(status=status)  # type: ignore[attr-defined]
    return len(deliveries) == count


async def _has_trips(pool: asyncpg.Pool, schema: str, endpoint: Endpoint, trips: int) -> bool:
    value: int = await pool.fetchval(
        f'SELECT circuit_trips FROM "{schema}".endpoints WHERE id = $1', endpoint.id
    )
    return value >= trips
