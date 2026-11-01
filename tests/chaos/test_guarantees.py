"""The guarantees from the README, proven with real worker processes.

These tests start ``pgdispatch worker`` as separate OS processes and kill them
without warning (``SIGKILL`` / ``TerminateProcess``), exactly like an OOM
killer or a crashed node would.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
from collections import Counter
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

import asyncpg
import pytest
from aiohttp import web

from pgdispatch import Webhooks
from tests.conftest import ReceivedRequest, Receiver, WebhooksFactory, wait_until

pytestmark = pytest.mark.slow

WORKER_ENV = {
    "PGDISPATCH_ALLOW_HTTP": "true",
    "PGDISPATCH_ALLOW_PRIVATE_IPS": "true",
    "PGDISPATCH_POLL_INTERVAL": "0.2",
    "PGDISPATCH_HEARTBEAT_INTERVAL": "0.5",
    "PGDISPATCH_REQUEST_TIMEOUT": "5",
    "PGDISPATCH_LEASE_DURATION": "6",
    "PGDISPATCH_RETRY_JITTER": "0",
}

StartWorker = Callable[..., subprocess.Popen[bytes]]


@pytest.fixture
def start_worker(dsn: str, schema: str) -> AsyncIterator[StartWorker]:
    processes: list[subprocess.Popen[bytes]] = []

    def start(**settings: str) -> subprocess.Popen[bytes]:
        env = {
            **os.environ,
            **WORKER_ENV,
            **{f"PGDISPATCH_{k.upper()}": v for k, v in settings.items()},
        }
        command = [sys.executable, "-m", "pgdispatch", "worker", "--log-level", "warning"]
        process = subprocess.Popen(
            [*command, "--dsn", dsn, "--schema", schema], env=env, stderr=subprocess.DEVNULL
        )
        processes.append(process)
        return process

    yield start  # type: ignore[misc]
    for process in processes:
        process.kill()
        process.wait()


async def count_succeeded(pool: asyncpg.Pool, schema: str) -> int:
    value: int = await pool.fetchval(
        f"SELECT count(*) FROM \"{schema}\".deliveries WHERE status = 'succeeded'"
    )
    return value


@asynccontextmanager
async def slow_receiver(receiver: Receiver, delay: float) -> AsyncIterator[None]:
    async def respond(_: ReceivedRequest) -> web.Response:
        await asyncio.sleep(delay)
        return web.Response(text="ok")

    receiver.responder = respond
    yield
    receiver.responder = None


async def test_kill_9_with_1000_deliveries_in_flight_loses_nothing(
    make_webhooks: WebhooksFactory,
    receiver: Receiver,
    pool: asyncpg.Pool,
    schema: str,
    start_worker: StartWorker,
) -> None:
    webhooks = await make_webhooks()
    await webhooks.endpoints.create(receiver.url())
    async with pool.acquire() as connection, connection.transaction():
        for number in range(1000):
            await webhooks.publish(connection, "tick", {"n": number})

    async with slow_receiver(receiver, delay=2.0):
        victim = start_worker(concurrency="1000", endpoint_concurrency="1000", batch_size="500")
        await wait_until(lambda: len(receiver.requests) >= 1000, timeout=30)
        victim.kill()  # every delivery is in flight right now
        victim.wait()
    assert await count_succeeded(pool, schema) == 0

    start_worker(concurrency="200", endpoint_concurrency="200")
    await wait_until(lambda: _all_succeeded(pool, schema, 1000), timeout=60)

    delivered = {request.headers["webhook-id"] for request in receiver.requests}
    assert len(delivered) == 1000


async def test_four_workers_deliver_10000_events_exactly_once_each(
    make_webhooks: WebhooksFactory,
    receiver: Receiver,
    pool: asyncpg.Pool,
    schema: str,
    start_worker: StartWorker,
) -> None:
    total = int(os.environ.get("PGDISPATCH_CHAOS_EVENTS", "10000"))
    webhooks = await make_webhooks()
    await webhooks.endpoints.create(receiver.url())
    async with pool.acquire() as connection, connection.transaction():
        for number in range(total):
            await webhooks.publish(connection, "tick", {"n": number})

    for _ in range(4):
        start_worker(concurrency="100", endpoint_concurrency="100")
    await wait_until(lambda: _all_succeeded(pool, schema, total), timeout=180)

    counts = Counter(request.headers["webhook-id"] for request in receiver.requests)
    assert len(counts) == total
    assert max(counts.values()) == 1
    rows = await pool.fetch(f'SELECT status, count(*) FROM "{schema}".deliveries GROUP BY status')
    assert {row["status"]: row["count"] for row in rows} == {"succeeded": total}
    attempts = await pool.fetchval(f'SELECT count(*) FROM "{schema}".attempts')
    assert attempts == total


async def test_1000_events_with_one_ordering_key_arrive_in_order(
    make_webhooks: WebhooksFactory,
    receiver: Receiver,
    pool: asyncpg.Pool,
    schema: str,
    start_worker: StartWorker,
) -> None:
    webhooks = await make_webhooks()
    await webhooks.endpoints.create(receiver.url())
    async with pool.acquire() as connection, connection.transaction():
        for number in range(1000):
            await webhooks.publish(connection, "tick", {"n": number}, ordering_key="order:42")

    # Two competing worker processes must still respect the order.
    start_worker()
    start_worker()
    await wait_until(lambda: _all_succeeded(pool, schema, 1000), timeout=120)

    assert [request.json["data"]["n"] for request in receiver.requests] == list(range(1000))


async def test_endpoint_outage_opens_the_breaker_and_everything_arrives_after_recovery(
    make_webhooks: WebhooksFactory,
    receiver: Receiver,
    pool: asyncpg.Pool,
    schema: str,
    start_worker: StartWorker,
) -> None:
    # Time is compressed: a 5 s outage with second-scale retries stands in for
    # a 10 minute outage with the default schedule.
    webhooks: Webhooks = await make_webhooks()
    endpoint = await webhooks.endpoints.create(receiver.url())
    outage = True
    receiver.responder = lambda _: web.Response(status=503 if outage else 200)
    async with pool.acquire() as connection, connection.transaction():
        for number in range(300):
            await webhooks.publish(connection, "tick", {"n": number})

    start_worker(
        failure_threshold="5",
        circuit_cooldown="1",
        circuit_max_cooldown="2",
        retry_schedule="[0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]",
    )
    await wait_until(lambda: _circuit_open(pool, schema), timeout=10)
    failed_requests = len(receiver.requests)
    await asyncio.sleep(5)
    # While the breaker is open only occasional probes reach the endpoint.
    assert len(receiver.requests) - failed_requests < 20

    outage = False
    await wait_until(lambda: _all_succeeded(pool, schema, 300), timeout=60)
    state = await webhooks.endpoints.get(endpoint.id)
    assert state.circuit_state.value == "closed"
    succeeded = {r.headers["webhook-id"] for r in receiver.requests if r.received_at}
    assert len(succeeded) == 300


async def _all_succeeded(pool: asyncpg.Pool, schema: str, expected: int) -> bool:
    return await count_succeeded(pool, schema) == expected


async def _circuit_open(pool: asyncpg.Pool, schema: str) -> bool:
    state = await pool.fetchval(f'SELECT circuit_state FROM "{schema}".endpoints')
    return bool(state != "closed")
