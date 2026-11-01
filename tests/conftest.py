from __future__ import annotations

import asyncio
import os
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import asyncpg
import pytest
from aiohttp import web

from pgdispatch import AsyncpgBackend, Webhooks, WebhooksConfig
from pgdispatch.backends.asyncpg import AsyncpgExecutor
from pgdispatch.backends.base import run_async
from pgdispatch.migrations import drop_schema

DSN_ENV = "PGDISPATCH_TEST_DSN"


def database_url() -> str:
    dsn = os.environ.get(DSN_ENV)
    if not dsn:
        pytest.skip(f"set {DSN_ENV} to run tests against PostgreSQL")
    return dsn


def make_config(schema: str, **overrides: Any) -> WebhooksConfig:
    settings: dict[str, Any] = {
        "db_schema": schema,
        "allow_http": True,
        "allow_private_ips": True,
        "poll_interval": timedelta(milliseconds=100),
        "heartbeat_interval": timedelta(milliseconds=200),
        "request_timeout": timedelta(seconds=5),
        "lease_duration": timedelta(seconds=10),
        "retry_jitter": 0.0,
        "shutdown_timeout": timedelta(seconds=5),
    }
    settings.update(overrides)
    return WebhooksConfig(**settings)


@pytest.fixture
def dsn() -> str:
    return database_url()


@pytest.fixture
async def schema(dsn: str) -> AsyncIterator[str]:
    name = f"pgd_test_{uuid.uuid4().hex[:10]}"
    yield name
    connection = await asyncpg.connect(dsn)
    try:
        await run_async(drop_schema(), AsyncpgExecutor(connection), schema=name)
    finally:
        await connection.close()


@pytest.fixture
async def pool(dsn: str) -> AsyncIterator[asyncpg.Pool]:
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4)
    yield pool
    await pool.close()


WebhooksFactory = Callable[..., Awaitable[Webhooks]]


@pytest.fixture
async def make_webhooks(dsn: str, schema: str) -> AsyncIterator[WebhooksFactory]:
    created: list[Webhooks] = []

    async def factory(**overrides: Any) -> Webhooks:
        webhooks = Webhooks(AsyncpgBackend(dsn), make_config(schema, **overrides))
        await webhooks.migrate()
        created.append(webhooks)
        return webhooks

    yield factory
    for webhooks in created:
        await webhooks.close()


@pytest.fixture
async def webhooks(make_webhooks: WebhooksFactory) -> Webhooks:
    return await make_webhooks()


@dataclass
class ReceivedRequest:
    path: str
    headers: dict[str, str]
    body: bytes
    received_at: float

    @property
    def json(self) -> Any:
        import json

        return json.loads(self.body)


Responder = Callable[[ReceivedRequest], Awaitable[web.StreamResponse] | web.StreamResponse]


@dataclass
class Receiver:
    """A local HTTP server that records webhook requests."""

    base_url: str = ""
    requests: list[ReceivedRequest] = field(default_factory=list)
    responder: Responder | None = None

    def url(self, path: str = "/hooks") -> str:
        return self.base_url + path

    def for_path(self, path: str) -> list[ReceivedRequest]:
        return [request for request in self.requests if request.path == path]

    async def handle(self, request: web.Request) -> web.StreamResponse:
        received = ReceivedRequest(
            path=request.path,
            headers=dict(request.headers),
            body=await request.read(),
            received_at=time.monotonic(),
        )
        self.requests.append(received)
        if self.responder is None:
            return web.Response(text="ok")
        response = self.responder(received)
        if asyncio.iscoroutine(response):
            response = await response
        assert isinstance(response, web.StreamResponse)
        return response


@pytest.fixture
async def receiver() -> AsyncIterator[Receiver]:
    state = Receiver()
    app = web.Application()
    app.router.add_route("POST", "/{tail:.*}", state.handle)
    runner = web.AppRunner(app, access_log=None, handler_cancellation=True, shutdown_timeout=0.1)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]  # type: ignore[union-attr]
    state.base_url = f"http://127.0.0.1:{port}"
    yield state
    await runner.cleanup()


async def wait_until(
    condition: Callable[[], Awaitable[bool] | bool],
    *,
    timeout: float = 10.0,
    interval: float = 0.05,
) -> None:
    deadline = time.monotonic() + timeout
    while True:
        result = condition()
        if asyncio.iscoroutine(result):
            result = await result
        if result:
            return
        if time.monotonic() > deadline:
            msg = f"condition not met within {timeout}s"
            raise AssertionError(msg)
        await asyncio.sleep(interval)
