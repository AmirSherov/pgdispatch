"""The delivery worker.

A worker runs four cooperating loops on one event loop:

* **dispatch** — waits for work (``LISTEN`` wake-up or the polling interval),
  claims due deliveries and starts one task per delivery, never exceeding
  ``concurrency`` requests in flight;
* **send tasks** — rate-limit, sign and POST one delivery each, then hand the
  result to the recorder;
* **recorder** — writes results back in batches (see :mod:`.recorder`);
* **maintenance** — heartbeat, metrics, ``LISTEN`` health and retention clean-up.

Any number of workers, in any number of processes or hosts, can run against
the same database: they coordinate only through row locks and leases.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
import time
import uuid
from collections import Counter
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp
import asyncpg
from asyncpg.pool import PoolConnectionProxy

from pgdispatch._version import __version__
from pgdispatch.backends.asyncpg import AsyncpgConnection, AsyncpgExecutor
from pgdispatch.backends.base import Row, render_schema, run_async, to_numeric_style
from pgdispatch.config import WebhooksConfig
from pgdispatch.core.circuit import BreakerPolicy
from pgdispatch.core.maintenance import delete_expired_events, queue_stats
from pgdispatch.core.payload import build_body
from pgdispatch.core.retry import next_retry_delay
from pgdispatch.core.secrets import SecretStore, fernet_cipher
from pgdispatch.core.signing import signature_header
from pgdispatch.hooks import Hooks
from pgdispatch.metrics import Metrics, get_metrics
from pgdispatch.models import DeliveryStatus
from pgdispatch.worker.fetcher import CLAIM, CLAIM_PROBES, ClaimedDelivery
from pgdispatch.worker.ratelimit import RateLimiter
from pgdispatch.worker.recorder import DeliveryRecord, Recorder
from pgdispatch.worker.sender import AttemptResult, Sender, SendOutcome, create_session

__all__ = ["PoolFactory", "Worker"]

logger = logging.getLogger("pgdispatch.worker")

PoolFactory = Callable[[], Awaitable[asyncpg.Pool]]

_DATABASE_ERRORS = (asyncpg.PostgresError, asyncpg.InterfaceError, OSError)
_METRICS_INTERVAL = 15.0
_REDACTED = "[redacted]"
_LOGGED_HEADERS = (
    "content-type",
    "user-agent",
    "webhook-id",
    "webhook-timestamp",
    "webhook-signature",
)

_HEARTBEAT = """
INSERT INTO {schema}.workers (id, hostname, pid, version, concurrency)
VALUES (CAST(:id AS uuid), CAST(:hostname AS text), CAST(:pid AS integer),
        CAST(:version AS text), CAST(:concurrency AS integer))
ON CONFLICT (id) DO UPDATE SET heartbeat_at = now()
"""

_ACTIVE_WORKERS = """
WITH expired AS (
    DELETE FROM {schema}.workers
    WHERE heartbeat_at < now() - CAST(:ttl AS double precision) * 10 * interval '1 second'
)
SELECT count(*) AS active FROM {schema}.workers
WHERE heartbeat_at > now() - CAST(:ttl AS double precision) * interval '1 second'
"""


class Worker:
    """Delivers webhooks until :meth:`stop` is called.

    Usually created with :meth:`Webhooks.worker() <pgdispatch.Webhooks.worker>`
    or run with ``pgdispatch worker``. Use it directly for custom setups::

        worker = Worker.from_dsn("postgresql://...", WebhooksConfig(concurrency=100))
        await worker.run()
    """

    def __init__(
        self,
        pool_factory: PoolFactory,
        config: WebhooksConfig | None = None,
        *,
        hooks: Hooks | None = None,
        metrics: Metrics | None = None,
        secret_store: SecretStore | None = None,
        http_session: aiohttp.ClientSession | None = None,
    ) -> None:
        self.config = config or WebhooksConfig()
        self.id = uuid.uuid4()
        self._pool_factory = pool_factory
        self._hooks = hooks or Hooks()
        self._metrics = metrics or get_metrics()
        self._secrets = secret_store or _secret_store_from(self.config)
        self._http_session = http_session
        self._limiter = RateLimiter()
        self._policy = BreakerPolicy(
            failure_threshold=self.config.failure_threshold,
            cooldown=self.config.circuit_cooldown,
            max_cooldown=self.config.circuit_max_cooldown,
            auto_disable_after=self.config.auto_disable_after,
        )

        self._stopping = asyncio.Event()
        self._wake = asyncio.Event()
        self._slot_freed = asyncio.Event()
        self._tasks: set[asyncio.Task[None]] = set()
        self._in_flight: Counter[uuid.UUID] = Counter()
        self._active_workers = 1
        self._listener: PoolConnectionProxy | None = None
        self._running = False

    @classmethod
    def from_dsn(cls, dsn: str, config: WebhooksConfig | None = None, **kwargs: Any) -> Worker:
        from pgdispatch.backends.asyncpg import AsyncpgBackend

        return cls(AsyncpgBackend(dsn).create_worker_pool, config, **kwargs)

    @property
    def running(self) -> bool:
        return self._running

    def stop(self) -> None:
        """Stop claiming deliveries and shut down gracefully.

        Safe to call from a signal handler.
        """
        self._stopping.set()
        self._wake.set()
        self._slot_freed.set()

    async def run(self) -> None:
        """Run until :meth:`stop` is called, then finish in-flight requests and return.

        A worker runs once: calling :meth:`stop` before or during ``run`` ends it.
        Create a new worker to start again.
        """
        if self._running:
            msg = "this worker is already running"
            raise RuntimeError(msg)
        pool = await self._pool_factory()
        self._running = True
        session = self._http_session or create_session(
            concurrency=self.config.concurrency,
            allow_private_ips=self.config.allow_private_ips,
            timeout=self.config.request_timeout,
        )
        sender = Sender(
            session,
            timeout=self.config.request_timeout,
            store_response_body=self.config.store_response_body,
            max_response_body_bytes=self.config.max_response_body_bytes,
            allow_private_ips=self.config.allow_private_ips,
            proxy=self.config.outbound_proxy,
        )
        recorder = Recorder(
            pool,
            schema=self.config.db_schema,
            policy=self._policy,
            hooks=self._hooks,
            metrics=self._metrics,
            on_flush=self._wake.set,  # results may unblock ordered deliveries
        )
        recorder_task = asyncio.create_task(recorder.run(), name="pgdispatch-recorder")
        maintenance_task: asyncio.Task[None] | None = None
        try:
            await self._heartbeat(pool)
            await self._listen(pool)
            maintenance_task = asyncio.create_task(
                self._maintain(pool), name="pgdispatch-maintenance"
            )
            logger.info(
                "pgdispatch worker %s started (concurrency=%d, schema=%s)",
                self.id,
                self.config.concurrency,
                self.config.db_schema,
            )
            await self._dispatch(pool, sender, recorder)
        finally:
            await self._drain()
            shutdown_timeout = self.config.shutdown_timeout.total_seconds()
            recorder.close(timeout=shutdown_timeout)
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(recorder_task, shutdown_timeout + 5)
            if maintenance_task is not None:
                maintenance_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await maintenance_task
            await self._shutdown_connections(pool)
            if self._http_session is None:
                await session.close()
            self._running = False
            logger.info("pgdispatch worker %s stopped", self.id)

    # --- dispatch -------------------------------------------------------------

    async def _dispatch(self, pool: asyncpg.Pool, sender: Sender, recorder: Recorder) -> None:
        backoff = 0.5
        while not self._stopping.is_set():
            free = self.config.concurrency - len(self._tasks)
            if free <= 0:
                self._slot_freed.clear()
                if not self._stopping.is_set():  # stop() may have fired just now
                    await self._slot_freed.wait()
                continue

            requested = min(free, self.config.batch_size)
            self._wake.clear()
            try:
                claimed = await self._claim(pool, requested)
            except _DATABASE_ERRORS:
                logger.warning(
                    "pgdispatch could not claim deliveries, retrying in %.1fs",
                    backoff,
                    exc_info=True,
                )
                await self._sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            backoff = 0.5

            if claimed:
                self._metrics.worker_batch_size.observe(len(claimed))
            for delivery in claimed:
                self._start(delivery, sender, recorder)
            if len(claimed) < requested and not self._stopping.is_set():
                # Nothing more is due right now: wait for a NOTIFY, a finished
                # batch, or the polling interval.
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(
                        self._wake.wait(), self.config.poll_interval.total_seconds()
                    )

    async def _claim(self, pool: asyncpg.Pool, limit: int) -> list[ClaimedDelivery]:
        busy = {endpoint: count for endpoint, count in self._in_flight.items() if count > 0}
        endpoint_limit = self.config.endpoint_concurrency
        lease = self.config.lease_duration.total_seconds()
        async with pool.acquire() as connection, connection.transaction():
            # See Recorder._flush: a lost claim only means an earlier retry.
            await connection.execute("SET LOCAL synchronous_commit TO off")
            rows = await self._fetch(connection, CLAIM_PROBES, lease=lease, limit=limit)
            if len(rows) < limit:
                rows += await self._fetch(
                    connection,
                    CLAIM,
                    lease=lease,
                    limit=limit - len(rows),
                    saturated=[str(e) for e, count in busy.items() if count >= endpoint_limit],
                    busy_ids=[str(e) for e in busy],
                    busy_counts=list(busy.values()),
                    endpoint_limit=endpoint_limit,
                )
        return [ClaimedDelivery.from_row(row) for row in rows]

    def _start(self, delivery: ClaimedDelivery, sender: Sender, recorder: Recorder) -> None:
        self._in_flight[delivery.endpoint_id] += 1
        task = asyncio.create_task(self._process(delivery, sender, recorder))
        self._tasks.add(task)

        def finished(done: asyncio.Task[None]) -> None:
            self._tasks.discard(done)
            self._in_flight[delivery.endpoint_id] -= 1
            if self._in_flight[delivery.endpoint_id] <= 0:
                del self._in_flight[delivery.endpoint_id]
            self._slot_freed.set()

        task.add_done_callback(finished)

    async def _drain(self) -> None:
        if not self._tasks:
            return
        logger.info("pgdispatch worker waiting for %d in-flight requests", len(self._tasks))
        _, pending = await asyncio.wait(
            set(self._tasks), timeout=self.config.shutdown_timeout.total_seconds()
        )
        for task in pending:
            task.cancel()
        if pending:
            logger.warning(
                "pgdispatch returned %d unfinished deliveries to the queue", len(pending)
            )
            await asyncio.gather(*pending, return_exceptions=True)

    # --- sending --------------------------------------------------------------

    async def _process(self, delivery: ClaimedDelivery, sender: Sender, recorder: Recorder) -> None:
        try:
            record = await self._deliver(delivery, sender)
        except asyncio.CancelledError:
            # Shutting down: hand the delivery back without counting an attempt.
            recorder.add(DeliveryRecord(delivery, DeliveryStatus.PENDING))
            raise
        except Exception as exc:
            logger.exception("pgdispatch failed to process delivery %s", delivery.id)
            outcome = SendOutcome(AttemptResult.NETWORK, None, None, f"internal error: {exc!r}", 0)
            record = self._resolve(delivery, outcome, {}, datetime.now(timezone.utc))
        # Keep the slot until the result is stored, so the next claim already
        # sees the updated circuit breaker and ordering state.
        await recorder.add(record)

    async def _deliver(self, delivery: ClaimedDelivery, sender: Sender) -> DeliveryRecord:
        if delivery.rate_limit_rps:
            share = delivery.rate_limit_rps / max(1, self._active_workers)
            delay = self._limiter.reserve(delivery.endpoint_id, share)
            if delay > self._max_rate_limit_wait:
                # Waiting would outlive the lease: put the delivery back instead.
                self._limiter.release(delivery.endpoint_id, share)
                return DeliveryRecord(delivery, DeliveryStatus.PENDING, retry_in=delay)
            if delay:
                await asyncio.sleep(delay)

        attempted_at = datetime.now(timezone.utc)
        body = build_body(delivery.event_type, delivery.event_created_at, delivery.payload)
        msg_id = str(delivery.event_id)
        timestamp = int(time.time())
        secrets = self._secrets.active_secrets(delivery.secrets, now=attempted_at)
        headers = {
            **delivery.headers,
            "content-type": "application/json",
            "user-agent": self.config.user_agent,
            "webhook-id": msg_id,
            "webhook-timestamp": str(timestamp),
            "webhook-signature": signature_header(secrets, msg_id, timestamp, body),
        }

        outcome = await sender.send(delivery.url, headers, body)

        result = outcome.result.value
        self._metrics.delivery_attempts.labels(result).inc()
        self._metrics.delivery_duration.labels(result).observe(outcome.duration_ms / 1000)
        self._log_attempt(delivery, outcome)
        return self._resolve(delivery, outcome, _loggable_headers(headers), attempted_at)

    def _resolve(
        self,
        delivery: ClaimedDelivery,
        outcome: SendOutcome,
        request_headers: dict[str, str],
        attempted_at: datetime,
    ) -> DeliveryRecord:
        """Decide what happens to the delivery after an attempt."""

        def record(status: DeliveryStatus, retry_in: float = 0.0) -> DeliveryRecord:
            return DeliveryRecord(
                delivery,
                status,
                retry_in=retry_in,
                outcome=outcome,
                request_headers=request_headers,
                attempted_at=attempted_at,
            )

        if outcome.succeeded:
            return record(DeliveryStatus.SUCCEEDED)
        if outcome.gone:
            return record(DeliveryStatus.CANCELLED)
        delay = next_retry_delay(
            self.config.retry_schedule,
            delivery.attempts_in_round,
            jitter=self.config.retry_jitter,
        )
        if delay is None:
            return record(DeliveryStatus.FAILED)
        if outcome.retry_after is not None:
            # Honour Retry-After, but never beyond the longest regular delay.
            delay = max(delay, min(outcome.retry_after, max(self.config.retry_schedule)))
        return record(DeliveryStatus.PENDING, retry_in=delay.total_seconds())

    @property
    def _max_rate_limit_wait(self) -> float:
        spare = self.config.lease_duration - self.config.request_timeout - timedelta(seconds=1)
        return max(spare.total_seconds(), 0.0)

    def _log_attempt(self, delivery: ClaimedDelivery, outcome: SendOutcome) -> None:
        extra = {
            "event_id": str(delivery.event_id),
            "delivery_id": delivery.id,
            "endpoint_id": str(delivery.endpoint_id),
            "attempt": delivery.attempt_count,
            "status_code": outcome.status_code,
            "duration_ms": outcome.duration_ms,
        }
        if outcome.succeeded:
            logger.debug("pgdispatch delivered %s", delivery.id, extra=extra)
        else:
            logger.info(
                "pgdispatch attempt %d of delivery %s failed: %s",
                delivery.attempt_count,
                delivery.id,
                outcome.error,
                extra=extra,
            )

    # --- LISTEN/NOTIFY ----------------------------------------------------------

    async def _listen(self, pool: asyncpg.Pool) -> None:
        """Hold one connection that LISTENs for wake-ups; polling covers any gap."""
        try:
            connection = await pool.acquire()
        except _DATABASE_ERRORS:
            logger.warning("pgdispatch could not LISTEN; falling back to polling", exc_info=True)
            return
        try:
            await connection.add_listener(self.config.notify_channel, self._on_notify)
        except _DATABASE_ERRORS:
            logger.warning("pgdispatch could not LISTEN; falling back to polling", exc_info=True)
            await pool.release(connection)
            return
        self._listener = connection

    def _on_notify(self, *_: object) -> None:
        self._wake.set()

    # --- maintenance ------------------------------------------------------------

    async def _maintain(self, pool: asyncpg.Pool) -> None:
        interval = self.config.heartbeat_interval.total_seconds()
        next_metrics = 0.0
        next_cleanup = time.monotonic() + self.config.cleanup_interval.total_seconds() / 10
        while True:
            await asyncio.sleep(interval)
            now = time.monotonic()
            await self._guard("heartbeat", self._heartbeat(pool))
            if self._listener is None or self._listener.is_closed():
                if self._listener is not None:
                    await self._guard("release LISTEN connection", pool.release(self._listener))
                    self._listener = None
                await self._listen(pool)
                self._wake.set()  # notifications may have been missed meanwhile
            if now >= next_metrics:
                await self._guard("metrics refresh", self._refresh_metrics(pool))
                next_metrics = now + _METRICS_INTERVAL
            if now >= next_cleanup:
                await self._guard("clean-up", self._cleanup(pool))
                next_cleanup = now + self.config.cleanup_interval.total_seconds()
            self._limiter.prune()

    async def _heartbeat(self, pool: asyncpg.Pool) -> None:
        async with pool.acquire() as connection:
            await self._fetch(
                connection,
                _HEARTBEAT,
                id=str(self.id),
                hostname=socket.gethostname(),
                pid=os.getpid(),
                version=__version__,
                concurrency=self.config.concurrency,
            )
            rows = await self._fetch(connection, _ACTIVE_WORKERS, ttl=self._worker_ttl)
        self._active_workers = max(1, rows[0]["active"])

    async def _refresh_metrics(self, pool: asyncpg.Pool) -> None:
        async with pool.acquire() as connection:
            stats = await run_async(
                queue_stats(worker_ttl=timedelta(seconds=self._worker_ttl)),
                AsyncpgExecutor(connection),
                schema=self.config.db_schema,
            )
        self._metrics.deliveries_pending.set(stats.pending)
        self._metrics.oldest_pending_age.set(stats.oldest_due_age.total_seconds())
        self._metrics.endpoints_circuit_open.set(stats.circuits_open)

    async def _cleanup(self, pool: asyncpg.Pool) -> None:
        """Purge expired data; an advisory lock ensures only one worker does it at a time."""
        lock = f"{self.config.db_schema}.cleanup"
        async with pool.acquire() as connection:
            acquired = await connection.fetchval("SELECT pg_try_advisory_lock(hashtext($1))", lock)
            if not acquired:
                return
            try:
                total = 0
                while not self._stopping.is_set():
                    async with connection.transaction():
                        deleted = await run_async(
                            delete_expired_events(
                                older_than=timedelta(days=self.config.retention_days),
                                batch_size=self.config.cleanup_batch_size,
                            ),
                            AsyncpgExecutor(connection),
                            schema=self.config.db_schema,
                        )
                    total += deleted
                    if deleted < self.config.cleanup_batch_size:
                        break
                if total:
                    logger.info("pgdispatch deleted %d expired events", total)
            finally:
                await connection.execute("SELECT pg_advisory_unlock(hashtext($1))", lock)

    async def _shutdown_connections(self, pool: asyncpg.Pool) -> None:
        listener, self._listener = self._listener, None
        if listener is not None and not listener.is_closed():
            await self._guard(
                "UNLISTEN", listener.remove_listener(self.config.notify_channel, self._on_notify)
            )
            await self._guard("release LISTEN connection", pool.release(listener))
        await self._guard(
            "deregistration",
            pool.execute(
                render_schema("DELETE FROM {schema}.workers WHERE id = $1", self.config.db_schema),
                self.id,
            ),
        )
        await pool.close()

    @property
    def _worker_ttl(self) -> float:
        return self.config.heartbeat_interval.total_seconds() * 3

    # --- helpers ------------------------------------------------------------------

    async def _sleep(self, seconds: float) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stopping.wait(), seconds)

    async def _fetch(self, connection: AsyncpgConnection, sql: str, **params: Any) -> list[Row]:
        statement, names = to_numeric_style(render_schema(sql, self.config.db_schema))
        records = await connection.fetch(statement, *(params[name] for name in names))
        return [dict(record) for record in records]

    @staticmethod
    async def _guard(what: str, awaitable: Awaitable[Any]) -> None:
        try:
            await awaitable
        except _DATABASE_ERRORS:
            logger.warning("pgdispatch %s failed", what, exc_info=True)


def _loggable_headers(headers: dict[str, str]) -> dict[str, str]:
    """Headers stored in the attempt log; custom header values may be credentials."""
    return {
        name.lower(): value if name.lower() in _LOGGED_HEADERS else _REDACTED
        for name, value in headers.items()
    }


def _secret_store_from(config: WebhooksConfig) -> SecretStore:
    keys = [key.get_secret_value() for key in config.secret_encryption_keys]
    return SecretStore(fernet_cipher(keys) if keys else None)
