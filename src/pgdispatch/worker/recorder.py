"""Writing delivery results back to PostgreSQL in batches.

Send tasks hand their results to the recorder instead of writing them one by
one. The recorder flushes whatever has accumulated in a single transaction:

1. lock the affected deliveries (in id order, so concurrent flushes cannot deadlock),
2. update them, fenced by ``attempt_count`` so a stale result never wins,
3. bulk-insert the attempt log,
4. update the circuit breaker of every endpoint involved.

Under load this turns thousands of tiny transactions into a few large ones,
which is what keeps PostgreSQL from becoming the bottleneck.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import asyncpg

from pgdispatch.backends.asyncpg import AsyncpgConnection
from pgdispatch.backends.base import Row, render_schema, to_numeric_style
from pgdispatch.core.circuit import (
    BreakerPolicy,
    BreakerState,
    BreakerUpdate,
    record_failure,
    record_success,
)
from pgdispatch.core.common import ENDPOINT_COLUMNS
from pgdispatch.hooks import Hooks
from pgdispatch.metrics import Metrics
from pgdispatch.models import CircuitState, Delivery, DeliveryStatus, Endpoint, to_uuid
from pgdispatch.worker.fetcher import ClaimedDelivery
from pgdispatch.worker.sender import SendOutcome

__all__ = ["DeliveryRecord", "Recorder"]

logger = logging.getLogger("pgdispatch.worker")

MAX_FLUSH_SIZE = 1000
GONE_REASON = "the endpoint answered 410 Gone"
AUTO_DISABLE_REASON = "the endpoint has been failing continuously since {since}"


@dataclass(frozen=True, slots=True)
class DeliveryRecord:
    """The result of processing one claimed delivery."""

    delivery: ClaimedDelivery
    status: DeliveryStatus
    """New status: ``pending`` means "try again in ``retry_in`` seconds"."""
    retry_in: float = 0.0
    outcome: SendOutcome | None = None
    """``None`` when no request was made and the claim is simply released."""
    request_headers: Mapping[str, str] = field(default_factory=dict)
    attempted_at: datetime | None = None

    @property
    def released(self) -> bool:
        return self.outcome is None


_LOCK_DELIVERIES = """
SELECT id FROM {schema}.deliveries
WHERE id = ANY(CAST(:ids AS bigint[]))
ORDER BY id
FOR UPDATE
"""

_UPDATE_DELIVERIES = """
UPDATE {schema}.deliveries AS d
SET status = u.status,
    attempt_count = CASE WHEN u.released THEN d.attempt_count - 1 ELSE d.attempt_count END,
    next_attempt_at = CASE
        WHEN u.status = 'pending' THEN now() + u.retry_in * interval '1 second'
        ELSE d.next_attempt_at
    END,
    locked_until = NULL,
    last_status_code = CASE WHEN u.released THEN d.last_status_code ELSE u.status_code END,
    last_error = CASE WHEN u.released THEN d.last_error ELSE u.error END,
    finished_at = CASE WHEN u.status IN ('succeeded', 'failed', 'cancelled') THEN now() END,
    updated_at = now()
FROM unnest(
    CAST(:ids AS bigint[]), CAST(:attempts AS integer[]), CAST(:statuses AS text[]),
    CAST(:retry_in AS double precision[]), CAST(:status_codes AS integer[]),
    CAST(:errors AS text[]), CAST(:released AS boolean[])
) AS u(id, attempt, status, retry_in, status_code, error, released)
WHERE d.id = u.id AND d.status = 'in_flight' AND d.attempt_count = u.attempt
RETURNING d.id
"""

_INSERT_ATTEMPTS = """
INSERT INTO {schema}.attempts
    (delivery_id, attempt_number, request_headers, response_status, response_body,
     duration_ms, error, created_at)
SELECT * FROM unnest(
    CAST(:ids AS bigint[]), CAST(:numbers AS integer[]), CAST(:headers AS jsonb[]),
    CAST(:statuses AS integer[]), CAST(:bodies AS text[]), CAST(:durations AS integer[]),
    CAST(:errors AS text[]), CAST(:created AS timestamptz[])
)
"""

# Endpoints that only saw successes and are already healthy need no update;
# skipping them keeps hot, healthy endpoints free of row-lock contention.
_LOCK_ENDPOINTS = """
SELECT now() AS now, e.id, e.is_enabled, e.circuit_state, e.consecutive_failures,
       e.failing_since, e.circuit_opened_at, e.circuit_retry_at, e.circuit_trips
FROM {schema}.endpoints AS e
WHERE e.id = ANY(CAST(:ids AS uuid[]))
  AND (
      e.id = ANY(CAST(:failing_ids AS uuid[]))
      OR e.circuit_state <> 'closed'
      OR e.consecutive_failures > 0
  )
ORDER BY e.id
FOR UPDATE
"""

_UPDATE_ENDPOINTS = f"""
UPDATE {{schema}}.endpoints AS e
SET circuit_state = u.state,
    consecutive_failures = u.failures,
    failing_since = u.failing_since,
    circuit_opened_at = u.opened_at,
    circuit_retry_at = u.retry_at,
    circuit_trips = u.trips,
    is_enabled = e.is_enabled AND u.disable_reason IS NULL,
    disabled_reason = CASE
        WHEN e.is_enabled AND u.disable_reason IS NOT NULL THEN u.disable_reason
        ELSE e.disabled_reason
    END,
    updated_at = now()
FROM unnest(
    CAST(:ids AS uuid[]), CAST(:states AS text[]), CAST(:failures AS integer[]),
    CAST(:failing_since AS timestamptz[]), CAST(:opened_at AS timestamptz[]),
    CAST(:retry_at AS timestamptz[]), CAST(:trips AS integer[]), CAST(:disable_reasons AS text[])
) AS u(id, state, failures, failing_since, opened_at, retry_at, trips, disable_reason)
WHERE e.id = u.id
RETURNING {ENDPOINT_COLUMNS}
"""


@dataclass(frozen=True, slots=True)
class _Entry:
    record: DeliveryRecord
    written: asyncio.Future[None]


@dataclass(slots=True)
class _Notifications:
    """Hook calls to make once the flush has committed."""

    failed: list[Delivery] = field(default_factory=list)
    circuits_opened: list[Endpoint] = field(default_factory=list)
    disabled: list[tuple[Endpoint, str]] = field(default_factory=list)


class Recorder:
    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        schema: str,
        policy: BreakerPolicy,
        hooks: Hooks,
        metrics: Metrics,
        on_flush: Callable[[], None],
    ) -> None:
        self._pool = pool
        self._schema = schema
        self._policy = policy
        self._hooks = hooks
        self._metrics = metrics
        self._on_flush = on_flush
        self._queue: asyncio.Queue[_Entry | None] = asyncio.Queue()
        self._give_up_at: float | None = None

    def add(self, record: DeliveryRecord) -> asyncio.Future[None]:
        """Queue a result; the returned future completes once it has been written."""
        written: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._queue.put_nowait(_Entry(record, written))
        return written

    async def run(self) -> None:
        """Flush records until :meth:`close` is called and everything is written."""
        closing = False
        while not closing:
            first = await self._queue.get()
            if first is None:
                break
            entries = [first]
            while len(entries) < MAX_FLUSH_SIZE and not self._queue.empty():
                entry = self._queue.get_nowait()
                if entry is None:
                    closing = True
                    break
                entries.append(entry)
            await self._flush_with_retries([entry.record for entry in entries])
            for entry in entries:
                if not entry.written.done():
                    entry.written.set_result(None)
            self._on_flush()

    def close(self, *, timeout: float) -> None:
        """Ask :meth:`run` to finish after writing everything queued so far."""
        self._give_up_at = asyncio.get_running_loop().time() + timeout
        self._queue.put_nowait(None)

    async def _flush_with_retries(self, batch: Sequence[DeliveryRecord]) -> None:
        delay = 0.5
        while True:
            try:
                notifications = await self._flush(batch)
            except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError):
                loop = asyncio.get_running_loop()
                if self._give_up_at is not None and loop.time() + delay > self._give_up_at:
                    logger.exception(
                        "pgdispatch could not record %d results before shutdown; "
                        "their leases will expire and they will be sent again",
                        len(batch),
                    )
                    return
                logger.warning(
                    "pgdispatch failed to record results, retrying in %.1fs", delay, exc_info=True
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, 10.0)
            except Exception:
                # A bug must not stall the worker: the leases of these deliveries
                # expire and they are sent again.
                logger.exception("pgdispatch failed to record %d results", len(batch))
                return
            else:
                await self._notify(notifications)
                return

    async def _flush(self, batch: Sequence[DeliveryRecord]) -> _Notifications:
        notifications = _Notifications()
        async with self._pool.acquire() as connection, connection.transaction():
            # Losing the last few commits in a database crash only means those
            # deliveries are sent again once their leases expire, which
            # at-least-once delivery allows; in exchange there is no fsync wait.
            await connection.execute("SET LOCAL synchronous_commit TO off")
            rows = await self._fetch(
                connection, _LOCK_DELIVERIES, ids=[record.delivery.id for record in batch]
            )
            existing = {row["id"] for row in rows}
            applied = await self._update_deliveries(connection, batch)
            await self._insert_attempts(connection, [r for r in batch if r.delivery.id in existing])
            await self._update_endpoints(connection, batch, notifications)

        for record in batch:
            if record.status is DeliveryStatus.FAILED and record.delivery.id in applied:
                self._metrics.deliveries_failed.inc()
                notifications.failed.append(_failed_delivery(record))
        return notifications

    async def _update_deliveries(
        self, connection: AsyncpgConnection, batch: Sequence[DeliveryRecord]
    ) -> set[int]:
        rows = await self._fetch(
            connection,
            _UPDATE_DELIVERIES,
            ids=[r.delivery.id for r in batch],
            attempts=[r.delivery.attempt_count for r in batch],
            statuses=[r.status.value for r in batch],
            retry_in=[r.retry_in for r in batch],
            status_codes=[r.outcome.status_code if r.outcome else None for r in batch],
            errors=[r.outcome.error if r.outcome else None for r in batch],
            released=[r.released for r in batch],
        )
        applied = {row["id"] for row in rows}
        stale = len(batch) - len(applied)
        if stale:
            # Possible if a lease expired mid-request (e.g. a long pause) and
            # another worker already retried the delivery.
            logger.warning("pgdispatch skipped %d stale delivery results", stale)
        return applied

    async def _insert_attempts(
        self, connection: AsyncpgConnection, batch: Sequence[DeliveryRecord]
    ) -> None:
        attempts = [r for r in batch if r.outcome is not None]
        if not attempts:
            return
        await self._fetch(
            connection,
            _INSERT_ATTEMPTS,
            ids=[r.delivery.id for r in attempts],
            numbers=[r.delivery.attempt_count for r in attempts],
            headers=[json.dumps(dict(r.request_headers)) for r in attempts],
            statuses=[r.outcome.status_code for r in attempts if r.outcome],
            bodies=[r.outcome.response_body for r in attempts if r.outcome],
            durations=[r.outcome.duration_ms for r in attempts if r.outcome],
            errors=[r.outcome.error for r in attempts if r.outcome],
            created=[r.attempted_at for r in attempts],
        )

    async def _update_endpoints(
        self,
        connection: AsyncpgConnection,
        batch: Sequence[DeliveryRecord],
        notifications: _Notifications,
    ) -> None:
        outcomes: dict[Any, list[DeliveryRecord]] = defaultdict(list)
        for record in batch:
            if record.outcome is not None:
                outcomes[record.delivery.endpoint_id].append(record)
        if not outcomes:
            return
        failing = {
            record.delivery.endpoint_id
            for record in batch
            if record.outcome is not None and not record.outcome.succeeded
        }
        rows = await self._fetch(
            connection,
            _LOCK_ENDPOINTS,
            ids=[str(i) for i in outcomes],
            failing_ids=[str(i) for i in failing],
        )

        changes: list[tuple[Row, BreakerUpdate, str | None]] = []
        for row in rows:
            before = _breaker_state(row)
            update = BreakerUpdate(before)
            opened = False
            gone = False
            for record in outcomes[to_uuid(row["id"])]:
                assert record.outcome is not None  # noqa: S101 - filtered above
                gone = gone or record.outcome.gone
                if record.outcome.succeeded:
                    update = record_success(update.state)
                else:
                    update = record_failure(update.state, now=row["now"], policy=self._policy)
                    opened = opened or update.opened
            opened = opened and update.state.state is CircuitState.OPEN
            update = BreakerUpdate(update.state, opened=opened, disable=update.disable)
            reason = _disable_reason(row, update, gone=gone)
            if update.state != before or reason is not None:
                changes.append((row, update, reason))
        if not changes:
            return

        updated = await self._fetch(
            connection,
            _UPDATE_ENDPOINTS,
            ids=[str(row["id"]) for row, _, _ in changes],
            states=[update.state.state.value for _, update, _ in changes],
            failures=[update.state.consecutive_failures for _, update, _ in changes],
            failing_since=[update.state.failing_since for _, update, _ in changes],
            opened_at=[update.state.opened_at for _, update, _ in changes],
            retry_at=[update.state.retry_at for _, update, _ in changes],
            trips=[update.state.trips for _, update, _ in changes],
            disable_reasons=[reason for _, _, reason in changes],
        )
        endpoints = {endpoint.id: endpoint for endpoint in map(Endpoint.from_row, updated)}
        for row, update, reason in changes:
            endpoint = endpoints[to_uuid(row["id"])]
            if update.opened:
                self._log_circuit_opened(endpoint, update)
                notifications.circuits_opened.append(endpoint)
            if reason is not None and row["is_enabled"]:
                logger.warning(
                    "pgdispatch disabled endpoint %s: %s",
                    endpoint.id,
                    reason,
                    extra={"endpoint_id": str(endpoint.id)},
                )
                notifications.disabled.append((endpoint, reason))

    async def _notify(self, notifications: _Notifications) -> None:
        for delivery in notifications.failed:
            await self._hooks.delivery_failed(delivery)
        for endpoint in notifications.circuits_opened:
            await self._hooks.circuit_opened(endpoint)
        for endpoint, reason in notifications.disabled:
            await self._hooks.endpoint_disabled(endpoint, reason)

    async def _fetch(self, connection: AsyncpgConnection, sql: str, **params: Any) -> list[Row]:
        statement, names = to_numeric_style(render_schema(sql, self._schema))
        records = await connection.fetch(statement, *(params[name] for name in names))
        return [dict(record) for record in records]

    @staticmethod
    def _log_circuit_opened(endpoint: Endpoint, update: BreakerUpdate) -> None:
        logger.warning(
            "pgdispatch opened the circuit breaker of endpoint %s after %d consecutive failures",
            endpoint.id,
            update.state.consecutive_failures,
            extra={
                "endpoint_id": str(endpoint.id),
                "consecutive_failures": update.state.consecutive_failures,
                "retry_at": update.state.retry_at.isoformat() if update.state.retry_at else None,
            },
        )


def _breaker_state(row: Row) -> BreakerState:
    return BreakerState(
        state=CircuitState(row["circuit_state"]),
        consecutive_failures=row["consecutive_failures"],
        failing_since=row["failing_since"],
        opened_at=row["circuit_opened_at"],
        retry_at=row["circuit_retry_at"],
        trips=row["circuit_trips"],
    )


def _disable_reason(row: Row, update: BreakerUpdate, *, gone: bool) -> str | None:
    if not row["is_enabled"]:
        return None
    if gone:
        return GONE_REASON
    if update.disable and update.state.failing_since is not None:
        return AUTO_DISABLE_REASON.format(since=update.state.failing_since.isoformat())
    return None


def _failed_delivery(record: DeliveryRecord) -> Delivery:
    outcome = record.outcome
    return record.delivery.to_delivery(
        status=DeliveryStatus.FAILED,
        status_code=outcome.status_code if outcome else None,
        error=outcome.error if outcome else None,
        finished_at=record.attempted_at,
    )
