"""Queue statistics and retention clean-up."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from pgdispatch.backends.base import Operation, Query

__all__ = ["QueueStats", "delete_expired_events", "queue_stats"]


@dataclass(frozen=True, slots=True)
class QueueStats:
    """A snapshot of the delivery queue, as shown by ``pgdispatch status``."""

    pending: int
    """Deliveries waiting for an attempt, on enabled endpoints."""
    due: int
    """Deliveries a worker could send right now (not held back by a breaker or ordering)."""
    in_flight: int
    """Deliveries being sent right now."""
    failed_last_24h: int
    oldest_due_age: timedelta
    """How long the most overdue sendable delivery has been waiting: the delivery lag.

    Deliveries held back on purpose (an open circuit breaker, an earlier
    delivery with the same ordering key still retrying) do not count, so the
    lag reflects worker capacity, not one customer's failing endpoint.
    """
    endpoints_enabled: int
    endpoints_disabled: int
    circuits_open: int
    """Enabled endpoints whose circuit breaker is open or half-open."""
    active_workers: int


_STATS = """
WITH active AS (
    SELECT d.status, d.next_attempt_at,
           -- A delivery is "due" only if a worker could send it right now:
           -- its time has come, its endpoint's breaker is closed, and it is
           -- not waiting behind an earlier delivery with the same ordering key.
           d.status = 'pending'
           AND d.next_attempt_at <= now()
           AND e.circuit_state = 'closed'
           AND (
               d.ordering_key IS NULL
               OR NOT EXISTS (
                   SELECT 1 FROM {schema}.deliveries AS prior
                   WHERE prior.endpoint_id = d.endpoint_id
                     AND prior.ordering_key = d.ordering_key
                     AND prior.id < d.id
                     AND prior.status IN ('pending', 'in_flight')
               )
           ) AS is_due
    FROM {schema}.deliveries AS d
    JOIN {schema}.endpoints AS e ON e.id = d.endpoint_id
    WHERE d.status IN ('pending', 'in_flight') AND e.is_enabled
)
SELECT
    (SELECT count(*) FROM active WHERE status = 'pending') AS pending,
    (SELECT count(*) FROM active WHERE is_due) AS due,
    (SELECT count(*) FROM active WHERE status = 'in_flight') AS in_flight,
    (SELECT count(*) FROM {schema}.deliveries
      WHERE status = 'failed' AND finished_at > now() - interval '24 hours') AS failed_last_24h,
    (SELECT coalesce(extract(epoch FROM now() - min(next_attempt_at)), 0)::double precision
       FROM active WHERE is_due) AS oldest_due_age,
    (SELECT count(*) FROM {schema}.endpoints WHERE is_enabled) AS endpoints_enabled,
    (SELECT count(*) FROM {schema}.endpoints WHERE NOT is_enabled) AS endpoints_disabled,
    (SELECT count(*) FROM {schema}.endpoints
      WHERE is_enabled AND circuit_state <> 'closed') AS circuits_open,
    (SELECT count(*) FROM {schema}.workers
      WHERE heartbeat_at > now() - CAST(:worker_ttl AS double precision) * interval '1 second')
      AS active_workers
"""


def queue_stats(*, worker_ttl: timedelta) -> Operation[QueueStats]:
    rows = yield Query(_STATS, {"worker_ttl": worker_ttl.total_seconds()})
    row = rows[0]
    return QueueStats(
        pending=row["pending"],
        due=row["due"],
        in_flight=row["in_flight"],
        failed_last_24h=row["failed_last_24h"],
        oldest_due_age=timedelta(seconds=row["oldest_due_age"]),
        endpoints_enabled=row["endpoints_enabled"],
        endpoints_disabled=row["endpoints_disabled"],
        circuits_open=row["circuits_open"],
        active_workers=row["active_workers"],
    )


def delete_expired_events(*, older_than: timedelta, batch_size: int) -> Operation[int]:
    """Delete one batch of events older than ``older_than``; return how many were deleted.

    Deliveries and attempts go with their event (``ON DELETE CASCADE``). Events
    with a delivery that a worker is sending right now are left for the next run.
    """
    rows = yield Query(
        """
        WITH doomed AS (
            SELECT ev.id FROM {schema}.events AS ev
            WHERE ev.created_at < now() - CAST(:age AS double precision) * interval '1 second'
              AND NOT EXISTS (
                  SELECT 1 FROM {schema}.deliveries AS d
                  WHERE d.event_id = ev.id AND d.status = 'in_flight' AND d.locked_until > now()
              )
            ORDER BY ev.created_at
            LIMIT CAST(:batch_size AS integer)
            FOR UPDATE SKIP LOCKED
        ), deleted AS (
            DELETE FROM {schema}.events AS ev USING doomed WHERE ev.id = doomed.id RETURNING 1
        )
        SELECT count(*) AS deleted FROM deleted
        """,
        {"age": older_than.total_seconds(), "batch_size": batch_size},
    )
    deleted: int = rows[0]["deleted"]
    return deleted
