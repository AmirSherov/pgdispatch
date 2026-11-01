"""Claiming due deliveries with ``FOR UPDATE SKIP LOCKED``.

Claiming is a *lease*, not a long-held lock: a single short transaction marks
deliveries ``in_flight`` with ``locked_until = now() + lease`` and commits
before any HTTP request is made. If the worker dies, the lease simply expires
and another worker picks the delivery up. ``attempt_count`` is incremented at
claim time; it doubles as a fencing token when the result is recorded, so a
worker whose lease expired cannot overwrite a newer attempt's outcome.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from pgdispatch.backends.base import Row
from pgdispatch.models import Delivery, DeliveryStatus, to_uuid

__all__ = ["CLAIM", "CLAIM_PROBES", "ClaimedDelivery"]

# A delivery is due when it is pending (or its lease expired) and its time has
# come. With an ordering key it must also be the oldest unfinished delivery
# with that key for its endpoint.
_DUE = """
    d.status IN ('pending', 'in_flight')
    AND (d.status = 'pending' OR d.locked_until < now())
    AND d.next_attempt_at <= now()
    AND (
        d.ordering_key IS NULL
        OR NOT EXISTS (
            SELECT 1 FROM {schema}.deliveries AS prior
            WHERE prior.endpoint_id = d.endpoint_id
              AND prior.ordering_key = d.ordering_key
              AND prior.id < d.id
              AND prior.status IN ('pending', 'in_flight')
        )
    )
"""

_LEASE = """
UPDATE {schema}.deliveries AS d
SET status = 'in_flight',
    locked_until = now() + CAST(:lease AS double precision) * interval '1 second',
    attempt_count = d.attempt_count + 1,
    updated_at = now()
FROM picked, {schema}.events AS ev, {schema}.endpoints AS e
WHERE d.id = picked.id AND ev.id = d.event_id AND e.id = d.endpoint_id
RETURNING d.id, d.event_id, d.endpoint_id, d.ordering_key, d.attempt_count, d.schedule_offset,
          d.next_attempt_at, d.created_at, ev.event_type, ev.payload::text AS payload,
          ev.created_at AS event_created_at, e.url, e.secrets::text AS secrets,
          e.headers::text AS headers, e.rate_limit_rps
"""

CLAIM = f"""
WITH due AS (
    SELECT d.id, d.endpoint_id, d.next_attempt_at
    FROM {{schema}}.deliveries AS d
    JOIN {{schema}}.endpoints AS e ON e.id = d.endpoint_id
    WHERE {_DUE}
      AND e.is_enabled
      AND e.circuit_state = 'closed'
      AND NOT (d.endpoint_id = ANY(CAST(:saturated AS uuid[])))
    ORDER BY d.next_attempt_at
    LIMIT CAST(:limit AS integer)
    FOR UPDATE OF d SKIP LOCKED
), ranked AS (
    SELECT due.id, due.endpoint_id,
           row_number() OVER (
               PARTITION BY due.endpoint_id ORDER BY due.next_attempt_at, due.id
           ) AS position
    FROM due
), picked AS (
    -- Respect the per-endpoint concurrency limit across this batch and the
    -- requests this worker already has in flight.
    SELECT ranked.id
    FROM ranked
    LEFT JOIN unnest(CAST(:busy_ids AS uuid[]), CAST(:busy_counts AS integer[]))
        AS busy(endpoint_id, in_flight) ON busy.endpoint_id = ranked.endpoint_id
    WHERE ranked.position <= CAST(:endpoint_limit AS integer) - coalesce(busy.in_flight, 0)
)
{_LEASE}
"""
"""Claim due deliveries of endpoints whose circuit breaker is closed."""

CLAIM_PROBES = f"""
WITH probe AS (
    UPDATE {{schema}}.endpoints AS e
    SET circuit_state = 'half_open',
        circuit_retry_at = now() + CAST(:lease AS double precision) * interval '1 second',
        updated_at = now()
    WHERE e.id IN (
        SELECT c.id FROM {{schema}}.endpoints AS c
        WHERE c.is_enabled
          AND c.circuit_state <> 'closed'
          AND c.circuit_retry_at <= now()
          AND EXISTS (
              SELECT 1 FROM {{schema}}.deliveries AS d
              WHERE d.endpoint_id = c.id AND {_DUE}
          )
        ORDER BY c.circuit_retry_at
        LIMIT CAST(:limit AS integer)
        FOR UPDATE SKIP LOCKED
    )
    RETURNING e.id
), picked AS (
    SELECT candidate.id
    FROM probe
    CROSS JOIN LATERAL (
        SELECT d.id FROM {{schema}}.deliveries AS d
        WHERE d.endpoint_id = probe.id AND {_DUE}
        ORDER BY d.next_attempt_at
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    ) AS candidate
)
{_LEASE}
"""
"""Move endpoints whose cooldown elapsed to half-open and claim one probe delivery each.

The endpoint itself is leased too (``circuit_retry_at``): if the worker dies
during the probe, another probe becomes possible once the lease expires.
"""


@dataclass(frozen=True, slots=True)
class ClaimedDelivery:
    """Everything a worker needs to send one delivery."""

    id: int
    event_id: uuid.UUID
    endpoint_id: uuid.UUID
    ordering_key: str | None
    attempt_count: int
    schedule_offset: int
    next_attempt_at: datetime
    created_at: datetime
    event_type: str
    payload: str
    event_created_at: datetime
    url: str
    secrets: str
    headers: Mapping[str, str]
    rate_limit_rps: float | None

    @property
    def attempts_in_round(self) -> int:
        """Attempts made since the retry schedule (re)started, including this one."""
        return self.attempt_count - self.schedule_offset

    @classmethod
    def from_row(cls, row: Row) -> ClaimedDelivery:
        values: dict[str, Any] = dict(row)
        values["event_id"] = to_uuid(row["event_id"])
        values["endpoint_id"] = to_uuid(row["endpoint_id"])
        values["headers"] = json.loads(row["headers"])
        return cls(**values)

    def to_delivery(
        self,
        *,
        status: DeliveryStatus,
        status_code: int | None,
        error: str | None,
        finished_at: datetime | None,
    ) -> Delivery:
        return Delivery(
            id=self.id,
            event_id=self.event_id,
            endpoint_id=self.endpoint_id,
            event_type=self.event_type,
            ordering_key=self.ordering_key,
            status=status,
            attempt_count=self.attempt_count,
            next_attempt_at=self.next_attempt_at,
            last_status_code=status_code,
            last_error=error,
            created_at=self.created_at,
            finished_at=finished_at,
        )
