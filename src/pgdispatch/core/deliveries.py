"""Inspecting and retrying deliveries."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pgdispatch.backends.base import Operation, Query
from pgdispatch.core.common import (
    DELIVERY_SELECT,
    OwnerFilter,
    first_or_not_found,
    owner_condition,
    validate_limit,
)
from pgdispatch.errors import ConflictError, ValidationError
from pgdispatch.models import Attempt, Delivery, DeliveryStatus

__all__ = [
    "cancel_delivery",
    "get_delivery",
    "list_attempts",
    "list_deliveries",
    "retry_deliveries",
    "retry_delivery",
]


def list_deliveries(
    *,
    endpoint_id: uuid.UUID | None,
    event_id: uuid.UUID | None,
    status: DeliveryStatus | str | None,
    since: datetime | None,
    before: int | None,
    limit: int,
    owner_id: OwnerFilter,
) -> Operation[list[Delivery]]:
    """Newest deliveries first; pass the last id as ``before`` to get the next page."""
    params: dict[str, Any] = {"limit": validate_limit(limit)}
    conditions = [owner_condition("e.owner_id", owner_id, params)]
    conditions.extend(_filters(params, endpoint_id, event_id, status, since))
    if before is not None:
        params["before"] = before
        conditions.append("d.id < CAST(:before AS bigint)")
    rows = yield Query(
        f"""
        {DELIVERY_SELECT}
        WHERE {" AND ".join(conditions)}
        ORDER BY d.id DESC
        LIMIT CAST(:limit AS integer)
        """,
        params,
    )
    return [Delivery.from_row(row) for row in rows]


def get_delivery(delivery_id: int, *, owner_id: OwnerFilter) -> Operation[Delivery]:
    params: dict[str, Any] = {"id": delivery_id}
    owner_sql = owner_condition("e.owner_id", owner_id, params)
    rows = yield Query(
        f"{DELIVERY_SELECT} WHERE d.id = CAST(:id AS bigint) AND {owner_sql}",
        params,
    )
    return Delivery.from_row(first_or_not_found(rows, "delivery", delivery_id))


def list_attempts(delivery_id: int, *, owner_id: OwnerFilter) -> Operation[list[Attempt]]:
    """All attempts of a delivery, oldest first."""
    yield from get_delivery(delivery_id, owner_id=owner_id)  # ownership check
    rows = yield Query(
        """
        SELECT a.id, a.delivery_id, a.attempt_number, a.request_headers::text AS request_headers,
               a.response_status, a.response_body, a.duration_ms, a.error, a.created_at
        FROM {schema}.attempts AS a
        WHERE a.delivery_id = CAST(:id AS bigint)
        ORDER BY a.id
        """,
        {"id": delivery_id},
    )
    return [Attempt.from_row(row) for row in rows]


# A retry restarts the schedule: ``schedule_offset`` marks where the new round begins.
_RETRY_ASSIGNMENTS = """
    status = 'pending', next_attempt_at = now(), locked_until = NULL,
    schedule_offset = d.attempt_count, finished_at = NULL, updated_at = now()
"""


def retry_delivery(delivery_id: int, *, owner_id: OwnerFilter, channel: str) -> Operation[Delivery]:
    """Schedule a delivery for an immediate attempt with a fresh retry schedule.

    Works for failed, cancelled and even succeeded deliveries (to resend).
    A delivery that a worker is sending right now cannot be retried.
    """
    params: dict[str, Any] = {"id": delivery_id, "channel": channel}
    owner_sql = owner_condition("e.owner_id", owner_id, params)
    rows = yield Query(
        f"""
        WITH target AS (
            SELECT d.id, d.status FROM {{schema}}.deliveries AS d
            JOIN {{schema}}.endpoints AS e ON e.id = d.endpoint_id
            WHERE d.id = CAST(:id AS bigint) AND {owner_sql}
            FOR UPDATE OF d
        ), updated AS (
            UPDATE {{schema}}.deliveries AS d SET {_RETRY_ASSIGNMENTS}
            FROM target
            WHERE d.id = target.id AND target.status <> 'in_flight'
            RETURNING d.id
        ), wake AS (
            SELECT pg_notify(CAST(:channel AS text), '') FROM updated
        )
        SELECT target.status, (SELECT count(*) FROM updated) AS updated,
               (SELECT count(*) FROM wake) AS notified
        FROM target
        """,
        params,
    )
    row = first_or_not_found(rows, "delivery", delivery_id)
    if not row["updated"]:
        msg = f"delivery {delivery_id} is being sent right now and cannot be retried"
        raise ConflictError(msg)
    return (yield from get_delivery(delivery_id, owner_id=owner_id))


def retry_deliveries(
    *,
    endpoint_id: uuid.UUID | None,
    status: DeliveryStatus | str,
    since: datetime | None,
    owner_id: OwnerFilter,
    channel: str,
) -> Operation[int]:
    """Retry every matching delivery (``failed`` by default); return how many were scheduled."""
    params: dict[str, Any] = {"channel": channel}
    conditions = [owner_condition("e.owner_id", owner_id, params)]
    conditions.extend(_filters(params, endpoint_id, None, status, since))
    if DeliveryStatus(status) is DeliveryStatus.IN_FLIGHT:
        msg = "deliveries that are in flight cannot be retried"
        raise ValidationError(msg)
    rows = yield Query(
        f"""
        WITH updated AS (
            UPDATE {{schema}}.deliveries AS d SET {_RETRY_ASSIGNMENTS}
            FROM {{schema}}.endpoints AS e
            WHERE e.id = d.endpoint_id AND {" AND ".join(conditions)}
            RETURNING d.id
        ), wake AS (
            SELECT pg_notify(CAST(:channel AS text), '') FROM updated LIMIT 1
        )
        SELECT (SELECT count(*) FROM updated) AS updated, (SELECT count(*) FROM wake) AS notified
        """,
        params,
    )
    count: int = rows[0]["updated"]
    return count


def cancel_delivery(delivery_id: int, *, owner_id: OwnerFilter) -> Operation[Delivery]:
    """Give up on a pending delivery."""
    params: dict[str, Any] = {"id": delivery_id}
    owner_sql = owner_condition("e.owner_id", owner_id, params)
    rows = yield Query(
        f"""
        WITH target AS (
            SELECT d.id, d.status FROM {{schema}}.deliveries AS d
            JOIN {{schema}}.endpoints AS e ON e.id = d.endpoint_id
            WHERE d.id = CAST(:id AS bigint) AND {owner_sql}
            FOR UPDATE OF d
        ), updated AS (
            UPDATE {{schema}}.deliveries AS d
            SET status = 'cancelled', finished_at = now(), locked_until = NULL,
                last_error = coalesce(d.last_error, 'cancelled manually'), updated_at = now()
            FROM target
            WHERE d.id = target.id AND target.status = 'pending'
            RETURNING d.id
        )
        SELECT target.status, (SELECT count(*) FROM updated) AS updated FROM target
        """,
        params,
    )
    row = first_or_not_found(rows, "delivery", delivery_id)
    if not row["updated"]:
        msg = f"only pending deliveries can be cancelled; delivery {delivery_id} is {row['status']}"
        raise ConflictError(msg)
    return (yield from get_delivery(delivery_id, owner_id=owner_id))


def _filters(
    params: dict[str, Any],
    endpoint_id: uuid.UUID | None,
    event_id: uuid.UUID | None,
    status: DeliveryStatus | str | None,
    since: datetime | None,
) -> list[str]:
    conditions = []
    if endpoint_id is not None:
        params["endpoint_id"] = str(endpoint_id)
        conditions.append("d.endpoint_id = CAST(:endpoint_id AS uuid)")
    if event_id is not None:
        params["event_id"] = str(event_id)
        conditions.append("d.event_id = CAST(:event_id AS uuid)")
    if status is not None:
        try:
            params["status"] = DeliveryStatus(status).value
        except ValueError:
            allowed = ", ".join(member.value for member in DeliveryStatus)
            msg = f"invalid status {status!r}; expected one of: {allowed}"
            raise ValidationError(msg) from None
        conditions.append("d.status = CAST(:status AS text)")
    if since is not None:
        params["since"] = since
        conditions.append("d.created_at >= CAST(:since AS timestamptz)")
    return conditions
