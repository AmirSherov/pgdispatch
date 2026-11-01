"""Reading published events."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from pgdispatch.backends.base import Operation, Query
from pgdispatch.core.common import OwnerFilter, first_or_not_found, owner_condition, validate_limit
from pgdispatch.core.matching import validate_event_type
from pgdispatch.models import Event

__all__ = ["get_event", "list_events"]

_EVENT_COLUMNS = (
    "ev.id, ev.event_type, ev.owner_id, ev.payload::text AS payload, "
    "ev.idempotency_key, ev.created_at"
)


def get_event(event_id: uuid.UUID, *, owner_id: OwnerFilter) -> Operation[Event]:
    params: dict[str, Any] = {"id": str(event_id)}
    owner_sql = owner_condition("ev.owner_id", owner_id, params)
    rows = yield Query(
        f"""
        SELECT {_EVENT_COLUMNS} FROM {{schema}}.events AS ev
        WHERE ev.id = CAST(:id AS uuid) AND {owner_sql}
        """,
        params,
    )
    return Event.from_row(first_or_not_found(rows, "event", event_id))


def list_events(
    *,
    owner_id: OwnerFilter,
    event_type: str | None,
    since: datetime | None,
    before: uuid.UUID | None,
    limit: int,
) -> Operation[list[Event]]:
    """Newest events first; pass the last id as ``before`` to get the next page."""
    params: dict[str, Any] = {"limit": validate_limit(limit)}
    conditions = [owner_condition("ev.owner_id", owner_id, params)]
    if event_type is not None:
        params["event_type"] = validate_event_type(event_type)
        conditions.append("ev.event_type = CAST(:event_type AS text)")
    if since is not None:
        params["since"] = since
        conditions.append("ev.created_at >= CAST(:since AS timestamptz)")
    if before is not None:
        # Event ids are UUIDv7, so they sort chronologically.
        params["before"] = str(before)
        conditions.append("ev.id < CAST(:before AS uuid)")
    rows = yield Query(
        f"""
        SELECT {_EVENT_COLUMNS} FROM {{schema}}.events AS ev
        WHERE {" AND ".join(conditions)}
        ORDER BY ev.id DESC
        LIMIT CAST(:limit AS integer)
        """,
        params,
    )
    return [Event.from_row(row) for row in rows]
