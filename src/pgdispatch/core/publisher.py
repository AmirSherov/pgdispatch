"""Publishing events: the transactional outbox.

:func:`publish` runs inside the caller's transaction and, in one statement,

1. inserts the event (skipping it if the idempotency key is already known),
2. fans it out into one ``deliveries`` row per subscribed endpoint, and
3. queues a ``NOTIFY`` for the workers.

PostgreSQL delivers notifications only when the transaction commits, so a
rolled-back transaction leaves no event, no deliveries and no wake-up.
Subscriptions are resolved at publish time: an endpoint created later does not
receive earlier events (use replay for that).
"""

from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any

from pgdispatch.backends.base import Operation, Query
from pgdispatch.core.common import (
    OwnerFilter,
    first_or_not_found,
    owner_condition,
    validate_key,
)
from pgdispatch.core.matching import candidate_patterns, validate_event_type
from pgdispatch.core.payload import serialize_payload, uuid7
from pgdispatch.errors import ConflictError, NotFoundError
from pgdispatch.models import PublishResult, to_uuid

__all__ = ["TEST_EVENT_TYPE", "publish", "replay", "send_test"]

TEST_EVENT_TYPE = "webhook.test"

_FANOUT_TARGETS = """
    FROM {schema}.endpoints AS e
    WHERE e.is_enabled
      AND {owner_match}
      AND e.event_types && CAST(:patterns AS text[])
"""

_PUBLISH = """
WITH new_event AS (
    INSERT INTO {schema}.events (id, event_type, owner_id, payload, idempotency_key)
    VALUES (CAST(:event_id AS uuid), CAST(:event_type AS text), CAST(:event_owner AS text),
            CAST(:payload AS jsonb), CAST(:idempotency_key AS text))
    ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
    RETURNING id
), fanout AS (
    INSERT INTO {schema}.deliveries (event_id, endpoint_id, ordering_key, next_attempt_at)
    SELECT new_event.id, e.id, CAST(:ordering_key AS text),
           now() + CAST(:initial_delay AS double precision) * interval '1 second'
    FROM new_event, ({targets}) AS e
    RETURNING 1
), wake AS (
    SELECT pg_notify(CAST(:channel AS text), '') FROM fanout LIMIT 1
)
SELECT (SELECT count(*) FROM new_event) AS inserted,
       (SELECT count(*) FROM fanout) AS deliveries,
       (SELECT count(*) FROM wake) AS notified
"""


def publish(
    *,
    event_type: str,
    payload: Any,
    owner_id: str | None,
    ordering_key: str | None,
    idempotency_key: str | None,
    channel: str,
    max_payload_bytes: int,
    initial_delay: timedelta = timedelta(0),
) -> Operation[PublishResult]:
    validate_event_type(event_type)
    params: dict[str, Any] = {
        "event_id": str(uuid7()),
        "event_type": event_type,
        "event_owner": validate_key(owner_id, "owner_id") if owner_id is not None else None,
        "payload": serialize_payload(payload, max_bytes=max_payload_bytes),
        "ordering_key": validate_key(ordering_key, "ordering_key") if ordering_key else None,
        "idempotency_key": (
            validate_key(idempotency_key, "idempotency_key") if idempotency_key else None
        ),
        "patterns": candidate_patterns(event_type),
        "channel": channel,
        "initial_delay": initial_delay.total_seconds(),
    }
    sql = _PUBLISH.replace("{targets}", "SELECT e.id " + _fanout_targets(owner_id, params))
    rows = yield Query(sql, params)
    row = rows[0]
    if row["inserted"]:
        return PublishResult(event_id=uuid.UUID(params["event_id"]), deliveries=row["deliveries"])

    # The idempotency key exists. In READ COMMITTED this statement sees rows
    # committed after the previous one started, so a concurrent duplicate is found.
    rows = yield Query(
        "SELECT id FROM {schema}.events WHERE idempotency_key = CAST(:key AS text)",
        {"key": idempotency_key},
    )
    if not rows:  # pragma: no cover - only under REPEATABLE READ with a concurrent publisher
        msg = f"an event with idempotency key {idempotency_key!r} is being published concurrently"
        raise ConflictError(msg)
    return PublishResult(event_id=to_uuid(rows[0]["id"]), deliveries=0, duplicate=True)


def replay(
    event_id: uuid.UUID,
    *,
    endpoint_id: uuid.UUID | None,
    owner_id: OwnerFilter,
    channel: str,
) -> Operation[list[int]]:
    """Deliver an existing event again; return the ids of the new deliveries.

    Without ``endpoint_id`` the event goes to every enabled endpoint that is
    subscribed *now*, which is how you backfill a newly created endpoint.
    Replays bypass ``ordering_key`` so they never wait behind live traffic.
    """
    params: dict[str, Any] = {"event_id": str(event_id), "channel": channel}
    owner_sql = owner_condition("ev.owner_id", owner_id, params)
    rows = yield Query(
        f"""
        SELECT ev.id, ev.event_type, ev.owner_id FROM {{schema}}.events AS ev
        WHERE ev.id = CAST(:event_id AS uuid) AND {owner_sql}
        """,
        params,
    )
    event = first_or_not_found(rows, "event", event_id)

    insert_params: dict[str, Any] = {"event_id": str(event_id), "channel": channel}
    if endpoint_id is not None:
        # Only endpoints of the event's owner may receive it: never leak across tenants.
        insert_params["endpoint_id"] = str(endpoint_id)
        same_owner = _owner_match(event["owner_id"], insert_params, column="e.owner_id")
        targets = f"""
            SELECT e.id FROM {{schema}}.endpoints AS e
            WHERE e.id = CAST(:endpoint_id AS uuid) AND {same_owner}
        """
    else:
        insert_params["patterns"] = candidate_patterns(event["event_type"])
        targets = "SELECT e.id " + _fanout_targets(event["owner_id"], insert_params)

    rows = yield Query(
        f"""
        WITH created AS (
            INSERT INTO {{schema}}.deliveries (event_id, endpoint_id)
            SELECT CAST(:event_id AS uuid), targets.id FROM ({targets}) AS targets
            RETURNING id
        ), wake AS (
            SELECT pg_notify(CAST(:channel AS text), '') FROM created LIMIT 1
        )
        SELECT id, (SELECT count(*) FROM wake) AS notified FROM created ORDER BY id
        """,
        insert_params,
    )
    if endpoint_id is not None and not rows:
        msg = f"endpoint {endpoint_id} not found for the owner of event {event_id}"
        raise NotFoundError(msg)
    return [row["id"] for row in rows]


def send_test(
    endpoint_id: uuid.UUID,
    *,
    owner_id: OwnerFilter,
    channel: str,
) -> Operation[PublishResult]:
    """Send a ``webhook.test`` event to one endpoint, regardless of its subscriptions."""
    params: dict[str, Any] = {"endpoint_id": str(endpoint_id)}
    owner_sql = owner_condition("e.owner_id", owner_id, params)
    rows = yield Query(
        f"""
        SELECT e.id, e.owner_id, e.is_enabled FROM {{schema}}.endpoints AS e
        WHERE e.id = CAST(:endpoint_id AS uuid) AND {owner_sql}
        FOR SHARE
        """,
        params,
    )
    endpoint = first_or_not_found(rows, "endpoint", endpoint_id)
    if not endpoint["is_enabled"]:
        msg = f"endpoint {endpoint_id} is disabled; enable it before sending a test event"
        raise ConflictError(msg)

    event_id = uuid7()
    payload = {
        "message": "This is a test event sent by pgdispatch.",
        "endpoint_id": str(endpoint_id),
    }
    yield Query(
        """
        WITH new_event AS (
            INSERT INTO {schema}.events (id, event_type, owner_id, payload)
            VALUES (CAST(:event_id AS uuid), CAST(:event_type AS text),
                    CAST(:owner_id AS text), CAST(:payload AS jsonb))
            RETURNING id
        ), delivery AS (
            INSERT INTO {schema}.deliveries (event_id, endpoint_id)
            SELECT new_event.id, CAST(:endpoint_id AS uuid) FROM new_event
            RETURNING 1
        ), wake AS (
            SELECT pg_notify(CAST(:channel AS text), '') FROM delivery LIMIT 1
        )
        SELECT (SELECT count(*) FROM delivery) AS deliveries,
               (SELECT count(*) FROM wake) AS notified
        """,
        {
            "event_id": str(event_id),
            "event_type": TEST_EVENT_TYPE,
            "owner_id": endpoint["owner_id"],
            "payload": serialize_payload(payload, max_bytes=1024),
            "endpoint_id": str(endpoint_id),
            "channel": channel,
        },
    )
    return PublishResult(event_id=event_id, deliveries=1)


def _fanout_targets(owner_id: str | None, params: dict[str, Any]) -> str:
    owner_match = _owner_match(owner_id, params, column="e.owner_id")
    return _FANOUT_TARGETS.replace("{owner_match}", owner_match)


def _owner_match(owner_id: str | None, params: dict[str, Any], *, column: str) -> str:
    # Events are only ever delivered to endpoints of the same owner (or, for
    # events without an owner, to endpoints without an owner).
    if owner_id is None:
        return f"{column} IS NULL"
    params["target_owner"] = owner_id
    return f"{column} = CAST(:target_owner AS text)"
