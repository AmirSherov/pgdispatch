"""Endpoint management operations."""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta, timezone
from typing import Any

from pgdispatch.backends.base import Operation, Query
from pgdispatch.core.common import (
    ENDPOINT_COLUMNS,
    UNSET,
    OwnerFilter,
    Unset,
    first_or_not_found,
    owner_condition,
    validate_key,
    validate_limit,
)
from pgdispatch.core.matching import validate_pattern
from pgdispatch.core.secrets import SecretStore
from pgdispatch.core.signing import generate_secret, validate_secret
from pgdispatch.errors import ValidationError
from pgdispatch.models import Endpoint
from pgdispatch.security.ssrf import validate_endpoint_url

__all__ = [
    "UrlPolicy",
    "create_endpoint",
    "delete_endpoint",
    "disable_endpoint",
    "enable_endpoint",
    "get_endpoint",
    "get_secret",
    "list_endpoints",
    "rotate_secret",
    "update_endpoint",
]

MAX_DESCRIPTION_LENGTH = 1024
MAX_EVENT_TYPES = 100
MAX_HEADERS = 20

_HEADER_NAME = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]{1,128}$")
_RESERVED_HEADERS = frozenset(
    {
        "connection",
        "content-length",
        "content-type",
        "host",
        "transfer-encoding",
        "user-agent",
        "webhook-id",
        "webhook-signature",
        "webhook-timestamp",
    }
)


class UrlPolicy:
    """Which endpoint URLs are acceptable (see :mod:`pgdispatch.security.ssrf`)."""

    def __init__(self, *, allow_http: bool, allow_private_ips: bool) -> None:
        self.allow_http = allow_http
        self.allow_private_ips = allow_private_ips

    def check(self, url: str) -> str:
        return validate_endpoint_url(
            url, allow_http=self.allow_http, allow_private_ips=self.allow_private_ips
        )


def create_endpoint(
    *,
    url: str,
    event_types: Iterable[str],
    owner_id: str | None,
    description: str | None,
    headers: Mapping[str, str] | None,
    rate_limit_rps: float | None,
    secret: str | None,
    enabled: bool,
    url_policy: UrlPolicy,
    secret_store: SecretStore,
) -> Operation[Endpoint]:
    plaintext = validate_secret(secret) if secret is not None else generate_secret()
    entry = secret_store.seal(plaintext, now=_now())
    rows = yield Query(
        f"""
        INSERT INTO {{schema}}.endpoints AS e
            (id, owner_id, url, description, event_types, secrets, headers, rate_limit_rps,
             is_enabled)
        VALUES (CAST(:id AS uuid), CAST(:owner_id AS text), CAST(:url AS text),
                CAST(:description AS text), CAST(:event_types AS text[]),
                CAST(:secrets AS jsonb), CAST(:headers AS jsonb),
                CAST(:rate_limit_rps AS double precision), CAST(:enabled AS boolean))
        RETURNING {ENDPOINT_COLUMNS}
        """,
        {
            "id": str(uuid.uuid4()),
            "owner_id": validate_key(owner_id, "owner_id") if owner_id is not None else None,
            "url": url_policy.check(url),
            "description": _validate_description(description),
            "event_types": _validate_event_types(event_types),
            "secrets": SecretStore.dump([entry]),
            "headers": _dump_headers(headers or {}),
            "rate_limit_rps": _validate_rate_limit(rate_limit_rps),
            "enabled": enabled,
        },
    )
    return Endpoint.from_row(rows[0], secret=plaintext)


def get_endpoint(endpoint_id: uuid.UUID, *, owner_id: OwnerFilter) -> Operation[Endpoint]:
    params: dict[str, Any] = {"id": str(endpoint_id)}
    owner_sql = owner_condition("e.owner_id", owner_id, params)
    rows = yield Query(
        f"""
        SELECT {ENDPOINT_COLUMNS} FROM {{schema}}.endpoints AS e
        WHERE e.id = CAST(:id AS uuid) AND {owner_sql}
        """,
        params,
    )
    return Endpoint.from_row(first_or_not_found(rows, "endpoint", endpoint_id))


def list_endpoints(
    *,
    owner_id: OwnerFilter,
    enabled: bool | None,
    limit: int,
    after: uuid.UUID | None,
) -> Operation[list[Endpoint]]:
    """List endpoints ordered by id; pass the last id as ``after`` to get the next page."""
    params: dict[str, Any] = {"limit": validate_limit(limit)}
    conditions = [owner_condition("e.owner_id", owner_id, params)]
    if enabled is not None:
        params["enabled"] = enabled
        conditions.append("e.is_enabled = CAST(:enabled AS boolean)")
    if after is not None:
        params["after"] = str(after)
        conditions.append("e.id > CAST(:after AS uuid)")
    rows = yield Query(
        f"""
        SELECT {ENDPOINT_COLUMNS} FROM {{schema}}.endpoints AS e
        WHERE {" AND ".join(conditions)}
        ORDER BY e.id
        LIMIT CAST(:limit AS integer)
        """,
        params,
    )
    return [Endpoint.from_row(row) for row in rows]


def update_endpoint(
    endpoint_id: uuid.UUID,
    *,
    owner_id: OwnerFilter,
    url: str | Unset,
    event_types: Iterable[str] | Unset,
    description: str | Unset | None,
    headers: Mapping[str, str] | Unset,
    rate_limit_rps: float | Unset | None,
    url_policy: UrlPolicy,
) -> Operation[Endpoint]:
    params: dict[str, Any] = {"id": str(endpoint_id)}
    assignments = ["updated_at = now()"]
    if url is not UNSET:
        params["url"] = url_policy.check(url)
        assignments.append("url = CAST(:url AS text)")
    if event_types is not UNSET:
        params["event_types"] = _validate_event_types(event_types)
        assignments.append("event_types = CAST(:event_types AS text[])")
    if description is not UNSET:
        params["description"] = _validate_description(description)
        assignments.append("description = CAST(:description AS text)")
    if headers is not UNSET:
        params["headers"] = _dump_headers(headers)
        assignments.append("headers = CAST(:headers AS jsonb)")
    if rate_limit_rps is not UNSET:
        params["rate_limit_rps"] = _validate_rate_limit(rate_limit_rps)
        assignments.append("rate_limit_rps = CAST(:rate_limit_rps AS double precision)")
    return (yield from _update(endpoint_id, ", ".join(assignments), params, owner_id))


def enable_endpoint(endpoint_id: uuid.UUID, *, owner_id: OwnerFilter) -> Operation[Endpoint]:
    """Enable the endpoint and reset its circuit breaker."""
    assignments = """
        is_enabled = true, disabled_reason = NULL, circuit_state = 'closed',
        consecutive_failures = 0, failing_since = NULL, circuit_opened_at = NULL,
        circuit_retry_at = NULL, circuit_trips = 0, updated_at = now()
    """
    return (yield from _update(endpoint_id, assignments, {"id": str(endpoint_id)}, owner_id))


def disable_endpoint(
    endpoint_id: uuid.UUID, *, reason: str | None, owner_id: OwnerFilter
) -> Operation[Endpoint]:
    """Stop deliveries to the endpoint. Pending deliveries resume when it is enabled again."""
    params: dict[str, Any] = {"id": str(endpoint_id), "reason": reason or "disabled manually"}
    assignments = "is_enabled = false, disabled_reason = CAST(:reason AS text), updated_at = now()"
    return (yield from _update(endpoint_id, assignments, params, owner_id))


def delete_endpoint(endpoint_id: uuid.UUID, *, owner_id: OwnerFilter) -> Operation[None]:
    """Delete the endpoint together with its deliveries and their attempts."""
    params: dict[str, Any] = {"id": str(endpoint_id)}
    owner_sql = owner_condition("e.owner_id", owner_id, params)
    rows = yield Query(
        f"""
        DELETE FROM {{schema}}.endpoints AS e
        WHERE e.id = CAST(:id AS uuid) AND {owner_sql}
        RETURNING e.id
        """,
        params,
    )
    first_or_not_found(rows, "endpoint", endpoint_id)


def rotate_secret(
    endpoint_id: uuid.UUID,
    *,
    grace_period: timedelta,
    secret: str | None,
    owner_id: OwnerFilter,
    secret_store: SecretStore,
) -> Operation[Endpoint]:
    """Replace the signing secret; the old one keeps signing for ``grace_period``."""
    if grace_period < timedelta(0):
        msg = "grace_period must not be negative"
        raise ValidationError(msg)
    new_secret = validate_secret(secret) if secret is not None else generate_secret()
    params: dict[str, Any] = {"id": str(endpoint_id)}
    owner_sql = owner_condition("e.owner_id", owner_id, params)
    rows = yield Query(
        f"""
        SELECT e.secrets::text AS secrets FROM {{schema}}.endpoints AS e
        WHERE e.id = CAST(:id AS uuid) AND {owner_sql}
        FOR UPDATE
        """,
        params,
    )
    current = SecretStore.load(first_or_not_found(rows, "endpoint", endpoint_id)["secrets"])
    rotated = secret_store.rotate(current, new_secret, now=_now(), grace_period=grace_period)
    rows = yield Query(
        f"""
        UPDATE {{schema}}.endpoints AS e
        SET secrets = CAST(:secrets AS jsonb), updated_at = now()
        WHERE e.id = CAST(:id AS uuid)
        RETURNING {ENDPOINT_COLUMNS}
        """,
        {"id": str(endpoint_id), "secrets": SecretStore.dump(rotated)},
    )
    return Endpoint.from_row(rows[0], secret=new_secret)


def get_secret(
    endpoint_id: uuid.UUID, *, owner_id: OwnerFilter, secret_store: SecretStore
) -> Operation[str]:
    """Return the newest signing secret, e.g. for a "reveal secret" button."""
    params: dict[str, Any] = {"id": str(endpoint_id)}
    owner_sql = owner_condition("e.owner_id", owner_id, params)
    rows = yield Query(
        f"""
        SELECT e.secrets::text AS secrets FROM {{schema}}.endpoints AS e
        WHERE e.id = CAST(:id AS uuid) AND {owner_sql}
        """,
        params,
    )
    entries = SecretStore.load(first_or_not_found(rows, "endpoint", endpoint_id)["secrets"])
    return secret_store.open(entries[0])


def _update(
    endpoint_id: uuid.UUID, assignments: str, params: dict[str, Any], owner_id: OwnerFilter
) -> Operation[Endpoint]:
    owner_sql = owner_condition("e.owner_id", owner_id, params)
    rows = yield Query(
        f"""
        UPDATE {{schema}}.endpoints AS e SET {assignments}
        WHERE e.id = CAST(:id AS uuid) AND {owner_sql}
        RETURNING {ENDPOINT_COLUMNS}
        """,
        params,
    )
    return Endpoint.from_row(first_or_not_found(rows, "endpoint", endpoint_id))


def _validate_event_types(event_types: Iterable[str]) -> list[str]:
    if isinstance(event_types, str):
        msg = "event_types must be a list of patterns, e.g. ['payment.*']"
        raise ValidationError(msg)
    patterns = list(dict.fromkeys(validate_pattern(pattern) for pattern in event_types))
    if not patterns:
        msg = "event_types must contain at least one pattern; use ['*'] for all events"
        raise ValidationError(msg)
    if len(patterns) > MAX_EVENT_TYPES:
        msg = f"an endpoint can subscribe to at most {MAX_EVENT_TYPES} patterns"
        raise ValidationError(msg)
    return patterns


def _validate_description(description: str | None) -> str | None:
    if description is not None and len(description) > MAX_DESCRIPTION_LENGTH:
        msg = f"description must be at most {MAX_DESCRIPTION_LENGTH} characters"
        raise ValidationError(msg)
    return description


def _validate_rate_limit(rate_limit_rps: float | None) -> float | None:
    if rate_limit_rps is not None and not rate_limit_rps > 0:
        msg = "rate_limit_rps must be a positive number"
        raise ValidationError(msg)
    return rate_limit_rps


def _dump_headers(headers: Mapping[str, str]) -> str:
    if len(headers) > MAX_HEADERS:
        msg = f"an endpoint can have at most {MAX_HEADERS} custom headers"
        raise ValidationError(msg)
    for name, value in headers.items():
        if not isinstance(name, str) or not _HEADER_NAME.match(name):
            msg = f"invalid header name {name!r}"
            raise ValidationError(msg)
        if name.lower() in _RESERVED_HEADERS:
            msg = f"header {name!r} is set by pgdispatch and cannot be overridden"
            raise ValidationError(msg)
        if not isinstance(value, str) or any(char in value for char in "\r\n\0"):
            msg = f"invalid value for header {name!r}"
            raise ValidationError(msg)
    return json.dumps(dict(headers))


def _now() -> datetime:
    return datetime.now(timezone.utc)
