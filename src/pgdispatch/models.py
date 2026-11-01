"""Immutable records returned by the public API."""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

__all__ = [
    "Attempt",
    "CircuitState",
    "Delivery",
    "DeliveryStatus",
    "Endpoint",
    "Event",
    "PublishResult",
]


class DeliveryStatus(str, Enum):
    """Lifecycle of a delivery."""

    PENDING = "pending"
    """Waiting for its next attempt."""
    IN_FLIGHT = "in_flight"
    """Claimed by a worker that is sending it right now."""
    SUCCEEDED = "succeeded"
    """The endpoint answered with a 2xx status."""
    FAILED = "failed"
    """All attempts were exhausted; can be retried manually."""
    CANCELLED = "cancelled"
    """Abandoned, e.g. because the endpoint answered ``410 Gone``."""

    @property
    def is_final(self) -> bool:
        return self in (DeliveryStatus.SUCCEEDED, DeliveryStatus.FAILED, DeliveryStatus.CANCELLED)


class CircuitState(str, Enum):
    """State of an endpoint's circuit breaker."""

    CLOSED = "closed"
    """Healthy: deliveries flow normally."""
    OPEN = "open"
    """Failing: no deliveries are sent until the cooldown elapses."""
    HALF_OPEN = "half_open"
    """Cooldown elapsed: a single probe delivery decides whether to close again."""


@dataclass(frozen=True, slots=True)
class Endpoint:
    """A receiver URL with its own signing secret and settings."""

    id: uuid.UUID
    url: str
    owner_id: str | None
    description: str | None
    event_types: tuple[str, ...]
    headers: Mapping[str, str]
    rate_limit_rps: float | None
    is_enabled: bool
    disabled_reason: str | None
    circuit_state: CircuitState
    consecutive_failures: int
    failing_since: datetime | None
    created_at: datetime
    updated_at: datetime
    secret: str | None = field(default=None, repr=False)
    """The signing secret. Only populated by ``create``, ``rotate_secret`` and ``get_secret``."""

    @classmethod
    def from_row(cls, row: Mapping[str, Any], *, secret: str | None = None) -> Endpoint:
        return cls(
            id=to_uuid(row["id"]),
            url=row["url"],
            owner_id=row["owner_id"],
            description=row["description"],
            event_types=tuple(row["event_types"]),
            headers=load_json(row["headers"]),
            rate_limit_rps=row["rate_limit_rps"],
            is_enabled=row["is_enabled"],
            disabled_reason=row["disabled_reason"],
            circuit_state=CircuitState(row["circuit_state"]),
            consecutive_failures=row["consecutive_failures"],
            failing_since=row["failing_since"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            secret=secret,
        )


@dataclass(frozen=True, slots=True)
class Event:
    """An immutable fact that happened in your system."""

    id: uuid.UUID
    event_type: str
    owner_id: str | None
    payload: Any
    idempotency_key: str | None
    created_at: datetime

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> Event:
        return cls(
            id=to_uuid(row["id"]),
            event_type=row["event_type"],
            owner_id=row["owner_id"],
            payload=json.loads(row["payload"]),
            idempotency_key=row["idempotency_key"],
            created_at=row["created_at"],
        )


@dataclass(frozen=True, slots=True)
class Delivery:
    """The task of delivering one event to one endpoint."""

    id: int
    event_id: uuid.UUID
    endpoint_id: uuid.UUID
    event_type: str
    ordering_key: str | None
    status: DeliveryStatus
    attempt_count: int
    next_attempt_at: datetime
    last_status_code: int | None
    last_error: str | None
    created_at: datetime
    finished_at: datetime | None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> Delivery:
        return cls(
            id=row["id"],
            event_id=to_uuid(row["event_id"]),
            endpoint_id=to_uuid(row["endpoint_id"]),
            event_type=row["event_type"],
            ordering_key=row["ordering_key"],
            status=DeliveryStatus(row["status"]),
            attempt_count=row["attempt_count"],
            next_attempt_at=row["next_attempt_at"],
            last_status_code=row["last_status_code"],
            last_error=row["last_error"],
            created_at=row["created_at"],
            finished_at=row["finished_at"],
        )


@dataclass(frozen=True, slots=True)
class Attempt:
    """A single HTTP request made for a delivery, with the receiver's answer."""

    id: int
    delivery_id: int
    attempt_number: int
    request_headers: Mapping[str, str]
    response_status: int | None
    response_body: str | None
    duration_ms: int | None
    error: str | None
    created_at: datetime

    @property
    def succeeded(self) -> bool:
        return self.response_status is not None and 200 <= self.response_status < 300

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> Attempt:
        return cls(
            id=row["id"],
            delivery_id=row["delivery_id"],
            attempt_number=row["attempt_number"],
            request_headers=load_json(row["request_headers"]) or {},
            response_status=row["response_status"],
            response_body=row["response_body"],
            duration_ms=row["duration_ms"],
            error=row["error"],
            created_at=row["created_at"],
        )


@dataclass(frozen=True, slots=True)
class PublishResult:
    """Outcome of :meth:`Webhooks.publish <pgdispatch.Webhooks.publish>`."""

    event_id: uuid.UUID
    deliveries: int
    """Number of deliveries created, one per subscribed endpoint."""
    duplicate: bool = False
    """``True`` if an event with the same idempotency key already existed; nothing was created."""


def to_uuid(value: Any) -> uuid.UUID:
    # asyncpg returns its own UUID subclass; normalise for equality and hashing.
    return value if type(value) is uuid.UUID else uuid.UUID(str(value))


def load_json(value: Any) -> Any:
    # Columns are selected as text so every driver behaves the same, but accept
    # already-decoded values too.
    if isinstance(value, (str, bytes)):
        return json.loads(value)
    return value
