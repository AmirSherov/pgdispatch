"""Building blocks shared by the database operations."""

from __future__ import annotations

import enum
import uuid
from typing import Any, Final

from pgdispatch.backends.base import Row
from pgdispatch.errors import NotFoundError, ValidationError

__all__ = [
    "DELIVERY_SELECT",
    "ENDPOINT_COLUMNS",
    "UNSET",
    "OwnerFilter",
    "Unset",
    "first_or_not_found",
    "owner_condition",
    "validate_key",
    "validate_limit",
]

MAX_KEY_LENGTH = 255
MAX_PAGE_SIZE = 1000


class Unset(enum.Enum):
    """Marker for "argument not given", distinct from an explicit ``None``."""

    UNSET = "UNSET"

    def __repr__(self) -> str:
        return "UNSET"


UNSET: Final = Unset.UNSET

OwnerFilter = str | None | Unset
"""``UNSET`` (default) ignores ownership; ``None`` or a string restricts results to that owner."""


ENDPOINT_COLUMNS = """
    e.id, e.owner_id, e.url, e.description, e.event_types, e.headers::text AS headers,
    e.rate_limit_rps, e.is_enabled, e.disabled_reason, e.circuit_state,
    e.consecutive_failures, e.failing_since, e.created_at, e.updated_at
"""

DELIVERY_SELECT = """
    SELECT d.id, d.event_id, d.endpoint_id, ev.event_type, d.ordering_key, d.status,
           d.attempt_count, d.next_attempt_at, d.last_status_code, d.last_error,
           d.created_at, d.finished_at
    FROM {schema}.deliveries AS d
    JOIN {schema}.events AS ev ON ev.id = d.event_id
    JOIN {schema}.endpoints AS e ON e.id = d.endpoint_id
"""


def owner_condition(column: str, owner_id: OwnerFilter, params: dict[str, Any]) -> str:
    """SQL condition restricting ``column`` to ``owner_id``; adds the bound parameter."""
    if owner_id is UNSET:
        return "TRUE"
    if owner_id is None:
        return f"{column} IS NULL"
    params["owner_id"] = validate_key(owner_id, "owner_id")
    return f"{column} = CAST(:owner_id AS text)"


def validate_key(value: str, name: str) -> str:
    if not isinstance(value, str) or not value or len(value) > MAX_KEY_LENGTH:
        msg = f"{name} must be a non-empty string of at most {MAX_KEY_LENGTH} characters"
        raise ValidationError(msg)
    return value


def validate_limit(limit: int) -> int:
    if not 1 <= limit <= MAX_PAGE_SIZE:
        msg = f"limit must be between 1 and {MAX_PAGE_SIZE}"
        raise ValidationError(msg)
    return limit


def first_or_not_found(rows: list[Row], what: str, identifier: uuid.UUID | int | str) -> Row:
    if not rows:
        msg = f"{what} {identifier} not found"
        raise NotFoundError(msg)
    return rows[0]
