"""Event types and the subscription patterns that select them.

An event type is a dot-separated name such as ``payment.paid``. An endpoint
subscribes with patterns:

* ``*`` matches every event type;
* ``payment.paid`` matches exactly that type;
* ``payment.*`` matches every type that starts with ``payment.``
  (``payment.paid``, ``payment.refund.created``), but not ``payment`` itself.

Because wildcards are only allowed as a trailing segment, the set of patterns
that can match a given event type is small and known in advance. Publishing
uses :func:`candidate_patterns` to find subscribers with a single array-overlap
check (``event_types && candidates``), which PostgreSQL can serve from an index.
"""

from __future__ import annotations

import re

from pgdispatch.errors import ValidationError

__all__ = [
    "MAX_EVENT_TYPE_LENGTH",
    "candidate_patterns",
    "matches",
    "validate_event_type",
    "validate_pattern",
]

MAX_EVENT_TYPE_LENGTH = 255
WILDCARD = "*"

_SEGMENT = r"[A-Za-z0-9_\-]+"
_EVENT_TYPE = re.compile(rf"{_SEGMENT}(?:\.{_SEGMENT})*")
_PATTERN = re.compile(rf"\*|{_SEGMENT}(?:\.{_SEGMENT})*(?:\.\*)?")


def validate_event_type(event_type: str) -> str:
    """Return ``event_type`` unchanged or raise :class:`ValidationError`."""
    if len(event_type) > MAX_EVENT_TYPE_LENGTH or not _EVENT_TYPE.fullmatch(event_type):
        msg = (
            f"invalid event type {event_type!r}: use dot-separated segments of letters, "
            "digits, '_' and '-', e.g. 'payment.paid'"
        )
        raise ValidationError(msg)
    return event_type


def validate_pattern(pattern: str) -> str:
    """Return ``pattern`` unchanged or raise :class:`ValidationError`."""
    if len(pattern) > MAX_EVENT_TYPE_LENGTH or not _PATTERN.fullmatch(pattern):
        msg = (
            f"invalid event type pattern {pattern!r}: expected '*', an event type such as "
            "'payment.paid', or a prefix wildcard such as 'payment.*'"
        )
        raise ValidationError(msg)
    return pattern


def candidate_patterns(event_type: str) -> list[str]:
    """List every pattern that matches ``event_type``.

    >>> candidate_patterns("payment.refund.created")
    ['*', 'payment.refund.created', 'payment.*', 'payment.refund.*']
    """
    segments = event_type.split(".")
    prefixes = [".".join(segments[:size]) + ".*" for size in range(1, len(segments))]
    return [WILDCARD, event_type, *prefixes]


def matches(pattern: str, event_type: str) -> bool:
    """Tell whether ``pattern`` selects ``event_type``."""
    if pattern in (WILDCARD, event_type):
        return True
    return pattern.endswith(".*") and event_type.startswith(pattern[:-1])
