"""Event payload serialization and the body format sent to receivers.

Receivers get the envelope recommended by Standard Webhooks::

    {"type": "payment.paid", "timestamp": "2026-11-01T10:15:00.123Z", "data": {...}}

The body is produced once per attempt and the exact same bytes are signed and
sent.
"""

from __future__ import annotations

import dataclasses
import enum
import json
import os
import time
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from pgdispatch.errors import PayloadTooLargeError, ValidationError

__all__ = ["build_body", "dumps", "serialize_payload", "uuid7"]


def dumps(value: Any) -> str:
    """Serialize to compact JSON, understanding common Python types.

    Supported beyond plain JSON: ``datetime``/``date`` (ISO 8601), ``UUID``,
    ``Decimal`` (as a string, to keep precision), ``Enum``, sets and tuples,
    dataclasses and Pydantic models.
    """
    return json.dumps(
        value,
        default=_default,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    )


def serialize_payload(payload: Any, *, max_bytes: int) -> str:
    """Serialize an event payload, enforcing the size limit and PostgreSQL's ``jsonb`` rules."""
    try:
        text = dumps(payload)
    except (TypeError, ValueError) as exc:
        msg = f"payload is not JSON-serializable: {exc}"
        raise ValidationError(msg) from exc
    if "\\u0000" in text:
        msg = "payload must not contain NUL characters (\\u0000); PostgreSQL jsonb rejects them"
        raise ValidationError(msg)
    size = len(text.encode())
    if size > max_bytes:
        msg = f"payload is {size} bytes, the limit is {max_bytes} (max_payload_bytes)"
        raise PayloadTooLargeError(msg)
    return text


def build_body(event_type: str, created_at: datetime, payload_json: str) -> bytes:
    """Wrap a stored payload in the Standard Webhooks envelope.

    ``payload_json`` is the payload exactly as PostgreSQL returned it, so it is
    embedded verbatim instead of being decoded and encoded again.
    """
    timestamp = created_at.astimezone(timezone.utc).isoformat(timespec="milliseconds")
    timestamp = timestamp.replace("+00:00", "Z")
    head = dumps({"type": event_type, "timestamp": timestamp})
    return f'{head[:-1]},"data":{payload_json}}}'.encode()


def uuid7() -> uuid.UUID:
    """Generate a time-ordered UUID (RFC 9562, version 7).

    Time ordering keeps the events primary-key index compact and lets ids be
    sorted chronologically.
    """
    timestamp_ms = time.time_ns() // 1_000_000
    rand = int.from_bytes(os.urandom(10), "big")
    value = (timestamp_ms & 0xFFFF_FFFF_FFFF) << 80
    value |= 0x7 << 76  # version
    value |= (rand >> 68) << 64  # 12 random bits
    value |= 0b10 << 62  # variant
    value |= rand & 0x3FFF_FFFF_FFFF_FFFF  # 62 random bits
    return uuid.UUID(int=value)


def _default(value: Any) -> Any:  # noqa: PLR0911 - one return per supported type
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, enum.Enum):
        return value.value
    if isinstance(value, (set, frozenset, tuple)):
        return list(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return dataclasses.asdict(value)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return model_dump(mode="json")
    msg = f"Object of type {type(value).__name__} is not JSON serializable"
    raise TypeError(msg)
