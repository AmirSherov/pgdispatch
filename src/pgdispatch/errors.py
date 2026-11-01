"""Exceptions raised by pgdispatch.

Every exception derives from :class:`PgDispatchError`, so callers can catch the
whole family with a single ``except`` clause.
"""

from __future__ import annotations


class PgDispatchError(Exception):
    """Base class for all pgdispatch errors."""


class ConfigurationError(PgDispatchError):
    """The library is configured inconsistently or an optional dependency is missing."""


class ValidationError(PgDispatchError, ValueError):
    """An argument passed to the public API is invalid."""


class PayloadTooLargeError(ValidationError):
    """The serialized event payload exceeds ``max_payload_bytes``."""


class UnsafeURLError(ValidationError):
    """An endpoint URL is not allowed, e.g. it points to a private network."""


class NotFoundError(PgDispatchError, LookupError):
    """The requested endpoint, event or delivery does not exist (or is not visible to the owner)."""


class ConflictError(PgDispatchError):
    """The operation cannot be applied in the object's current state."""


class InvalidSignatureError(PgDispatchError):
    """A webhook signature could not be verified."""


class MigrationError(PgDispatchError):
    """The database schema could not be migrated."""
