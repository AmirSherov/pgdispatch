"""pgdispatch: reliable webhook delivery using only the PostgreSQL you already have.

Publish events inside your database transaction, and a background worker
delivers them to every subscribed endpoint, signed according to Standard
Webhooks, with retries, circuit breaking and a full attempt log.
"""

from pgdispatch._version import __version__
from pgdispatch.backends.asyncpg import AsyncpgBackend
from pgdispatch.client import Webhooks
from pgdispatch.config import WebhooksConfig
from pgdispatch.core.common import UNSET
from pgdispatch.core.maintenance import QueueStats
from pgdispatch.core.retry import DEFAULT_RETRY_SCHEDULE, exponential_backoff
from pgdispatch.core.signing import generate_secret
from pgdispatch.core.signing import verify as verify_signature
from pgdispatch.errors import (
    ConfigurationError,
    ConflictError,
    InvalidSignatureError,
    MigrationError,
    NotFoundError,
    PayloadTooLargeError,
    PgDispatchError,
    UnsafeURLError,
    ValidationError,
)
from pgdispatch.hooks import Hooks
from pgdispatch.models import (
    Attempt,
    CircuitState,
    Delivery,
    DeliveryStatus,
    Endpoint,
    Event,
    PublishResult,
)
from pgdispatch.worker.worker import Worker

__all__ = [
    "DEFAULT_RETRY_SCHEDULE",
    "UNSET",
    "AsyncpgBackend",
    "Attempt",
    "CircuitState",
    "ConfigurationError",
    "ConflictError",
    "Delivery",
    "DeliveryStatus",
    "Endpoint",
    "Event",
    "Hooks",
    "InvalidSignatureError",
    "MigrationError",
    "NotFoundError",
    "PayloadTooLargeError",
    "PgDispatchError",
    "PublishResult",
    "QueueStats",
    "UnsafeURLError",
    "ValidationError",
    "Webhooks",
    "WebhooksConfig",
    "Worker",
    "__version__",
    "exponential_backoff",
    "generate_secret",
    "verify_signature",
]
