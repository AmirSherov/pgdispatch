"""Library configuration.

All settings live in one immutable :class:`WebhooksConfig`. Values come from, in
order of precedence: keyword arguments, environment variables prefixed with
``PGDISPATCH_`` (``PGDISPATCH_CONCURRENCY=100``), and the defaults below. Django
projects can also use ``settings.PGDISPATCH``.
"""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Any

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from pgdispatch._version import __version__
from pgdispatch.core.retry import DEFAULT_RETRY_SCHEDULE

_IDENTIFIER = re.compile(r"^[a-z_][a-z0-9_]{0,62}$")
_NUMBER = re.compile(r"^\d+(\.\d+)?$")
_DURATION_PART = re.compile(r"(\d+)(ms|s|m|h|d|w)")
_DURATION_UNITS = {
    "ms": "milliseconds",
    "s": "seconds",
    "m": "minutes",
    "h": "hours",
    "d": "days",
    "w": "weeks",
}
_DURATION_FIELDS = (
    "cleanup_interval",
    "poll_interval",
    "lease_duration",
    "heartbeat_interval",
    "shutdown_timeout",
    "request_timeout",
    "circuit_cooldown",
    "circuit_max_cooldown",
    "auto_disable_after",
)


def parse_duration(value: str) -> timedelta:
    """Parse a duration: ``"30"`` and ``"0.5"`` are seconds; units: ``ms s m h d w``.

    Examples: ``"250ms"``, ``"30s"``, ``"5m"``, ``"1h30m"``, ``"14d"``.
    """
    text = value.strip()
    if _NUMBER.match(text):
        return timedelta(seconds=float(text))
    parts = _DURATION_PART.findall(text)
    if not parts or "".join(number + unit for number, unit in parts) != text:
        msg = f"invalid duration {value!r}; use seconds or e.g. 500ms, 30s, 5m, 1h30m, 14d"
        raise ValueError(msg)
    return sum((timedelta(**{_DURATION_UNITS[unit]: int(n)}) for n, unit in parts), timedelta())


def _coerce_duration(value: Any) -> Any:
    # ISO 8601 strings ("PT5M") are left to pydantic.
    if isinstance(value, str) and not value.strip().upper().startswith("P"):
        return parse_duration(value)
    return value


class WebhooksConfig(BaseSettings):
    """Tunable behaviour of publishing, delivery and housekeeping.

    The defaults are production-ready; most applications never change them.
    Durations accept :class:`~datetime.timedelta`, a number of seconds, a
    short string such as ``"500ms"``, ``"30s"``, ``"5m"``, ``"1h30m"`` or
    ``"14d"``, or an ISO 8601 duration (``"PT5M"``).
    """

    model_config = SettingsConfigDict(
        env_prefix="PGDISPATCH_",
        frozen=True,
        extra="forbid",
    )

    # --- storage -------------------------------------------------------------
    db_schema: str = Field(
        default="pgdispatch",
        description="PostgreSQL schema that holds all pgdispatch tables.",
    )
    retention_days: int = Field(
        default=14,
        ge=1,
        description="How long events, deliveries and attempts are kept.",
    )
    cleanup_interval: timedelta = Field(
        default=timedelta(minutes=10),
        description="How often a worker purges data older than the retention period.",
    )
    cleanup_batch_size: int = Field(default=5_000, ge=1)

    # --- publishing ----------------------------------------------------------
    max_payload_bytes: int = Field(
        default=256 * 1024,
        ge=1,
        description="Upper bound for the JSON-serialized event payload.",
    )

    # --- worker --------------------------------------------------------------
    concurrency: int = Field(default=50, ge=1, description="Simultaneous HTTP requests per worker.")
    endpoint_concurrency: int = Field(
        default=10,
        ge=1,
        description="Simultaneous HTTP requests per worker to a single endpoint.",
    )
    batch_size: int = Field(default=100, ge=1, description="Deliveries claimed per database query.")
    poll_interval: timedelta = Field(
        default=timedelta(seconds=1),
        description="Fallback polling period in case a NOTIFY wake-up is missed.",
    )
    lease_duration: timedelta = Field(
        default=timedelta(seconds=30),
        description="How long a claimed delivery stays reserved for a worker.",
    )
    heartbeat_interval: timedelta = Field(default=timedelta(seconds=5))
    shutdown_timeout: timedelta = Field(
        default=timedelta(seconds=25),
        description="Grace period for in-flight requests when the worker stops.",
    )

    # --- HTTP ----------------------------------------------------------------
    request_timeout: timedelta = Field(default=timedelta(seconds=15))
    user_agent: str = Field(default=f"pgdispatch/{__version__}")
    store_response_body: bool = Field(default=True)
    max_response_body_bytes: int = Field(default=4096, ge=0)
    outbound_proxy: str | None = Field(
        default=None,
        description="Send all requests through this proxy; SSRF filtering is then the proxy's job.",
    )

    # --- retries -------------------------------------------------------------
    retry_schedule: tuple[timedelta, ...] = Field(
        default=DEFAULT_RETRY_SCHEDULE,
        min_length=1,
        description="Delay before each attempt; its length is the maximum number of attempts.",
    )
    retry_jitter: float = Field(default=0.2, ge=0.0, lt=1.0)

    # --- circuit breaker -----------------------------------------------------
    failure_threshold: int = Field(default=10, ge=1)
    circuit_cooldown: timedelta = Field(default=timedelta(minutes=5))
    circuit_max_cooldown: timedelta = Field(default=timedelta(hours=1))
    auto_disable_after: timedelta | None = Field(
        default=timedelta(days=5),
        description="Disable an endpoint that has been failing continuously for this long.",
    )

    # --- security ------------------------------------------------------------
    allow_http: bool = Field(default=False, description="Allow plain-HTTP endpoints (development).")
    allow_private_ips: bool = Field(
        default=False,
        description="Disable SSRF protection. Never enable this in production.",
    )
    secret_encryption_keys: tuple[SecretStr, ...] = Field(
        default=(),
        description="Fernet keys used to encrypt signing secrets at rest; the first one encrypts.",
    )

    @field_validator(*_DURATION_FIELDS, mode="before")
    @classmethod
    def _parse_durations(cls, value: Any) -> Any:
        return _coerce_duration(value)

    @field_validator("retry_schedule", mode="before")
    @classmethod
    def _parse_schedule(cls, value: Any) -> Any:
        if isinstance(value, (list, tuple)):
            return [_coerce_duration(item) for item in value]
        return value

    @field_validator("db_schema")
    @classmethod
    def _check_schema(cls, value: str) -> str:
        if not _IDENTIFIER.match(value):
            msg = "db_schema must be a lowercase SQL identifier, e.g. 'pgdispatch'"
            raise ValueError(msg)
        return value

    @field_validator("retry_schedule")
    @classmethod
    def _check_retry_schedule(cls, value: tuple[timedelta, ...]) -> tuple[timedelta, ...]:
        if any(delay < timedelta(0) for delay in value):
            msg = "retry_schedule delays must not be negative"
            raise ValueError(msg)
        return value

    @model_validator(mode="after")
    def _check_timeouts(self) -> WebhooksConfig:
        positive = {
            "poll_interval": self.poll_interval,
            "lease_duration": self.lease_duration,
            "heartbeat_interval": self.heartbeat_interval,
            "request_timeout": self.request_timeout,
            "circuit_cooldown": self.circuit_cooldown,
            "cleanup_interval": self.cleanup_interval,
        }
        for name, value in positive.items():
            if value <= timedelta(0):
                msg = f"{name} must be positive"
                raise ValueError(msg)
        if self.lease_duration <= self.request_timeout:
            msg = "lease_duration must be longer than request_timeout"
            raise ValueError(msg)
        if self.circuit_max_cooldown < self.circuit_cooldown:
            msg = "circuit_max_cooldown must not be shorter than circuit_cooldown"
            raise ValueError(msg)
        return self

    @property
    def notify_channel(self) -> str:
        """``LISTEN``/``NOTIFY`` channel used to wake workers up."""
        return f"{self.db_schema}_new"

    @property
    def max_attempts(self) -> int:
        """Number of attempts before a delivery is marked as failed."""
        return len(self.retry_schedule)
