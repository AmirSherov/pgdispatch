"""Retry schedule, jitter and ``Retry-After`` handling."""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

__all__ = [
    "DEFAULT_RETRY_SCHEDULE",
    "exponential_backoff",
    "next_retry_delay",
    "parse_retry_after",
]

#: Eight attempts over roughly 27 hours, the schedule popularised by Stripe and Svix.
#: Element *n* is the delay before attempt *n + 1*; the first attempt is immediate.
DEFAULT_RETRY_SCHEDULE: tuple[timedelta, ...] = (
    timedelta(0),
    timedelta(seconds=5),
    timedelta(minutes=5),
    timedelta(minutes=30),
    timedelta(hours=2),
    timedelta(hours=5),
    timedelta(hours=10),
    timedelta(hours=10),
)


def exponential_backoff(
    attempts: int,
    *,
    initial: timedelta = timedelta(seconds=5),
    factor: float = 2.0,
    max_delay: timedelta = timedelta(hours=10),
) -> tuple[timedelta, ...]:
    """Build a retry schedule whose delays grow geometrically.

    ``exponential_backoff(4, initial=timedelta(seconds=10))`` yields delays of
    0 s, 10 s, 20 s and 40 s. The result can be passed as ``retry_schedule``.
    """
    if attempts < 1:
        msg = "attempts must be at least 1"
        raise ValueError(msg)
    if factor < 1:
        msg = "factor must be at least 1"
        raise ValueError(msg)
    delays = [timedelta(0)]
    for retry in range(attempts - 1):
        delays.append(min(initial * factor**retry, max_delay))
    return tuple(delays)


def next_retry_delay(
    schedule: tuple[timedelta, ...],
    attempts_made: int,
    *,
    jitter: float = 0.0,
    rng: random.Random | None = None,
) -> timedelta | None:
    """Return the delay before the next attempt, or ``None`` when attempts are exhausted.

    ``attempts_made`` counts the attempts already performed in the current retry
    round. A random spread of ``±jitter`` is applied so that retries for many
    deliveries that failed together do not hit the receiver at the same instant.
    """
    if attempts_made >= len(schedule):
        return None
    delay = schedule[attempts_made]
    if jitter and delay:
        spread = (rng or random).uniform(1 - jitter, 1 + jitter)
        delay = delay * spread
    return delay


def parse_retry_after(value: str | None, *, now: datetime | None = None) -> timedelta | None:
    """Parse an HTTP ``Retry-After`` header (delta-seconds or HTTP-date)."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return timedelta(seconds=int(value))
    try:
        moment = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    delay = moment - (now or datetime.now(timezone.utc))
    return max(delay, timedelta(0))
