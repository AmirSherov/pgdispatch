"""Per-endpoint circuit breaker.

The breaker protects the worker (and the receiver) from hammering an endpoint
that is down::

    closed ──(failure_threshold failures in a row)──▶ open
    open ──(cooldown elapsed, a worker claims a probe)──▶ half_open
    half_open ──(probe succeeds)──▶ closed
    half_open ──(probe fails)──▶ open, with a longer cooldown

While a breaker is open its deliveries are not claimed at all, so they keep
their remaining attempts. The cooldown doubles with every consecutive trip, up
to ``max_cooldown``. Independently of the breaker, an endpoint that has been
failing continuously for ``auto_disable_after`` is disabled.

This module is pure: it computes the next state, and the worker persists it.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta

from pgdispatch.models import CircuitState

__all__ = ["BreakerPolicy", "BreakerState", "BreakerUpdate", "record_failure", "record_success"]


@dataclass(frozen=True, slots=True)
class BreakerPolicy:
    failure_threshold: int
    cooldown: timedelta
    max_cooldown: timedelta
    auto_disable_after: timedelta | None

    def cooldown_for(self, trips: int) -> timedelta:
        """Cooldown after the ``trips``-th consecutive opening (1-based)."""
        # Cap the exponent: the cooldown hits max_cooldown long before 2**20.
        factor: int = 2 ** min(trips - 1, 20)
        return min(self.cooldown * factor, self.max_cooldown)


@dataclass(frozen=True, slots=True)
class BreakerState:
    state: CircuitState = CircuitState.CLOSED
    consecutive_failures: int = 0
    failing_since: datetime | None = None
    opened_at: datetime | None = None
    retry_at: datetime | None = None
    trips: int = 0


@dataclass(frozen=True, slots=True)
class BreakerUpdate:
    state: BreakerState
    opened: bool = False
    """The breaker transitioned to ``open`` with this result."""
    disable: bool = False
    """The endpoint has been failing for longer than ``auto_disable_after``."""


def record_success(state: BreakerState) -> BreakerUpdate:
    """Any successful delivery proves the endpoint works and closes the breaker."""
    del state  # the outcome does not depend on the previous state
    return BreakerUpdate(BreakerState())


def record_failure(state: BreakerState, *, now: datetime, policy: BreakerPolicy) -> BreakerUpdate:
    """Account for a failed attempt."""
    failures = state.consecutive_failures + 1
    failing_since = state.failing_since or now
    disable = (
        policy.auto_disable_after is not None and now - failing_since >= policy.auto_disable_after
    )

    should_open = state.state is CircuitState.HALF_OPEN or (
        state.state is CircuitState.CLOSED and failures >= policy.failure_threshold
    )
    if not should_open:
        # Closed below the threshold, or already open (a request that was in
        # flight when the breaker opened): keep the current schedule.
        next_state = replace(state, consecutive_failures=failures, failing_since=failing_since)
        return BreakerUpdate(next_state, disable=disable)

    trips = state.trips + 1
    next_state = BreakerState(
        state=CircuitState.OPEN,
        consecutive_failures=failures,
        failing_since=failing_since,
        opened_at=now,
        retry_at=now + policy.cooldown_for(trips),
        trips=trips,
    )
    return BreakerUpdate(next_state, opened=True, disable=disable)
