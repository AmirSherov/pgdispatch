"""Per-endpoint rate limiting without Redis.

Each worker keeps a GCRA (generic cell rate algorithm, an exact token bucket)
per rate-limited endpoint in memory. An endpoint's ``rate_limit_rps`` is split
evenly between the workers that are currently alive, which the workers learn
from their heartbeats. The result is approximate during scale-up and
scale-down, but needs no shared state beyond PostgreSQL.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Hashable

__all__ = ["RateLimiter"]


class RateLimiter:
    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        # Theoretical arrival time of the next request, per key.
        self._next_slot: dict[Hashable, float] = {}

    def reserve(self, key: Hashable, rate: float) -> float:
        """Reserve a slot for one request; return how many seconds to wait before sending.

        Bursts of up to ``max(1, rate)`` requests (one second's worth) pass
        without waiting.
        """
        now = self._clock()
        interval = 1.0 / rate
        burst_allowance = interval * (max(1.0, rate) - 1)
        next_slot = max(self._next_slot.get(key, now), now)
        delay = max(0.0, next_slot - burst_allowance - now)
        self._next_slot[key] = next_slot + interval
        return delay

    def release(self, key: Hashable, rate: float) -> None:
        """Give back a slot reserved with :meth:`reserve` that will not be used."""
        if key in self._next_slot:
            self._next_slot[key] -= 1.0 / rate

    def prune(self) -> None:
        """Forget keys that have been idle long enough to have a full burst again."""
        now = self._clock()
        for key in [key for key, slot in self._next_slot.items() if slot < now]:
            del self._next_slot[key]
