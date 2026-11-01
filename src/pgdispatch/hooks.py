"""Callbacks that let the application react to delivery problems.

Register them with decorators::

    @webhooks.hooks.on_endpoint_disabled
    async def notify_customer(endpoint: Endpoint, reason: str) -> None:
        await mailer.send(endpoint.owner_id, f"Your webhook endpoint was disabled: {reason}")

Callbacks may be plain functions or coroutines. They run in the worker after
the state change is committed; an exception in a callback is logged and never
affects delivery.
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from pgdispatch.models import Delivery, Endpoint

__all__ = ["Hooks"]

logger = logging.getLogger("pgdispatch.hooks")

DeliveryFailedHook = Callable[[Delivery], Awaitable[None] | None]
EndpointDisabledHook = Callable[[Endpoint, str], Awaitable[None] | None]
CircuitOpenedHook = Callable[[Endpoint], Awaitable[None] | None]

F = TypeVar("F", bound=Callable[..., Any])


class Hooks:
    """Registry of application callbacks."""

    def __init__(self) -> None:
        self._delivery_failed: list[DeliveryFailedHook] = []
        self._endpoint_disabled: list[EndpointDisabledHook] = []
        self._circuit_opened: list[CircuitOpenedHook] = []

    def on_delivery_failed(self, callback: F) -> F:
        """Called with the :class:`Delivery` when its last attempt fails."""
        self._delivery_failed.append(callback)
        return callback

    def on_endpoint_disabled(self, callback: F) -> F:
        """Called with the :class:`Endpoint` and the reason when the worker disables it."""
        self._endpoint_disabled.append(callback)
        return callback

    def on_circuit_opened(self, callback: F) -> F:
        """Called with the :class:`Endpoint` when its circuit breaker opens."""
        self._circuit_opened.append(callback)
        return callback

    async def delivery_failed(self, delivery: Delivery) -> None:
        for callback in self._delivery_failed:
            await _call(callback, delivery)

    async def endpoint_disabled(self, endpoint: Endpoint, reason: str) -> None:
        for callback in self._endpoint_disabled:
            await _call(callback, endpoint, reason)

    async def circuit_opened(self, endpoint: Endpoint) -> None:
        for callback in self._circuit_opened:
            await _call(callback, endpoint)


async def _call(callback: Callable[..., Any], *args: Any) -> None:
    try:
        result = callback(*args)
        if inspect.isawaitable(result):
            await result
    except Exception:
        logger.exception("pgdispatch hook %s raised an exception", _name(callback))


def _name(callback: Callable[..., Any]) -> str:
    return getattr(callback, "__qualname__", repr(callback))
