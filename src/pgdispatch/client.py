"""The public, asynchronous entry point: :class:`Webhooks`."""

from __future__ import annotations

import asyncio
import builtins
import contextlib
import logging
import uuid
from collections.abc import AsyncIterator, Callable, Iterable, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, TypeVar

from prometheus_client import CollectorRegistry

from pgdispatch.backends.base import Backend, Operation, run_async
from pgdispatch.config import WebhooksConfig
from pgdispatch.core import deliveries as delivery_ops
from pgdispatch.core import endpoints as endpoint_ops
from pgdispatch.core import events as event_ops
from pgdispatch.core import publisher
from pgdispatch.core.common import UNSET, OwnerFilter, Unset
from pgdispatch.core.maintenance import QueueStats, delete_expired_events, queue_stats
from pgdispatch.core.secrets import SecretCipher, SecretStore, fernet_cipher
from pgdispatch.hooks import Hooks
from pgdispatch.metrics import get_metrics
from pgdispatch.migrations import migrate
from pgdispatch.models import Attempt, Delivery, DeliveryStatus, Endpoint, Event, PublishResult
from pgdispatch.worker.worker import Worker

__all__ = ["DeliveriesAPI", "EndpointsAPI", "EventsAPI", "Webhooks"]

logger = logging.getLogger("pgdispatch")

T = TypeVar("T")


class Webhooks:
    """Publish events and manage endpoints and deliveries.

    ::

        from pgdispatch import Webhooks
        from pgdispatch.sqlalchemy import SQLAlchemyBackend

        webhooks = Webhooks(SQLAlchemyBackend(engine))

        async with session.begin():
            session.add(payment)
            await webhooks.publish(session, "payment.paid", {"id": payment.id})

    Every method that takes ``owner_id`` uses it to scope the operation to one
    tenant: objects of other owners are reported as not found. Leave it out in
    trusted, internal code to operate on all owners.
    """

    def __init__(
        self,
        backend: Backend,
        config: WebhooksConfig | None = None,
        *,
        hooks: Hooks | None = None,
        secret_cipher: SecretCipher | None = None,
        metrics_registry: CollectorRegistry | None = None,
    ) -> None:
        self.backend = backend
        self.config = config or WebhooksConfig()
        self.hooks = hooks or Hooks()
        if secret_cipher is None and self.config.secret_encryption_keys:
            keys = [key.get_secret_value() for key in self.config.secret_encryption_keys]
            secret_cipher = fernet_cipher(keys)
        self._secrets = SecretStore(secret_cipher)
        self._metrics = get_metrics(metrics_registry)
        self._url_policy = endpoint_ops.UrlPolicy(
            allow_http=self.config.allow_http, allow_private_ips=self.config.allow_private_ips
        )
        self.endpoints = EndpointsAPI(self)
        self.deliveries = DeliveriesAPI(self)
        self.events = EventsAPI(self)

    async def publish(
        self,
        connection: Any,
        event_type: str,
        payload: Any,
        *,
        owner_id: str | None = None,
        ordering_key: str | None = None,
        idempotency_key: str | None = None,
    ) -> PublishResult:
        """Record an event in the caller's transaction and schedule its deliveries.

        ``connection`` is whatever your backend works with: an SQLAlchemy
        ``AsyncSession``/``AsyncConnection`` or an asyncpg connection. Run this
        inside the transaction that changes your business data: if it rolls
        back, the event never existed. Pass ``None`` to publish in a
        transaction of its own.

        :param event_type: Dot-separated type such as ``payment.paid``.
        :param payload: Any JSON-serializable value; sent as ``data``.
        :param owner_id: Deliver only to endpoints of this owner (tenant).
        :param ordering_key: Deliveries with the same key to the same endpoint
            are sent strictly one after another, in publish order.
        :param idempotency_key: Publishing the same key twice creates the
            event only once; the duplicate returns the original event id.
        """
        operation = publisher.publish(
            event_type=event_type,
            payload=payload,
            owner_id=owner_id,
            ordering_key=ordering_key,
            idempotency_key=idempotency_key,
            channel=self.config.notify_channel,
            max_payload_bytes=self.config.max_payload_bytes,
            initial_delay=self.config.retry_schedule[0],
        )
        if connection is None:
            result = await self._run(operation)
        else:
            executor = self.backend.executor(connection)
            result = await run_async(operation, executor, schema=self.config.db_schema)
        if not result.duplicate:
            self._metrics.events_published.labels(event_type).inc()
        return result

    async def migrate(self) -> list[int]:
        """Create or upgrade the database schema; return the versions applied."""
        return await self._run(migrate())

    async def status(self) -> QueueStats:
        """Queue size, delivery lag, open circuits and live workers."""
        ttl = self.config.heartbeat_interval * 3
        return await self._run(queue_stats(worker_ttl=ttl))

    async def cleanup(self, older_than: timedelta | None = None) -> int:
        """Delete events (with deliveries and attempts) older than the retention period."""
        age = older_than or timedelta(days=self.config.retention_days)
        total = 0
        while True:
            deleted = await self._run(
                delete_expired_events(older_than=age, batch_size=self.config.cleanup_batch_size)
            )
            total += deleted
            if deleted < self.config.cleanup_batch_size:
                return total

    def worker(self, **overrides: Any) -> Worker:
        """Create a :class:`Worker` sharing this instance's configuration and hooks.

        Keyword arguments override configuration values, e.g. ``concurrency=100``.
        """
        config = (
            WebhooksConfig(**{**self.config.model_dump(), **overrides})
            if overrides
            else self.config
        )
        return Worker(
            self.backend.create_worker_pool,
            config,
            hooks=self.hooks,
            metrics=self._metrics,
            secret_store=self._secrets,
        )

    @asynccontextmanager
    async def running_worker(self, **overrides: Any) -> AsyncIterator[Worker]:
        """Run a worker in the background for the duration of the ``async with`` block."""
        worker = self.worker(**overrides)
        task = asyncio.create_task(worker.run(), name="pgdispatch-worker")
        task.add_done_callback(_log_worker_crash)
        try:
            yield worker
        finally:
            worker.stop()
            with contextlib.suppress(Exception):
                await task

    def lifespan(
        self, *, run_worker: bool = True, migrate: bool = False
    ) -> Callable[[Any], AbstractAsyncContextManager[None]]:
        """An ASGI lifespan handler, e.g. ``FastAPI(lifespan=webhooks.lifespan())``.

        :param run_worker: Deliver webhooks from inside the application process.
            Fine for small deployments; run ``pgdispatch worker`` separately to
            scale delivery independently.
        :param migrate: Apply pending migrations on start-up.
        """

        @asynccontextmanager
        async def handler(app: Any) -> AsyncIterator[None]:
            del app
            try:
                if migrate:
                    await self.migrate()
                if run_worker:
                    async with self.running_worker():
                        yield
                else:
                    yield
            finally:
                await self.close()

        return handler

    async def close(self) -> None:
        """Release connections owned by the backend."""
        await self.backend.close()

    async def __aenter__(self) -> Webhooks:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.close()

    async def _run(self, operation: Operation[T]) -> T:
        async with self.backend.transaction() as executor:
            return await run_async(operation, executor, schema=self.config.db_schema)


class EndpointsAPI:
    """``webhooks.endpoints``: register and manage receiver URLs."""

    def __init__(self, webhooks: Webhooks) -> None:
        self._webhooks = webhooks

    async def create(
        self,
        url: str,
        *,
        event_types: Iterable[str] = ("*",),
        owner_id: str | None = None,
        description: str | None = None,
        headers: Mapping[str, str] | None = None,
        rate_limit_rps: float | None = None,
        secret: str | None = None,
        enabled: bool = True,
    ) -> Endpoint:
        """Register an endpoint. The returned ``endpoint.secret`` is the signing secret.

        :param event_types: Subscription patterns: ``"*"``, ``"payment.paid"``, ``"payment.*"``.
        :param headers: Extra HTTP headers sent with every request (e.g. an API key).
        :param rate_limit_rps: Maximum requests per second to this endpoint.
        :param secret: Use this ``whsec_`` secret instead of generating one.
        """
        w = self._webhooks
        return await w._run(
            endpoint_ops.create_endpoint(
                url=url,
                event_types=event_types,
                owner_id=owner_id,
                description=description,
                headers=headers,
                rate_limit_rps=rate_limit_rps,
                secret=secret,
                enabled=enabled,
                url_policy=w._url_policy,
                secret_store=w._secrets,
            )
        )

    async def get(self, endpoint_id: uuid.UUID, *, owner_id: OwnerFilter = UNSET) -> Endpoint:
        return await self._webhooks._run(endpoint_ops.get_endpoint(endpoint_id, owner_id=owner_id))

    async def list(
        self,
        *,
        owner_id: OwnerFilter = UNSET,
        enabled: bool | None = None,
        limit: int = 100,
        after: uuid.UUID | None = None,
    ) -> builtins.list[Endpoint]:
        """Endpoints ordered by id; pass the last id as ``after`` for the next page."""
        return await self._webhooks._run(
            endpoint_ops.list_endpoints(
                owner_id=owner_id, enabled=enabled, limit=limit, after=after
            )
        )

    async def update(
        self,
        endpoint_id: uuid.UUID,
        *,
        url: str | Unset = UNSET,
        event_types: Iterable[str] | Unset = UNSET,
        description: str | Unset | None = UNSET,
        headers: Mapping[str, str] | Unset = UNSET,
        rate_limit_rps: float | Unset | None = UNSET,
        owner_id: OwnerFilter = UNSET,
    ) -> Endpoint:
        """Change the given fields; fields that are not passed stay as they are."""
        w = self._webhooks
        return await w._run(
            endpoint_ops.update_endpoint(
                endpoint_id,
                owner_id=owner_id,
                url=url,
                event_types=event_types,
                description=description,
                headers=headers,
                rate_limit_rps=rate_limit_rps,
                url_policy=w._url_policy,
            )
        )

    async def enable(self, endpoint_id: uuid.UUID, *, owner_id: OwnerFilter = UNSET) -> Endpoint:
        """Enable the endpoint and reset its circuit breaker."""
        return await self._webhooks._run(
            endpoint_ops.enable_endpoint(endpoint_id, owner_id=owner_id)
        )

    async def disable(
        self, endpoint_id: uuid.UUID, *, reason: str | None = None, owner_id: OwnerFilter = UNSET
    ) -> Endpoint:
        """Pause deliveries. Pending deliveries are kept and resume on :meth:`enable`."""
        return await self._webhooks._run(
            endpoint_ops.disable_endpoint(endpoint_id, reason=reason, owner_id=owner_id)
        )

    async def delete(self, endpoint_id: uuid.UUID, *, owner_id: OwnerFilter = UNSET) -> None:
        """Delete the endpoint with its deliveries and attempt log."""
        await self._webhooks._run(endpoint_ops.delete_endpoint(endpoint_id, owner_id=owner_id))

    async def rotate_secret(
        self,
        endpoint_id: uuid.UUID,
        *,
        grace_period: timedelta = timedelta(hours=24),
        secret: str | None = None,
        owner_id: OwnerFilter = UNSET,
    ) -> Endpoint:
        """Issue a new signing secret; the old one stays valid for ``grace_period``.

        During the grace period requests carry signatures for both secrets, so
        the receiver can switch whenever it is ready.
        """
        w = self._webhooks
        return await w._run(
            endpoint_ops.rotate_secret(
                endpoint_id,
                grace_period=grace_period,
                secret=secret,
                owner_id=owner_id,
                secret_store=w._secrets,
            )
        )

    async def get_secret(self, endpoint_id: uuid.UUID, *, owner_id: OwnerFilter = UNSET) -> str:
        """Return the current signing secret."""
        w = self._webhooks
        return await w._run(
            endpoint_ops.get_secret(endpoint_id, owner_id=owner_id, secret_store=w._secrets)
        )

    async def send_test(
        self, endpoint_id: uuid.UUID, *, owner_id: OwnerFilter = UNSET
    ) -> PublishResult:
        """Send a ``webhook.test`` event to this endpoint only."""
        w = self._webhooks
        return await w._run(
            publisher.send_test(endpoint_id, owner_id=owner_id, channel=w.config.notify_channel)
        )


class DeliveriesAPI:
    """``webhooks.deliveries``: inspect and retry deliveries."""

    def __init__(self, webhooks: Webhooks) -> None:
        self._webhooks = webhooks

    async def list(
        self,
        *,
        endpoint_id: uuid.UUID | None = None,
        event_id: uuid.UUID | None = None,
        status: DeliveryStatus | str | None = None,
        since: datetime | None = None,
        before: int | None = None,
        limit: int = 50,
        owner_id: OwnerFilter = UNSET,
    ) -> builtins.list[Delivery]:
        """Newest first; pass the last id as ``before`` for the next page."""
        return await self._webhooks._run(
            delivery_ops.list_deliveries(
                endpoint_id=endpoint_id,
                event_id=event_id,
                status=status,
                since=since,
                before=before,
                limit=limit,
                owner_id=owner_id,
            )
        )

    async def get(self, delivery_id: int, *, owner_id: OwnerFilter = UNSET) -> Delivery:
        return await self._webhooks._run(delivery_ops.get_delivery(delivery_id, owner_id=owner_id))

    async def attempts(
        self, delivery_id: int, *, owner_id: OwnerFilter = UNSET
    ) -> builtins.list[Attempt]:
        """Every HTTP request made for the delivery, with responses; oldest first."""
        return await self._webhooks._run(delivery_ops.list_attempts(delivery_id, owner_id=owner_id))

    async def retry(self, delivery_id: int, *, owner_id: OwnerFilter = UNSET) -> Delivery:
        """Send again now, with a fresh retry schedule."""
        w = self._webhooks
        return await w._run(
            delivery_ops.retry_delivery(
                delivery_id, owner_id=owner_id, channel=w.config.notify_channel
            )
        )

    async def retry_all(
        self,
        *,
        endpoint_id: uuid.UUID | None = None,
        status: DeliveryStatus | str = DeliveryStatus.FAILED,
        since: datetime | None = None,
        owner_id: OwnerFilter = UNSET,
    ) -> int:
        """Retry every matching delivery (failed ones by default); return how many."""
        w = self._webhooks
        return await w._run(
            delivery_ops.retry_deliveries(
                endpoint_id=endpoint_id,
                status=status,
                since=since,
                owner_id=owner_id,
                channel=w.config.notify_channel,
            )
        )

    async def cancel(self, delivery_id: int, *, owner_id: OwnerFilter = UNSET) -> Delivery:
        """Abandon a pending delivery."""
        return await self._webhooks._run(
            delivery_ops.cancel_delivery(delivery_id, owner_id=owner_id)
        )


class EventsAPI:
    """``webhooks.events``: look up and replay events."""

    def __init__(self, webhooks: Webhooks) -> None:
        self._webhooks = webhooks

    async def get(self, event_id: uuid.UUID, *, owner_id: OwnerFilter = UNSET) -> Event:
        return await self._webhooks._run(event_ops.get_event(event_id, owner_id=owner_id))

    async def list(
        self,
        *,
        owner_id: OwnerFilter = UNSET,
        event_type: str | None = None,
        since: datetime | None = None,
        before: uuid.UUID | None = None,
        limit: int = 50,
    ) -> builtins.list[Event]:
        """Newest first; pass the last id as ``before`` for the next page."""
        return await self._webhooks._run(
            event_ops.list_events(
                owner_id=owner_id, event_type=event_type, since=since, before=before, limit=limit
            )
        )

    async def replay(
        self,
        event_id: uuid.UUID,
        *,
        endpoint_id: uuid.UUID | None = None,
        owner_id: OwnerFilter = UNSET,
    ) -> builtins.list[int]:
        """Deliver an event again; return the new delivery ids.

        With ``endpoint_id``, only to that endpoint (it must belong to the
        event's owner). Without it, to every endpoint subscribed *now*.
        """
        w = self._webhooks
        return await w._run(
            publisher.replay(
                event_id,
                endpoint_id=endpoint_id,
                owner_id=owner_id,
                channel=w.config.notify_channel,
            )
        )


def _log_worker_crash(task: asyncio.Task[None]) -> None:
    if not task.cancelled() and task.exception() is not None:
        logger.error("pgdispatch worker crashed", exc_info=task.exception())
