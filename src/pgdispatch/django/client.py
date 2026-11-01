"""The synchronous Django facade: ``pgdispatch.django.webhooks``."""

from __future__ import annotations

import builtins
import uuid
from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta
from functools import cache
from typing import Any, TypeVar

import asyncpg
from django.conf import settings
from django.db import connections, transaction
from django.db.backends.base.base import BaseDatabaseWrapper

from pgdispatch.backends.asyncpg import WORKER_POOL_SIZE
from pgdispatch.backends.base import Operation, Row, run_sync, to_pyformat_style
from pgdispatch.config import WebhooksConfig
from pgdispatch.core import deliveries as delivery_ops
from pgdispatch.core import endpoints as endpoint_ops
from pgdispatch.core import events as event_ops
from pgdispatch.core import publisher
from pgdispatch.core.common import UNSET, OwnerFilter, Unset
from pgdispatch.core.maintenance import QueueStats, queue_stats
from pgdispatch.core.secrets import SecretStore, fernet_cipher
from pgdispatch.errors import ConfigurationError
from pgdispatch.hooks import Hooks
from pgdispatch.metrics import get_metrics
from pgdispatch.migrations import drop_schema as drop_schema_operation
from pgdispatch.migrations import migrate
from pgdispatch.models import Attempt, Delivery, DeliveryStatus, Endpoint, Event, PublishResult
from pgdispatch.worker.worker import Worker

__all__ = ["DjangoWebhooks", "drop_schema", "get_webhooks", "migrate_schema"]

T = TypeVar("T")


class DjangoExecutor:
    """Runs statements through a Django database connection."""

    def __init__(self, connection: BaseDatabaseWrapper) -> None:
        if connection.vendor != "postgresql":
            msg = (
                f"pgdispatch requires PostgreSQL, "
                f"but database {connection.alias!r} is {connection.vendor}"
            )
            raise ConfigurationError(msg)
        self._connection = connection

    def fetch(self, sql: str, params: Mapping[str, Any]) -> list[Row]:
        with self._connection.cursor() as cursor:
            cursor.execute(to_pyformat_style(sql), dict(params))
            if cursor.description is None:
                return []
            names = [column[0] for column in cursor.description]
            return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]


class DjangoWebhooks:
    """Blocking counterpart of :class:`pgdispatch.Webhooks` for Django projects.

    Every call runs in ``transaction.atomic(using=...)``, which becomes a
    savepoint when you are already inside a transaction, so ``publish`` joins
    your business transaction.
    """

    def __init__(self, config: WebhooksConfig | None = None, *, using: str = "default") -> None:
        self.config = config or WebhooksConfig()
        self.using = using
        self.hooks = Hooks()
        keys = [key.get_secret_value() for key in self.config.secret_encryption_keys]
        self._secrets = SecretStore(fernet_cipher(keys) if keys else None)
        self._metrics = get_metrics()
        self._url_policy = endpoint_ops.UrlPolicy(
            allow_http=self.config.allow_http, allow_private_ips=self.config.allow_private_ips
        )
        self.endpoints = DjangoEndpoints(self)
        self.deliveries = DjangoDeliveries(self)
        self.events = DjangoEvents(self)

    def publish(
        self,
        event_type: str,
        payload: Any,
        *,
        owner_id: str | None = None,
        ordering_key: str | None = None,
        idempotency_key: str | None = None,
        using: str | None = None,
    ) -> PublishResult:
        """Record an event in the current transaction; see :meth:`pgdispatch.Webhooks.publish`."""
        result = self._run(
            publisher.publish(
                event_type=event_type,
                payload=payload,
                owner_id=owner_id,
                ordering_key=ordering_key,
                idempotency_key=idempotency_key,
                channel=self.config.notify_channel,
                max_payload_bytes=self.config.max_payload_bytes,
                initial_delay=self.config.retry_schedule[0],
            ),
            using=using,
        )
        if not result.duplicate:
            self._metrics.events_published.labels(event_type).inc()
        return result

    def status(self) -> QueueStats:
        return self._run(queue_stats(worker_ttl=self.config.heartbeat_interval * 3))

    def worker(self, **overrides: Any) -> Worker:
        """A :class:`Worker` connected to the same database (used by ``pgdispatch_worker``)."""
        config = (
            WebhooksConfig(**{**self.config.model_dump(), **overrides})
            if overrides
            else self.config
        )
        connect_kwargs = asyncpg_connect_kwargs(settings.DATABASES[self.using])

        async def create_pool() -> asyncpg.Pool:
            pool: asyncpg.Pool = await asyncpg.create_pool(
                min_size=1, max_size=WORKER_POOL_SIZE, **connect_kwargs
            )
            return pool

        return Worker(
            create_pool, config, hooks=self.hooks, metrics=self._metrics, secret_store=self._secrets
        )

    def _run(self, operation: Operation[T], *, using: str | None = None) -> T:
        alias = using or self.using
        with transaction.atomic(using=alias):
            return run_sync(
                operation, DjangoExecutor(connections[alias]), schema=self.config.db_schema
            )


class DjangoEndpoints:
    def __init__(self, webhooks: DjangoWebhooks) -> None:
        self._webhooks = webhooks

    def create(
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
        w = self._webhooks
        return w._run(
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

    def get(self, endpoint_id: uuid.UUID, *, owner_id: OwnerFilter = UNSET) -> Endpoint:
        return self._webhooks._run(endpoint_ops.get_endpoint(endpoint_id, owner_id=owner_id))

    def list(
        self,
        *,
        owner_id: OwnerFilter = UNSET,
        enabled: bool | None = None,
        limit: int = 100,
        after: uuid.UUID | None = None,
    ) -> builtins.list[Endpoint]:
        return self._webhooks._run(
            endpoint_ops.list_endpoints(
                owner_id=owner_id, enabled=enabled, limit=limit, after=after
            )
        )

    def update(
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
        w = self._webhooks
        return w._run(
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

    def enable(self, endpoint_id: uuid.UUID, *, owner_id: OwnerFilter = UNSET) -> Endpoint:
        return self._webhooks._run(endpoint_ops.enable_endpoint(endpoint_id, owner_id=owner_id))

    def disable(
        self, endpoint_id: uuid.UUID, *, reason: str | None = None, owner_id: OwnerFilter = UNSET
    ) -> Endpoint:
        return self._webhooks._run(
            endpoint_ops.disable_endpoint(endpoint_id, reason=reason, owner_id=owner_id)
        )

    def delete(self, endpoint_id: uuid.UUID, *, owner_id: OwnerFilter = UNSET) -> None:
        self._webhooks._run(endpoint_ops.delete_endpoint(endpoint_id, owner_id=owner_id))

    def rotate_secret(
        self,
        endpoint_id: uuid.UUID,
        *,
        grace_period: timedelta = timedelta(hours=24),
        secret: str | None = None,
        owner_id: OwnerFilter = UNSET,
    ) -> Endpoint:
        w = self._webhooks
        return w._run(
            endpoint_ops.rotate_secret(
                endpoint_id,
                grace_period=grace_period,
                secret=secret,
                owner_id=owner_id,
                secret_store=w._secrets,
            )
        )

    def get_secret(self, endpoint_id: uuid.UUID, *, owner_id: OwnerFilter = UNSET) -> str:
        w = self._webhooks
        return w._run(
            endpoint_ops.get_secret(endpoint_id, owner_id=owner_id, secret_store=w._secrets)
        )

    def send_test(self, endpoint_id: uuid.UUID, *, owner_id: OwnerFilter = UNSET) -> PublishResult:
        w = self._webhooks
        return w._run(
            publisher.send_test(endpoint_id, owner_id=owner_id, channel=w.config.notify_channel)
        )


class DjangoDeliveries:
    def __init__(self, webhooks: DjangoWebhooks) -> None:
        self._webhooks = webhooks

    def list(
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
        return self._webhooks._run(
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

    def get(self, delivery_id: int, *, owner_id: OwnerFilter = UNSET) -> Delivery:
        return self._webhooks._run(delivery_ops.get_delivery(delivery_id, owner_id=owner_id))

    def attempts(
        self, delivery_id: int, *, owner_id: OwnerFilter = UNSET
    ) -> builtins.list[Attempt]:
        return self._webhooks._run(delivery_ops.list_attempts(delivery_id, owner_id=owner_id))

    def retry(self, delivery_id: int, *, owner_id: OwnerFilter = UNSET) -> Delivery:
        w = self._webhooks
        return w._run(
            delivery_ops.retry_delivery(
                delivery_id, owner_id=owner_id, channel=w.config.notify_channel
            )
        )

    def retry_all(
        self,
        *,
        endpoint_id: uuid.UUID | None = None,
        status: DeliveryStatus | str = DeliveryStatus.FAILED,
        since: datetime | None = None,
        owner_id: OwnerFilter = UNSET,
    ) -> int:
        w = self._webhooks
        return w._run(
            delivery_ops.retry_deliveries(
                endpoint_id=endpoint_id,
                status=status,
                since=since,
                owner_id=owner_id,
                channel=w.config.notify_channel,
            )
        )

    def cancel(self, delivery_id: int, *, owner_id: OwnerFilter = UNSET) -> Delivery:
        return self._webhooks._run(delivery_ops.cancel_delivery(delivery_id, owner_id=owner_id))


class DjangoEvents:
    def __init__(self, webhooks: DjangoWebhooks) -> None:
        self._webhooks = webhooks

    def get(self, event_id: uuid.UUID, *, owner_id: OwnerFilter = UNSET) -> Event:
        return self._webhooks._run(event_ops.get_event(event_id, owner_id=owner_id))

    def list(
        self,
        *,
        owner_id: OwnerFilter = UNSET,
        event_type: str | None = None,
        since: datetime | None = None,
        before: uuid.UUID | None = None,
        limit: int = 50,
    ) -> builtins.list[Event]:
        return self._webhooks._run(
            event_ops.list_events(
                owner_id=owner_id, event_type=event_type, since=since, before=before, limit=limit
            )
        )

    def replay(
        self,
        event_id: uuid.UUID,
        *,
        endpoint_id: uuid.UUID | None = None,
        owner_id: OwnerFilter = UNSET,
    ) -> builtins.list[int]:
        w = self._webhooks
        return w._run(
            publisher.replay(
                event_id,
                endpoint_id=endpoint_id,
                owner_id=owner_id,
                channel=w.config.notify_channel,
            )
        )


@cache
def get_webhooks() -> DjangoWebhooks:
    """The project-wide instance, configured from ``settings.PGDISPATCH``."""
    options = dict(getattr(settings, "PGDISPATCH", {}))
    using = options.pop("database", "default")
    return DjangoWebhooks(WebhooksConfig(**options), using=using)


def migrate_schema(connection: BaseDatabaseWrapper, *, target: int | None = None) -> list[int]:
    """Apply pgdispatch SQL migrations through a Django connection."""
    return run_sync(
        migrate(target=target), DjangoExecutor(connection), schema=get_webhooks().config.db_schema
    )


def drop_schema(connection: BaseDatabaseWrapper) -> None:
    run_sync(
        drop_schema_operation(), DjangoExecutor(connection), schema=get_webhooks().config.db_schema
    )


def asyncpg_connect_kwargs(database: Mapping[str, Any]) -> dict[str, Any]:
    """Translate a Django ``DATABASES`` entry into asyncpg connection arguments."""
    if "postgresql" not in database.get("ENGINE", ""):
        msg = f"pgdispatch requires PostgreSQL, got ENGINE={database.get('ENGINE')!r}"
        raise ConfigurationError(msg)
    options = database.get("OPTIONS", {})
    kwargs: dict[str, Any] = {
        "host": database.get("HOST") or None,
        "port": int(database["PORT"]) if database.get("PORT") else None,
        "user": database.get("USER") or None,
        "password": database.get("PASSWORD") or None,
        "database": database.get("NAME") or None,
    }
    if options.get("sslmode"):
        kwargs["ssl"] = options["sslmode"]
    return {key: value for key, value in kwargs.items() if value is not None}
