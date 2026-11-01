"""Django integration.

Add the app and publish events inside ``transaction.atomic()``::

    INSTALLED_APPS = [..., "pgdispatch.django"]
    PGDISPATCH = {"concurrency": 50}  # optional, any WebhooksConfig field

    from pgdispatch.django import webhooks

    with transaction.atomic():
        order.save()
        webhooks.publish("order.shipped", {"id": order.id}, owner_id=order.shop_id)

``python manage.py migrate`` creates the tables and ``python manage.py
pgdispatch_worker`` delivers the webhooks.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pgdispatch.django.client import DjangoWebhooks

    webhooks: DjangoWebhooks

__all__ = ["webhooks"]


def __getattr__(name: str) -> Any:
    # Lazy, so that importing this package does not require configured settings.
    if name == "webhooks":
        from pgdispatch.django.client import get_webhooks

        return get_webhooks()
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
