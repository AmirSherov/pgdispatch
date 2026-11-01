# Publishing events

`publish()` writes the event and its deliveries **inside your transaction**.
That is the main point of pgdispatch. If the transaction commits, the event
will be delivered. If it rolls back, the event never existed.

## SQLAlchemy 2.0 (async)

```python
async with session.begin():
    session.add(order)
    await session.flush()
    result = await webhooks.publish(
        session,  # an AsyncSession or AsyncConnection
        "order.created",
        {"id": order.id, "total": order.total},
        owner_id=order.merchant_id,
    )
```

Any async PostgreSQL driver works for publishing: `asyncpg` or `psycopg`.

## asyncpg

```python
from pgdispatch import AsyncpgBackend, Webhooks

webhooks = Webhooks(AsyncpgBackend("postgresql://app:secret@localhost/app"))

async with pool.acquire() as connection, connection.transaction():
    await connection.execute("UPDATE orders SET status = 'paid' WHERE id = $1", order_id)
    await webhooks.publish(connection, "order.paid", {"id": order_id})
```

## Django

```python
from django.db import transaction
from pgdispatch.django import webhooks

with transaction.atomic():
    order.save()
    webhooks.publish("order.shipped", {"id": order.id}, owner_id=order.shop_id)
```

Inside `atomic()` the event joins your transaction (pgdispatch uses a savepoint).
Outside it, the event is committed on its own.

## Without a business transaction

Pass `None` instead of a connection to publish in a transaction of its own:

```python
await webhooks.publish(None, "report.ready", {"url": url})
```

## Parameters

| Parameter | Meaning |
| --- | --- |
| `event_type` | Dot-separated type such as `order.paid` (at most 255 characters). |
| `payload` | Any JSON-serializable value; sent as `data`. `datetime`, `UUID`, `Decimal`, `Enum`, dataclasses and Pydantic models are converted automatically. At most `max_payload_bytes` (256 KB by default). |
| `owner_id` | Deliver only to endpoints of this owner. |
| `ordering_key` | Deliver events with this key one at a time, in order (see [Concepts](concepts.md#ordering)). |
| `idempotency_key` | Publish at most once per key. |

The result is a `PublishResult(event_id, deliveries, duplicate)`, where
`deliveries` is the number of endpoints the event was fanned out to.

## The request body

Receivers get the envelope recommended by Standard Webhooks:

```json
{"type": "order.paid", "timestamp": "2026-11-01T10:00:00.123Z", "data": {"id": 42}}
```

`timestamp` is the time the event was published. The `webhook-id` header holds
the event id. It is the same on every attempt and every replay.

## How fast is delivery?

The fan-out statement queues a `NOTIFY`, and PostgreSQL delivers it when the
transaction commits. Idle workers wake up at once, so delivery typically starts
a few milliseconds after the commit. If a notification is lost (for example, the
connection was down), workers still poll every `poll_interval` (1 s).
