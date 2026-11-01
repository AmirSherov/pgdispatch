# pgdispatch

**Reliable webhook delivery for Python apps. It needs only the PostgreSQL you already have.**

[![CI](https://github.com/AmirSherov/pgdispatch/actions/workflows/ci.yml/badge.svg)](https://github.com/AmirSherov/pgdispatch/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pgdispatch)](https://pypi.org/project/pgdispatch/)
[![Python](https://img.shields.io/pypi/pyversions/pgdispatch)](https://pypi.org/project/pgdispatch/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

You publish an event in the same database transaction as your business change.
pgdispatch then delivers it to every subscribed endpoint. Each request is signed
according to [Standard Webhooks](https://www.standardwebhooks.com/). Failed
requests are retried for about 27 hours, a circuit breaker protects endpoints
that are down, and every attempt is logged so you can replay it. You don't need
Redis, Kafka, Celery or a separate webhook service.

```python
async with session.begin():
    order.status = "paid"
    await webhooks.publish(session, "order.paid", {"id": order.id, "amount": 15000})
# Committed: the event will be delivered. Rolled back: it never existed.
```

## Why

Sending webhooks naively loses events and duplicates them:

```python
order.status = "paid"
await session.commit()
await httpx.post(customer_url, json=payload)  # the process dies here, and the event is gone
```

- **Dual write.** If the process crashes between `commit` and `post`, the order
  is saved but the webhook is lost forever. If you swap the two lines, a
  customer can be told about an order that was never saved.
- **No retries, no back-off.** A `503` or a timeout from the receiver is the end of it.
- **Dead endpoints slow everyone down.** A customer whose server is down for a day keeps tying up your workers.
- **No authenticity.** Receivers can't prove that a request came from you.
- **No audit trail.** Support can't answer "did customer X get event Y, and what did they reply?"

pgdispatch solves these problems with a **transactional outbox** that lives in
your database. A small async **worker** reads it with `FOR UPDATE SKIP LOCKED`
and wakes up instantly on `LISTEN/NOTIFY`.

## Features

- **Transactional outbox**: `publish()` joins your transaction. Works with SQLAlchemy 2.0 (async), Django and asyncpg.
- **At-least-once delivery**: workers can be killed at any moment (`kill -9` included) and nothing is lost.
- **Standard Webhooks signatures**: receivers verify requests with official SDKs in any language. Secrets can be rotated with zero downtime.
- **Retries** with jitter and `Retry-After` support. The default schedule has 8 attempts over about 27 hours and can be changed.
- **Per-endpoint circuit breaker** with growing cool-downs. Endpoints that keep failing are disabled automatically.
- **Ordered delivery** per `ordering_key` when you need it; full parallelism when you don't.
- **Idempotent publishing** with `idempotency_key`.
- **Multi-tenant**: endpoints belong to an `owner_id`, and events never cross owners.
- **SSRF protection**: private addresses are blocked, the connection is pinned to the checked IP (which stops DNS rebinding), and redirects are never followed.
- **Observability**: Prometheus metrics, a Grafana dashboard, alert rules, a CLI and a full attempt log.
- **Ready-made FastAPI router** that lets your customers manage their endpoints.
- **Typed** (`mypy --strict`), with a small set of core dependencies: `asyncpg`, `aiohttp`, `pydantic`, `prometheus-client`.

## Installation

```bash
pip install "pgdispatch[sqlalchemy]"   # FastAPI / SQLAlchemy projects
pip install "pgdispatch[django]"       # Django projects
pip install pgdispatch                 # plain asyncpg
```

You need Python 3.10+ and PostgreSQL 13+.

## Quick start: FastAPI + SQLAlchemy

```python
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import create_async_engine

from pgdispatch import Webhooks
from pgdispatch.sqlalchemy import SQLAlchemyBackend

engine = create_async_engine("postgresql+asyncpg://app:secret@localhost/app")
webhooks = Webhooks(SQLAlchemyBackend(engine))

# Create the schema on start-up and deliver webhooks from inside the app.
app = FastAPI(lifespan=webhooks.lifespan(migrate=True))


@app.post("/orders/{order_id}/pay")
async def pay(order_id: int, session: SessionDep) -> None:
    async with session.begin():
        order = await session.get(Order, order_id)
        order.status = "paid"
        await webhooks.publish(
            session,
            "order.paid",
            {"id": order.id, "amount": order.amount},
            owner_id=order.merchant_id,  # deliver only to this merchant's endpoints
            ordering_key=f"order:{order.id}",  # optional: keep this order's events in sequence
            idempotency_key=f"order.paid:{order.id}",  # optional: publish at most once
        )
```

Then register an endpoint. You would normally do this from your product's API:

```python
endpoint = await webhooks.endpoints.create(
    "https://merchant.example.com/webhooks",
    event_types=["order.*"],
    owner_id="merchant_42",
)
endpoint.secret  # "whsec_..." — show it to the customer once
```

As traffic grows, take the worker out of the web process and scale it on its own:

```bash
pgdispatch worker --dsn postgresql://app:secret@localhost/app --concurrency 100
```

## Quick start: Django

```python
# settings.py
INSTALLED_APPS = [..., "pgdispatch.django"]
PGDISPATCH = {"concurrency": 50}  # optional; any WebhooksConfig field
```

```python
from django.db import transaction
from pgdispatch.django import webhooks

with transaction.atomic():
    order.status = "shipped"
    order.save()
    webhooks.publish("order.shipped", {"id": order.id}, owner_id=order.shop_id)
```

```bash
python manage.py migrate
python manage.py pgdispatch_worker --concurrency 50
```

## What receivers get

```http
POST /webhooks HTTP/1.1
content-type: application/json
webhook-id: 01a1985f-e246-725c-a639-369d5fd0a21b
webhook-timestamp: 1793527200
webhook-signature: v1,K5oZfzN95Z9UVu1EsfQmfVNQhnkZ2pj9o9NDN/H/pI4=

{"type":"order.paid","timestamp":"2026-11-01T10:00:00.123Z","data":{"id":42,"amount":15000}}
```

Receivers can verify requests with any [Standard Webhooks library](https://github.com/standard-webhooks/standard-webhooks/tree/main/libraries)
or with pgdispatch itself:

```python
from pgdispatch import verify_signature

verify_signature(secret, request.headers, await request.body())  # raises InvalidSignatureError
```

[`examples/receivers`](examples/receivers) has complete, dependency-free receivers in
Python, Node.js and Go. `webhook-id` is the same on every retry, so receivers
should use it to deduplicate.

## Guarantees

| Guarantee | Yes / No | Details |
| --- | --- | --- |
| An event from a rolled-back transaction is never sent | **yes** | The event and your data are written in one transaction. |
| A committed event is never lost | **yes** | If a worker dies, its lease expires and another worker takes the delivery over. |
| Exactly-once delivery | **no** | A worker can die after the receiver answered `200` but before the result was stored. Deduplicate by `webhook-id`. |
| Ordering | **with `ordering_key`** | Deliveries with the same key go to an endpoint one at a time, in publish order. Without a key, events can arrive in any order. |
| Delivery while the receiver is down | **for about 27 hours** | Then the delivery is marked `failed`, and you can retry or replay it at any time. |
| No extra infrastructure | **yes** | PostgreSQL 13+ only. |

The test suite proves these guarantees against real PostgreSQL and real worker processes
([`tests/chaos`](tests/chaos/test_guarantees.py)):

- [x] A rolled-back transaction creates 0 deliveries.
- [x] `kill -9` of a worker with 1,000 deliveries in flight: after a restart all 1,000 arrive and none are lost.
- [x] 4 worker processes deliver 10,000 events, each exactly once.
- [x] The endpoint goes down, the circuit breaker opens, and after recovery every event arrives.
- [x] 1,000 events with one `ordering_key`, handled by two competing workers, arrive strictly in order.

## How it works

```text
 your transaction                       pgdispatch worker(s)
┌──────────────────────────┐          ┌───────────────────────────────────────┐
│ UPDATE orders ...        │          │ 1. LISTEN pgdispatch_new / poll       │
│ publish():               │ COMMIT   │ 2. claim due deliveries               │
│   INSERT event           │ ───────▶ │    FOR UPDATE SKIP LOCKED + lease     │
│   INSERT deliveries      │  NOTIFY  │ 3. sign + POST (aiohttp, SSRF-safe)   │
│   (one per subscriber)   │          │ 4. record attempts, schedule retries, │
└──────────────────────────┘          │    update circuit breakers (batched)  │
                                      └───────────────────────────────────────┘
```

All state is kept in the `pgdispatch` schema: `endpoints`, `events`,
`deliveries`, `attempts` and `workers`. A worker claims deliveries with a
short **lease** instead of holding a lock while it sends a request, so any
worker can be stopped at any time. Run as many workers as you like; they
coordinate only through row locks. See [docs/architecture.md](docs/architecture.md).

## Performance

These numbers come from one desktop machine (i5-14400F, PostgreSQL 17 in
Docker), with a fast local receiver:

| workers | deliveries/s | p50 latency | p99 latency |
| ---: | ---: | ---: | ---: |
| 1 | 1,083 | 6 ms | 11 ms |
| 2 | 2,707 | 7 ms | 14 ms |
| 4 | 4,995 | 6 ms | 14 ms |

Latency is measured from `publish()` until the request reaches the receiver.
See [benchmarks](benchmarks) to reproduce the run on your own hardware.

## Comparison

| | Form | Infrastructure | Transactional outbox | Signatures | License |
| --- | --- | --- | --- | --- | --- |
| **pgdispatch** | Python library | PostgreSQL | yes, in your transaction | Standard Webhooks | MIT |
| [Svix](https://github.com/svix/svix-webhooks) | standalone server (Rust) | PostgreSQL, optional Redis | no, called over HTTP | Standard Webhooks | MIT |
| [Convoy](https://github.com/frain-dev/convoy) | standalone server (Go) | PostgreSQL + Redis | no, called over HTTP | own scheme | Elastic License 2.0 |
| [Hook0](https://github.com/hook0/hook0) | standalone server (Rust) | PostgreSQL | no, called over HTTP | own scheme | SSPL v1 |

Standalone servers are a good fit when many services in many languages need to
send webhooks. pgdispatch is for Python teams who want webhooks to be part of
their own application and database, with no extra service to deploy, monitor and
secure. (Licenses were checked in November 2026.)

## Operations

```bash
pgdispatch status                                  # queue size, delivery lag, open circuit breakers
pgdispatch deliveries --status failed --since 1h
pgdispatch deliveries show 18342                   # every attempt with the receiver's response
pgdispatch retry 18342                             # retry one delivery
pgdispatch retry --endpoint <id>                   # retry all failed deliveries of an endpoint
pgdispatch replay --event <id> [--endpoint <id>]   # deliver an event again
pgdispatch endpoints list --owner merchant_42
pgdispatch endpoints enable <id>                   # re-enable and reset the circuit breaker
pgdispatch cleanup --older-than 14d
```

The same operations are available from Python (`webhooks.deliveries.retry(...)`,
`webhooks.events.replay(...)`, …). Metrics such as
`pgdispatch_oldest_pending_age_seconds` (delivery lag) are exported to Prometheus.
[`contrib/`](contrib) contains a Grafana dashboard and alert rules.

## Documentation

- [Getting started](docs/getting-started.md)
- [Concepts](docs/concepts.md): events, endpoints, deliveries, attempts, owners, ordering
- [Publishing](docs/publishing.md) and [managing endpoints](docs/endpoints.md)
- [Receiving webhooks](docs/receiving.md): signature verification in any language
- [Running workers](docs/workers.md): deployment, scaling, graceful shutdown
- [Configuration](docs/configuration.md)
- [Reliability and guarantees](docs/guarantees.md)
- [Security](docs/security.md): SSRF, secrets, data retention
- [Observability](docs/observability.md): metrics, logs, hooks, CLI
- [Architecture](docs/architecture.md)

## Try it in five minutes

```bash
git clone https://github.com/AmirSherov/pgdispatch && cd pgdispatch
docker compose up --build
```

This starts PostgreSQL, an example shop, a worker, a receiver, Prometheus and
Grafana. Follow [examples/fastapi_shop](examples/fastapi_shop) to send your first webhook.

## Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md).
Please report security problems privately, as described in [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE) © Amir Sherov. pgdispatch is free for commercial use.
