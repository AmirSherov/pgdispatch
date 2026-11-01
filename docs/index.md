# pgdispatch

**Reliable webhook delivery for Python apps. It needs only the PostgreSQL you already have.**

You publish an event in the same transaction as your business change.
pgdispatch then delivers it to every subscribed endpoint. Each request is signed
according to [Standard Webhooks](https://www.standardwebhooks.com/). Failed
requests are retried, a circuit breaker protects endpoints that are down, and
every attempt is logged.

```python
async with session.begin():
    order.status = "paid"
    await webhooks.publish(session, "order.paid", {"id": order.id})
```

- **Nothing new to run.** The queue, the retry schedule and the attempt log are tables in your database.
- **No lost events.** The outbox is transactional, delivery is at-least-once, and workers can be killed at any time.
- **Webhooks are the main feature.** You get signatures, secret rotation, a circuit breaker, ordering, replay and multi-tenancy.
- **Works with FastAPI, SQLAlchemy 2.0, Django and asyncpg.**

[Get started](getting-started.md){ .md-button .md-button--primary }
[Concepts](concepts.md){ .md-button }
