# Concepts

The whole model has five entities. An **event** creates one **delivery** for
each subscribed **endpoint**. Each delivery makes one or more **attempts**.

| Entity | What it is | Example |
| --- | --- | --- |
| **Event** | An immutable fact with a type and a JSON payload | `order.paid`, `{"id": 42}` |
| **Endpoint** | A receiver URL with its own signing secret and settings | `https://shop.example.com/hooks` |
| **Subscription** | The event types an endpoint wants (`event_types`) | `["order.*", "refund.created"]` |
| **Delivery** | The task "deliver event X to endpoint Y", with a status | `pending → succeeded` |
| **Attempt** | One HTTP request of a delivery, with its response | attempt 3, `503`, 1,200 ms |

## Event types and subscriptions

An event type is a dot-separated name made of letters, digits, `_` and `-`,
for example `order.paid` or `invoice.payment_failed`. An endpoint subscribes with patterns:

| Pattern | Matches |
| --- | --- |
| `*` | every event |
| `order.paid` | exactly `order.paid` |
| `order.*` | `order.paid`, `order.refund.created`, … (but not `order` itself) |

Subscriptions are resolved **when the event is published**. An endpoint
created later does not receive older events. You can
[replay](endpoints.md#replaying-events) them to it.

## Owners (tenants)

Every endpoint and event can have an `owner_id`, for example a merchant or
organisation id. An event is delivered only to endpoints with the **same**
owner. Events without an owner go only to endpoints without an owner. This
keeps one customer's data from reaching another customer's endpoint.

Management methods take `owner_id` too. When you pass it, the call is limited
to that owner, and objects of other owners behave as if they don't exist. This
is how the [FastAPI router](endpoints.md#a-ready-made-api-for-your-customers)
isolates tenants. If you leave `owner_id` out (internal, trusted code), the
call works across all owners. `owner_id=None` means "objects without an owner".

## Delivery lifecycle

```text
          ┌───────── retry (5 s … 10 h) ─────────┐
          ▼                                      │
 ──▶ pending ──claim──▶ in_flight ──2xx──▶ succeeded
                            │
                            ├──error, attempts left──▶ pending
                            ├──error, no attempts left──▶ failed
                            └──410 Gone──▶ cancelled (endpoint disabled)
```

| Status | Meaning |
| --- | --- |
| `pending` | Waiting for its next attempt (`next_attempt_at`). |
| `in_flight` | A worker has claimed it and is sending it. The claim is a lease that expires if the worker dies. |
| `succeeded` | The endpoint answered with a 2xx status. |
| `failed` | Every attempt in the retry schedule failed. You can retry or replay it at any time. |
| `cancelled` | Abandoned: the endpoint answered `410 Gone`, or the delivery was cancelled manually. |

## What counts as success

| Response | Result |
| --- | --- |
| 2xx | Success. |
| `410 Gone` | The receiver asks to unsubscribe: the endpoint is disabled and the delivery cancelled. |
| `429`, 5xx, timeout, connection or DNS error | Retried on schedule. `Retry-After` is respected for `429` and `503`. |
| 3xx | Treated as an error. Redirects are never followed (SSRF protection). |
| Any other 4xx | Retried. The receiver may have broken its server only for a while. |

Every failure counts towards the endpoint's [circuit breaker](guarantees.md#circuit-breaker).

## Ordering

By default deliveries run in parallel and may arrive in any order. That gives
the best throughput. When order matters, pass an `ordering_key`:

```python
await webhooks.publish(session, "order.updated", payload, ordering_key=f"order:{order.id}")
```

For each endpoint, a delivery with a key is sent only after every earlier
delivery with the same key has finished, whether it succeeded or failed. The
cost is head-of-line blocking: an event stuck in retries holds back later
events **with the same key**, but nothing else.

## Idempotent publishing

```python
await webhooks.publish(session, "order.paid", payload, idempotency_key=f"order.paid:{order.id}")
```

When you publish with a key that already exists, nothing is created and you
get back the original event id (`result.duplicate is True`). Keys are
remembered for as long as events are kept (`retention_days`).
