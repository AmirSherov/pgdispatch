# Managing endpoints

All examples use the async API. The Django facade (`pgdispatch.django.webhooks`)
has the same methods, only blocking.

## Create

```python
endpoint = await webhooks.endpoints.create(
    "https://merchant.example.com/webhooks",
    event_types=["order.*"],  # default: ["*"]
    owner_id="merchant_42",
    description="Production",
    headers={"X-Api-Key": "..."},  # sent with every request
    rate_limit_rps=20,  # optional per-endpoint rate limit
)
endpoint.secret  # "whsec_..." — show it to the customer once
```

The URL must use `https` and must not contain credentials or point to a
private address (see [Security](security.md)). Custom headers cannot override
`webhook-*`, `content-type`, `user-agent` and other headers that pgdispatch sets.

## Read, update, delete

```python
await webhooks.endpoints.get(endpoint_id, owner_id="merchant_42")
await webhooks.endpoints.list(owner_id="merchant_42", enabled=True, limit=100, after=last_id)
await webhooks.endpoints.update(endpoint_id, event_types=["order.*", "refund.*"], description=None)
await webhooks.endpoints.delete(endpoint_id)  # also deletes its deliveries and attempts
```

`update` changes only the fields you pass. Pass `None` to clear `description`
or `rate_limit_rps`.

## Enable and disable

```python
await webhooks.endpoints.disable(endpoint_id, reason="customer request")
await webhooks.endpoints.enable(endpoint_id)  # also resets the circuit breaker
```

Pending deliveries of a disabled endpoint stay in the queue and resume when it is enabled again.
New events are not fanned out to disabled endpoints.

## Signing secrets

```python
secret = await webhooks.endpoints.get_secret(endpoint_id)
rotated = await webhooks.endpoints.rotate_secret(endpoint_id, grace_period=timedelta(hours=24))
rotated.secret  # the new secret
```

During the grace period every request carries **two** signatures, one per secret.
The receiver can switch to the new secret whenever it is ready, and no request
fails verification. Use `grace_period=timedelta(0)` to revoke a leaked secret
immediately.

## Test events

```python
await webhooks.endpoints.send_test(endpoint_id)
```

This sends a `webhook.test` event to this endpoint only, whatever it is subscribed to.
It is handy for a "Send test event" button.

## Inspecting and retrying deliveries

```python
failed = await webhooks.deliveries.list(endpoint_id=endpoint_id, status="failed", limit=50)
attempts = await webhooks.deliveries.attempts(failed[0].id)  # requests and responses
await webhooks.deliveries.retry(failed[0].id)  # now, with a fresh retry schedule
await webhooks.deliveries.retry_all(endpoint_id=endpoint_id)  # every failed delivery
await webhooks.deliveries.cancel(delivery_id)  # give up on a pending delivery
```

Lists return the newest items first. To get the next page, pass the last id as `before`.

## Replaying events

```python
await webhooks.events.replay(event_id)  # to every endpoint subscribed now
await webhooks.events.replay(event_id, endpoint_id=endpoint_id)  # to one endpoint
```

Replay creates new deliveries for an existing event. Use it to backfill a newly
created endpoint, or to resend after the customer fixed their server. Replays
keep the original `webhook-id` and ignore `ordering_key`. An event can be replayed
only to endpoints of its own owner.

## A ready-made API for your customers

`pgdispatch.fastapi.router` gives your product a complete endpoint-management API:

```python
from fastapi import Depends, FastAPI
from pgdispatch.fastapi import router


async def current_merchant(user=Depends(current_user)) -> str:
    return user.merchant_id


app.include_router(router(webhooks, owner=current_merchant), prefix="/webhooks")
```

| Method | Path | |
| --- | --- | --- |
| `GET` | `/endpoints` | list |
| `POST` | `/endpoints` | create (the response includes the secret) |
| `GET`, `PATCH`, `DELETE` | `/endpoints/{id}` | read, update, delete |
| `POST` | `/endpoints/{id}/enable`, `/disable` | |
| `GET` | `/endpoints/{id}/secret` | reveal the secret |
| `POST` | `/endpoints/{id}/rotate-secret` | `{"grace_period_seconds": 86400}` |
| `POST` | `/endpoints/{id}/test` | send `webhook.test` |
| `GET` | `/deliveries?endpoint_id=&status=&before=` | list |
| `GET` | `/deliveries/{id}` | detail with attempts |
| `POST` | `/deliveries/{id}/retry` | |

The `owner` dependency is required. It authenticates the request and limits every
operation to that owner.
