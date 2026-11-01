# Observability

## Prometheus metrics

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `pgdispatch_events_published_total` | counter | `event_type` | Events published. |
| `pgdispatch_delivery_attempts_total` | counter | `result` | HTTP attempts: `success`, `http_error`, `timeout`, `network`. |
| `pgdispatch_delivery_duration_seconds` | histogram | `result` | Duration of attempts. |
| `pgdispatch_deliveries_failed_total` | counter | — | Deliveries that used up every attempt. |
| `pgdispatch_deliveries_pending` | gauge | — | Deliveries waiting on enabled endpoints. |
| `pgdispatch_oldest_pending_age_seconds` | gauge | — | **Delivery lag**: how long the most overdue delivery has waited. This is the main metric to alert on. |
| `pgdispatch_endpoints_circuit_open` | gauge | — | Enabled endpoints whose breaker is open or half-open. |
| `pgdispatch_worker_batch_size` | histogram | — | Deliveries claimed per query. |

The metrics are registered in `prometheus_client`'s default registry, so they
appear on your application's existing `/metrics` endpoint. Standalone workers
serve them with `--metrics-port 9100`. Endpoint ids are deliberately left out
of the labels, because thousands of endpoints would create too many time
series. For per-endpoint details, use the CLI or the API.

[`contrib/`](https://github.com/AmirSherov/pgdispatch/tree/main/contrib) contains:

- `grafana/dashboard.json`: throughput, success ratio, latency percentiles, lag, queue size, circuit breakers;
- `prometheus/alerts.yml`: alerts for a lag over 5 minutes, a failure ratio over 20% for 10 minutes, a rising number of open breakers, and pending deliveries that nothing is delivering.

## Logs

pgdispatch logs through the standard `logging` module, under the `pgdispatch`
logger (`pgdispatch.worker`, `pgdispatch.hooks`). Attempt records carry
structured fields in `extra`: `event_id`, `delivery_id`, `endpoint_id`,
`attempt`, `status_code` and `duration_ms`. JSON formatters and `structlog`
pick these up.

| Level | What |
| --- | --- |
| `DEBUG` | every successful attempt |
| `INFO` | failed attempts, worker start and stop, clean-up |
| `WARNING` | circuit breaker opened, endpoint disabled, database errors (retried) |

## Hooks

```python
@webhooks.hooks.on_endpoint_disabled
async def notify_customer(endpoint: Endpoint, reason: str) -> None:
    await mailer.send(endpoint.owner_id, f"Your webhook endpoint was disabled: {reason}")


@webhooks.hooks.on_circuit_opened
def page_on_call(endpoint: Endpoint) -> None: ...


@webhooks.hooks.on_delivery_failed
async def record(delivery: Delivery) -> None: ...
```

Hooks run in the worker after the change has been committed. They can be
plain functions or coroutines. An exception in a hook is logged and does not
affect delivery.

## CLI

```bash
pgdispatch status [--json]
pgdispatch deliveries [--status S] [--endpoint ID] [--event ID] [--owner O] [--since 1h] [--json]
pgdispatch deliveries show ID [--json]
pgdispatch retry ID | pgdispatch retry --endpoint ID [--status failed] [--since 1d]
pgdispatch replay --event ID [--endpoint ID]
pgdispatch endpoints list [--owner O] [--enabled/--no-enabled] [--json]
pgdispatch endpoints enable ID | pgdispatch endpoints disable ID [--reason TEXT]
pgdispatch cleanup [--older-than 14d]
pgdispatch migrate
pgdispatch worker [--concurrency N] [--batch-size N] [--metrics-port P]
```

The database comes from `--dsn`, `PGDISPATCH_DSN` or `DATABASE_URL`.
SQLAlchemy-style URLs such as `postgresql+asyncpg://` are accepted.
