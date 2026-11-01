# Configuration

All settings live in one immutable `WebhooksConfig`. Every value can be set:

- in code: `WebhooksConfig(concurrency=100)`;
- through environment variables with the `PGDISPATCH_` prefix, for example `PGDISPATCH_CONCURRENCY=100`;
- in Django: `settings.PGDISPATCH = {"concurrency": 100}`.

Arguments take priority over the environment. Durations accept a
`timedelta`, a number of seconds (`30`, `0.5`), a short form (`250ms`,
`30s`, `5m`, `1h30m`, `14d`) or ISO 8601 (`PT5M`). The defaults are meant for
production.

## Storage

| Setting | Default | Meaning |
| --- | --- | --- |
| `db_schema` | `pgdispatch` | Schema that holds the tables. |
| `retention_days` | 14 | Events, deliveries and attempts older than this are deleted. |
| `cleanup_interval` | 10 min | How often a worker cleans up. |
| `cleanup_batch_size` | 5000 | Rows deleted per transaction. |
| `max_payload_bytes` | 256 KB | Largest serialized payload accepted by `publish()`. |

## Worker

| Setting | Default | Meaning |
| --- | --- | --- |
| `concurrency` | 50 | HTTP requests in flight per worker. |
| `endpoint_concurrency` | 10 | Requests in flight per worker to a single endpoint. |
| `batch_size` | 100 | Deliveries claimed per query. |
| `poll_interval` | 1 s | How often to poll, in case a `NOTIFY` is missed. |
| `lease_duration` | 30 s | How long a claimed delivery stays reserved. Must be longer than `request_timeout`. |
| `heartbeat_interval` | 5 s | Worker heartbeat. |
| `shutdown_timeout` | 25 s | Grace period for requests in flight on shutdown. |

## HTTP

| Setting | Default | Meaning |
| --- | --- | --- |
| `request_timeout` | 15 s | Total time allowed for one request. |
| `user_agent` | `pgdispatch/<version>` | `User-Agent` header. |
| `store_response_body` | `true` | Keep receivers' response bodies in the attempt log. |
| `max_response_body_bytes` | 4096 | How much of each response body to read and keep. |
| `outbound_proxy` | — | Send every request through this HTTP proxy. |

## Retries

| Setting | Default | Meaning |
| --- | --- | --- |
| `retry_schedule` | `0, 5s, 5m, 30m, 2h, 5h, 10h, 10h` | The delay before each attempt. Its length is the maximum number of attempts. |
| `retry_jitter` | 0.2 | Random spread of ±20% on each delay. |

`exponential_backoff()` builds a schedule for you:

```python
from datetime import timedelta
from pgdispatch import WebhooksConfig, exponential_backoff

WebhooksConfig(retry_schedule=exponential_backoff(12, initial=timedelta(seconds=10)))
```

## Circuit breaker

| Setting | Default | Meaning |
| --- | --- | --- |
| `failure_threshold` | 10 | Failures in a row before the breaker opens. |
| `circuit_cooldown` | 5 min | First pause before a probe. It doubles after each failed probe… |
| `circuit_max_cooldown` | 1 h | …up to this limit. |
| `auto_disable_after` | 5 days | Disable an endpoint that has failed without a break for this long. `None` turns this off. |

## Security

| Setting | Default | Meaning |
| --- | --- | --- |
| `allow_http` | `false` | Accept `http://` endpoints. Use it in development only. |
| `allow_private_ips` | `false` | Turn off SSRF protection. Use it in tests only. |
| `secret_encryption_keys` | — | Fernet keys that encrypt signing secrets at rest. The first key encrypts, and all keys decrypt. |

In the environment, set lists as JSON: `PGDISPATCH_RETRY_SCHEDULE='[0, "30s", "5m"]'`,
`PGDISPATCH_SECRET_ENCRYPTION_KEYS='["key1", "key2"]'`.
