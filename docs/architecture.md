# Architecture

## Components

```text
            ┌──────────── your application ────────────┐
            │  publish()   endpoints.*   deliveries.*   │
            └─────┬────────────────┬───────────────────┘
                  │ same tx        │ management
                  ▼                ▼
   ┌──────────────── PostgreSQL: schema "pgdispatch" ─────────────────┐
   │ endpoints   events   deliveries (the queue)   attempts   workers │
   └──────┬──────────────────────▲──────────────────────▲─────────────┘
   NOTIFY │              claim   │ SKIP LOCKED          │ results (batched)
          ▼                      │                      │
   ┌───────────────────── worker (N processes) ──────────────────────┐
   │ dispatch loop ──▶ send tasks (aiohttp, signed) ──▶ recorder     │
   │ maintenance: heartbeat · metrics · LISTEN health · clean-up     │
   └──────────────────────────────┬─────────────────────────────────┘
                                  ▼ HTTPS
                          customer endpoints
```

## Schema

| Table | Holds | Notes |
| --- | --- | --- |
| `endpoints` | URL, `event_types[]`, `secrets` (jsonb, newest first), headers, rate limit, circuit breaker state | GIN index on `event_types` for fan-out |
| `events` | `id` (UUIDv7, time-ordered), type, owner, `payload` jsonb, idempotency key | unique partial index on the idempotency key |
| `deliveries` | event × endpoint, status, `attempt_count`, `next_attempt_at`, `locked_until` | partial index on `next_attempt_at` for the hot claim query |
| `attempts` | one row per HTTP request: status, body (≤ 4 KB), duration, error | written in bulk |
| `workers` | live workers and their heartbeats | used for `status` and rate-limit sharing |
| `schema_migrations` | applied migration versions | |

**Fan-out at publish time.** `publish()` inserts the deliveries right away,
so the subscriptions are fixed at the moment of the event, and workers only
read one table. It finds subscribers with one array-overlap test
(`event_types && candidates`). The candidates are every pattern that can match
the type: `["*", "a.b.c", "a.*", "a.b.*"]`. This works because wildcards are
only allowed as a trailing segment.

## The worker

- **Dispatch** waits for `LISTEN` notifications, a finished batch or the
  polling interval. It then claims up to the number of free slots. Claiming
  also moves endpoints whose cool-down has ended to half-open, and claims
  exactly one probe for each of them. A per-endpoint limit is applied inside
  the claim query with a window function, so a slow endpoint cannot take every slot.
- **Send tasks** apply the endpoint's rate limit (an in-memory GCRA bucket,
  shared between live workers), sign the body once, POST it and classify the answer.
- **Recorder** collects results and writes them in a single transaction per
  batch: it locks the deliveries in id order, applies the updates with
  `attempt_count` fencing, bulk-inserts the attempts and updates the circuit
  breakers. Endpoints that are healthy and only saw successes are not touched,
  so a busy endpoint never becomes a lock hotspot. A send task gives up its
  slot only after its result is stored, so the next claim always sees the
  current breaker and ordering state.
- **Maintenance** sends heartbeats, refreshes metrics, re-establishes `LISTEN`
  if its connection drops, and runs the retention clean-up under an advisory lock.

## One SQL path for every framework

The same operations run on asyncpg, SQLAlchemy (any async driver) and
Django (psycopg, synchronously). Each operation is written once, as a
generator that yields queries and receives rows:

```python
def get_endpoint(endpoint_id: UUID, *, owner_id: OwnerFilter) -> Operation[Endpoint]:
    rows = yield Query("SELECT ... WHERE e.id = CAST(:id AS uuid) ...", {"id": str(endpoint_id)})
    return Endpoint.from_row(first_or_not_found(rows, "endpoint", endpoint_id))
```

`run_async` and `run_sync` drive the generator against an executor for the
driver in use. Executors only translate placeholders and run one statement,
so the business logic stays in one place. The worker is asyncpg-only and
uses array parameters (`unnest`) for its batched statements.

## Design decisions

- **Leases instead of long transactions.** Holding a row lock during an HTTP
  request would tie up connections and make crashes costly. Leases make every
  worker disposable.
- **aiohttp for outbound HTTP.** Its connection pool scales to hundreds of
  concurrent requests per process, and its resolver hook is the public API
  that lets the SSRF filter connect to the IP it checked.
- **Signing is built in.** HMAC over three headers is a few lines of standard
  library code. The test suite checks it against the reference
  `standardwebhooks` package, which removes a dependency without losing compatibility.
- **No ORM models for pgdispatch tables.** The schema is owned by versioned SQL
  migrations, shared by the CLI, Alembic and Django. Two sources of truth
  would drift apart.
