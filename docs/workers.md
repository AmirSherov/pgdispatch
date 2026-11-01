# Running workers

A worker claims due deliveries, sends them, and writes the results back. All
coordination happens through PostgreSQL, so you can run any number of workers
in any number of processes and hosts.

## In the application process

The simplest setup suits small and medium traffic:

```python
app = FastAPI(lifespan=webhooks.lifespan())  # FastAPI / Starlette
```

```python
async with webhooks.running_worker():  # any asyncio program
    await serve_forever()
```

If you already have a lifespan, nest the worker inside it:

```python
@asynccontextmanager
async def lifespan(app):
    async with engine.begin() as connection:
        ...
    async with webhooks.running_worker():
        yield
```

!!! note "Several web processes"
    Each process with an in-app worker is one more worker. That is safe, but with
    many web replicas it is usually cleaner to run dedicated workers.

## As separate processes

```bash
pgdispatch worker --dsn postgresql://app:secret@db/app --concurrency 100 --metrics-port 9100
python manage.py pgdispatch_worker --concurrency 100 --metrics-port 9100   # Django
```

All `PGDISPATCH_*` environment variables apply (see [Configuration](configuration.md)).
In Kubernetes, run workers as a `Deployment` and scale the replica count.

## Sizing

| Setting | Default | Notes |
| --- | --- | --- |
| `concurrency` | 50 | HTTP requests in flight per worker. Most of the time is spent waiting for receivers, so values of 100–500 are fine. |
| `endpoint_concurrency` | 10 | Requests in flight per worker **to one endpoint**. It keeps one slow endpoint from taking every slot. |
| `batch_size` | 100 | Deliveries claimed per query. |

A worker keeps at most 5 database connections, one of which is used for
`LISTEN`. The worker needs a direct (or session-pooled) connection: `LISTEN`
does not work through PgBouncer in transaction-pooling mode. If you use
transaction pooling, workers still work by polling every `poll_interval`.

## Graceful shutdown

On `SIGTERM` or `SIGINT`, the worker:

1. stops claiming new deliveries;
2. waits up to `shutdown_timeout` (25 s) for requests in flight;
3. returns deliveries that are still unfinished to `pending`, without counting an attempt;
4. writes all results and removes itself from the `workers` table.

Set your orchestrator's grace period a little longer than `shutdown_timeout`.
Kubernetes defaults to 30 s, which fits the default 25 s.

## Crashes

If a worker is killed without warning (OOM, `kill -9`, a lost node), its
claims are leases that expire after `lease_duration` (30 s). Another worker
then picks those deliveries up. Nothing is lost, but a request that was in
flight may be sent again. This is why receivers deduplicate by `webhook-id`.

## Housekeeping

Workers also:

- send a heartbeat every `heartbeat_interval`. `pgdispatch status` shows live workers, and per-endpoint rate limits are split between them;
- refresh the queue gauges for Prometheus;
- delete events older than `retention_days`, together with their deliveries and attempts, in batches. Only one worker cleans up at a time, guarded by an advisory lock.
