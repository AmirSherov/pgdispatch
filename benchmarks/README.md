# Benchmarks

`bench.py` measures two things for 1, 2 and 4 worker processes:

- **Throughput**: 20,000 deliveries are queued first, then the workers drain the
  backlog. The result is deliveries per second, counting from worker start to
  the last recorded success.
- **Latency**: the workers are already running and events are published at a
  steady 200 per second. The receiver records the time from `publish()` (the
  event timestamp) to the request arriving, and the table shows p50 and p99.

The receiver (`receiver.py`) is a separate process that answers `200` at once.
The numbers therefore show the cost of pgdispatch itself: claiming, signing,
sending and recording. They do not include the cost of a slow receiver.

## Results

| workers | deliveries/s | p50 latency | p99 latency |
| ---: | ---: | ---: | ---: |
| 1 | 1,083 | 6 ms | 11 ms |
| 2 | 2,707 | 7 ms | 14 ms |
| 4 | 4,995 | 6 ms | 14 ms |

Measured on 2026-11-01 on one desktop machine:

- **CPU**: Intel Core i5-14400F, 32 GB RAM
- **Database**: PostgreSQL 17 in Docker Desktop (WSL 2, default settings)
- **Workers**: Python 3.13 on Windows 11, `concurrency=100` per worker, everything on localhost

Throughput grows almost linearly with the number of workers, because workers
only coordinate through row locks (`FOR UPDATE SKIP LOCKED`). The single
PostgreSQL instance was not the limit at four workers. On Linux, with the
database on its own host, expect higher numbers. Run the benchmark on your
own hardware before you rely on these figures.

## Run it yourself

```bash
docker run -d --name pgdispatch-bench -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:17
python benchmarks/bench.py --dsn postgresql://postgres:postgres@localhost:5432/postgres
```

| Option | Meaning |
| --- | --- |
| `--events N` | backlog size for the throughput run (default 20,000) |
| `--workers 1 2 4` | worker process counts to measure |
| `--concurrency N` | concurrent requests per worker (default 100) |
| `--rate N --duration S` | publishing rate and length of the latency run |

The benchmark drops and recreates the `pgdispatch_bench` schema, so point it at a scratch database.
