"""Measure pgdispatch throughput and end-to-end latency.

Two scenarios run for each number of worker processes:

* **throughput** - a backlog of ``--events`` deliveries is published first,
  then the workers drain it; reported as deliveries per second;
* **latency** - the workers are already running and events are published at a
  steady ``--rate`` per second; the receiver reports p50/p99 of the time from
  ``publish()`` to arrival.

Usage::

    python benchmarks/bench.py --dsn postgresql://postgres:postgres@localhost/postgres
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import platform
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from typing import Any

import asyncpg

from pgdispatch import AsyncpgBackend, Webhooks, WebhooksConfig

SCHEMA = "pgdispatch_bench"
HERE = Path(__file__).parent


def receiver_stats(port: int) -> dict[str, Any]:
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/stats") as response:
        stats: dict[str, Any] = json.load(response)
        return stats


def receiver_reset(port: int) -> None:
    request = urllib.request.Request(f"http://127.0.0.1:{port}/reset", method="POST")
    urllib.request.urlopen(request).close()


def start_workers(count: int, dsn: str, concurrency: int) -> list[subprocess.Popen[bytes]]:
    env = {
        **os.environ,
        "PGDISPATCH_ALLOW_HTTP": "true",
        "PGDISPATCH_ALLOW_PRIVATE_IPS": "true",
        "PGDISPATCH_CONCURRENCY": str(concurrency),
        "PGDISPATCH_ENDPOINT_CONCURRENCY": str(concurrency),
        "PGDISPATCH_BATCH_SIZE": str(min(concurrency, 500)),
    }
    command = [sys.executable, "-m", "pgdispatch", "worker", "--dsn", dsn, "--schema", SCHEMA]
    return [subprocess.Popen([*command, "--log-level", "warning"], env=env) for _ in range(count)]


def stop(processes: list[subprocess.Popen[bytes]]) -> None:
    for process in processes:
        process.terminate()
    for process in processes:
        process.wait()


async def prepare(dsn: str, port: int) -> Webhooks:
    connection = await asyncpg.connect(dsn)
    await connection.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    await connection.close()
    config = WebhooksConfig(db_schema=SCHEMA, allow_http=True, allow_private_ips=True)
    webhooks = Webhooks(AsyncpgBackend(dsn), config)
    await webhooks.migrate()
    await webhooks.endpoints.create(f"http://127.0.0.1:{port}/hooks")
    return webhooks


async def succeeded(dsn: str) -> int:
    connection = await asyncpg.connect(dsn)
    try:
        count: int = await connection.fetchval(
            f"SELECT count(*) FROM {SCHEMA}.deliveries WHERE status = 'succeeded'"
        )
        return count
    finally:
        await connection.close()


async def throughput(args: argparse.Namespace, workers: int) -> float:
    webhooks = await prepare(args.dsn, args.port)
    connection = await asyncpg.connect(args.dsn)
    async with connection.transaction():
        for number in range(args.events):
            await webhooks.publish(connection, "bench.event", {"n": number})
    await connection.close()

    processes = start_workers(workers, args.dsn, args.concurrency)
    started = time.perf_counter()
    try:
        while await succeeded(args.dsn) < args.events:
            await asyncio.sleep(0.05)
        return args.events / (time.perf_counter() - started)
    finally:
        stop(processes)
        await webhooks.close()


async def latency(args: argparse.Namespace, workers: int) -> dict[str, Any]:
    webhooks = await prepare(args.dsn, args.port)
    processes = start_workers(workers, args.dsn, args.concurrency)
    await asyncio.sleep(2)  # let the workers start
    receiver_reset(args.port)
    total = args.rate * args.duration
    try:
        started = time.perf_counter()
        for number in range(total):
            await webhooks.publish(None, "bench.event", {"n": number})
            ahead = started + (number + 1) / args.rate - time.perf_counter()
            if ahead > 0:
                await asyncio.sleep(ahead)
        while receiver_stats(args.port)["received"] < total:
            await asyncio.sleep(0.1)
        return receiver_stats(args.port)
    finally:
        stop(processes)
        await webhooks.close()


async def run(args: argparse.Namespace) -> None:
    system = f"{platform.system()} {platform.machine()}, Python {platform.python_version()}"
    print(f"pgdispatch benchmark: {system}")
    print(
        f"{args.events} events, concurrency {args.concurrency} per worker, "
        f"latency measured at {args.rate} events/s\n"
    )
    print("| workers | deliveries/s | p50 latency | p99 latency |")
    print("| ---: | ---: | ---: | ---: |")
    for workers in args.workers:
        rate = await throughput(args, workers)
        stats = await latency(args, workers)
        p50, p99 = stats["p50"] * 1000, stats["p99"] * 1000
        print(f"| {workers} | {rate:,.0f} | {p50:.0f} ms | {p99:.0f} ms |", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Measure pgdispatch throughput and latency.")
    parser.add_argument("--dsn", default=os.environ.get("PGDISPATCH_DSN"))
    parser.add_argument("--events", type=int, default=20_000)
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--rate", type=int, default=200, help="events per second (latency run)")
    parser.add_argument("--duration", type=int, default=10, help="seconds (latency run)")
    parser.add_argument("--port", type=int, default=8099)
    args = parser.parse_args()
    if not args.dsn:
        parser.error("pass --dsn or set PGDISPATCH_DSN")

    receiver = subprocess.Popen(
        [sys.executable, str(HERE / "receiver.py"), "--port", str(args.port)]
    )
    try:
        time.sleep(1.5)
        asyncio.run(run(args))
    finally:
        receiver.terminate()
        receiver.wait()


if __name__ == "__main__":
    main()
