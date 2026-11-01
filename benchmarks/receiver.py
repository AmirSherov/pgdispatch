"""A minimal, fast webhook receiver for benchmarks.

Answers every POST with 200 and records the end-to-end latency of each
webhook: the time between the event being published (the ``timestamp`` in the
body) and its arrival here. ``GET /stats`` returns the count and percentiles,
``POST /reset`` clears them.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime

from aiohttp import web

latencies: list[float] = []


async def receive(request: web.Request) -> web.Response:
    body = json.loads(await request.read())
    published = datetime.fromisoformat(body["timestamp"].replace("Z", "+00:00")).timestamp()
    latencies.append(time.time() - published)
    return web.Response(text="ok")


async def stats(_: web.Request) -> web.Response:
    ordered = sorted(latencies)

    def percentile(fraction: float) -> float | None:
        if not ordered:
            return None
        return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]

    return web.json_response(
        {"received": len(ordered), "p50": percentile(0.5), "p99": percentile(0.99)}
    )


async def reset(_: web.Request) -> web.Response:
    latencies.clear()
    return web.Response(text="ok")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8099)
    args = parser.parse_args()
    app = web.Application()
    app.router.add_get("/stats", stats)
    app.router.add_post("/reset", reset)
    app.router.add_post("/hooks", receive)
    web.run_app(app, host="127.0.0.1", port=args.port, access_log=None, print=None)


if __name__ == "__main__":
    main()
