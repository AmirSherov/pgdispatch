"""A minimal webhook receiver: verifies pgdispatch signatures and logs every event.

::

    WEBHOOK_SECRET=whsec_... python receiver.py --port 8080

Every path is accepted, so one receiver can stand in for several endpoints.

``WEBHOOK_SECRET``
    The endpoint's signing secret. Requests are rejected with 401 until it is set.
``FAIL_RATE``
    Fraction of requests (0 to 1) to answer with 503, to watch pgdispatch retry
    and open the circuit breaker. Default: 0.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random

from aiohttp import web

from pgdispatch import InvalidSignatureError, verify_signature

log = logging.getLogger("receiver")

SECRET_KEY = web.AppKey("secret", str)
FAIL_RATE_KEY = web.AppKey("fail_rate", float)


async def receive(request: web.Request) -> web.Response:
    # Verify against the raw bytes: re-serialized JSON would not match the signature.
    body = await request.read()
    message_id = request.headers.get("webhook-id", "-")
    try:
        verify_signature(request.app[SECRET_KEY], request.headers, body)
    except InvalidSignatureError as exc:
        log.warning("rejected %s: %s", message_id, exc)
        return web.json_response({"error": str(exc)}, status=401)

    if random.random() < request.app[FAIL_RATE_KEY]:
        log.info("simulating an outage for %s", message_id)
        return web.json_response({"error": "simulated outage"}, status=503)

    # `webhook-id` stays the same across retries: a real receiver stores it
    # and ignores messages it has already processed.
    event = json.loads(body)
    log.info("%s %s %s", event["type"], message_id, json.dumps(event["data"]))
    return web.json_response({"received": message_id})


async def health(_: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


def create_app(secret: str, fail_rate: float) -> web.Application:
    app = web.Application()
    app[SECRET_KEY] = secret
    app[FAIL_RATE_KEY] = fail_rate
    app.router.add_get("/health", health)
    app.router.add_post("/{path:.*}", receive)
    return app


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    secret = os.environ.get("WEBHOOK_SECRET", "")
    if not secret:
        # An empty secret never matches, so every request is answered with 401.
        log.warning("WEBHOOK_SECRET is not set: every webhook will be rejected")
    fail_rate = float(os.environ.get("FAIL_RATE", "0"))
    web.run_app(
        create_app(secret, fail_rate), host=args.host, port=args.port, access_log=None, print=None
    )


if __name__ == "__main__":
    main()
