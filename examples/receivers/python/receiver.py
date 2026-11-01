"""A minimal webhook receiver that verifies pgdispatch / Standard Webhooks signatures.

Standard library only (Python 3.9+). Run it with::

    WEBHOOK_SECRET=whsec_... PORT=8000 python receiver.py

If you can add a dependency, the official ``standardwebhooks`` package does
the same verification in two lines::

    from standardwebhooks import Webhook, WebhookVerificationError

    event = Webhook(secret).verify(raw_body, headers)  # raises on failure

The important part is :func:`verify`; copy it into your own framework
(FastAPI, Django, Flask, ...) and pass it the *raw* request body.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from collections.abc import Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

TOLERANCE_SECONDS = 5 * 60
MAX_BODY_BYTES = 1024 * 1024


class InvalidWebhookError(Exception):
    """The request is not an authentic, fresh webhook."""


def verify(secret: str, headers: Mapping[str, str], body: bytes) -> None:
    """Raise :class:`InvalidWebhookError` unless ``body`` was signed with ``secret``.

    ``headers`` lookups must be case-insensitive (``http.server``, Starlette
    and Django's ``request.headers`` all are). ``body`` must be the exact bytes
    received: re-serialized JSON would not match the signature.
    """
    msg_id = headers.get("webhook-id")
    timestamp = headers.get("webhook-timestamp")
    signatures = headers.get("webhook-signature")
    if not msg_id or not timestamp or not signatures:
        raise InvalidWebhookError("missing webhook headers")

    # Replay protection: reject requests signed too far in the past or future.
    try:
        sent_at = int(timestamp)
    except ValueError:
        raise InvalidWebhookError("invalid webhook-timestamp") from None
    if abs(time.time() - sent_at) > TOLERANCE_SECONDS:
        raise InvalidWebhookError("timestamp outside the tolerance window")

    # The key is the base64 part of the secret, after the "whsec_" prefix.
    key = base64.b64decode(secret.removeprefix("whsec_"))
    signed_content = f"{msg_id}.{timestamp}.".encode() + body
    expected = base64.b64encode(hmac.new(key, signed_content, hashlib.sha256).digest())

    # During a secret rotation the header holds several "v1,<sig>" entries
    # separated by spaces; one match is enough.
    for entry in signatures.split():
        version, _, signature = entry.partition(",")
        if version == "v1" and hmac.compare_digest(signature.encode(), expected):
            return
    raise InvalidWebhookError("no matching signature")


class WebhookHandler(BaseHTTPRequestHandler):
    secret = os.environ["WEBHOOK_SECRET"]

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        if length > MAX_BODY_BYTES:
            self.send_error(413)
            return
        body = self.rfile.read(length)

        try:
            verify(self.secret, self.headers, body)
        except InvalidWebhookError as exc:
            self.send_error(401, str(exc))
            return

        event = json.loads(body)
        # Delivery is at-least-once: the same webhook-id may arrive again after
        # a timeout or a retry. Store processed ids (e.g. a unique column) and
        # skip duplicates. Keep this handler fast: enqueue the work and answer
        # 2xx right away, then process it in the background.
        print(f"received {event['type']} ({self.headers['webhook-id']})")

        self.send_response(204)
        self.end_headers()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    print(f"listening on :{port}")
    ThreadingHTTPServer(("", port), WebhookHandler).serve_forever()
