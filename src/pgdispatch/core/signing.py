"""Request signing according to the `Standard Webhooks`_ specification.

Every request carries three headers:

``webhook-id``
    The event id. It is identical across retries, so receivers can deduplicate.
``webhook-timestamp``
    Unix time of this attempt, in seconds.
``webhook-signature``
    ``v1,<base64 HMAC-SHA256(secret, "{id}.{timestamp}.{body}")>``. During a
    secret rotation several space-separated signatures are sent, one per
    active secret, and a receiver accepts the request if any of them matches.

The implementation is a few lines of standard-library code; the test-suite
checks it against the reference ``standardwebhooks`` package, so receivers can
use any official Standard Webhooks SDK.

.. _Standard Webhooks: https://www.standardwebhooks.com/
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import secrets
import time
from collections.abc import Iterable, Mapping

from pgdispatch.errors import InvalidSignatureError, ValidationError

__all__ = [
    "SECRET_PREFIX",
    "generate_secret",
    "sign",
    "signature_header",
    "validate_secret",
    "verify",
]

SECRET_PREFIX = "whsec_"  # noqa: S105 - a prefix, not a password
SIGNATURE_VERSION = "v1"
DEFAULT_TOLERANCE_SECONDS = 5 * 60

_MIN_SECRET_BYTES = 24
_MAX_SECRET_BYTES = 64


def generate_secret(num_bytes: int = 32) -> str:
    """Create a new random signing secret such as ``whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw``."""
    if not _MIN_SECRET_BYTES <= num_bytes <= _MAX_SECRET_BYTES:
        msg = f"secrets must be {_MIN_SECRET_BYTES}-{_MAX_SECRET_BYTES} bytes long"
        raise ValueError(msg)
    return SECRET_PREFIX + base64.b64encode(secrets.token_bytes(num_bytes)).decode("ascii")


def validate_secret(secret: str) -> str:
    """Return ``secret`` if it is a well-formed ``whsec_`` secret.

    :raises ValidationError: if it is not.
    """
    try:
        key = _decode_secret(secret)
    except ValueError:
        key = b""
    if (
        not secret.startswith(SECRET_PREFIX)
        or not _MIN_SECRET_BYTES <= len(key) <= _MAX_SECRET_BYTES
    ):
        msg = (
            f"a signing secret must look like '{SECRET_PREFIX}<base64>' and encode "
            f"{_MIN_SECRET_BYTES}-{_MAX_SECRET_BYTES} random bytes"
        )
        raise ValidationError(msg)
    return secret


def sign(secret: str, msg_id: str, timestamp: int, body: bytes) -> str:
    """Return a single ``v1,<signature>`` value for ``body``."""
    signed_content = f"{msg_id}.{timestamp}.".encode() + body
    digest = hmac.new(_decode_secret(secret), signed_content, hashlib.sha256).digest()
    return f"{SIGNATURE_VERSION},{base64.b64encode(digest).decode('ascii')}"


def signature_header(secrets_: Iterable[str], msg_id: str, timestamp: int, body: bytes) -> str:
    """Build the ``webhook-signature`` header value, one signature per active secret."""
    return " ".join(sign(secret, msg_id, timestamp, body) for secret in secrets_)


def verify(
    secret: str,
    headers: Mapping[str, str],
    body: bytes,
    *,
    tolerance: int = DEFAULT_TOLERANCE_SECONDS,
    now: float | None = None,
) -> None:
    """Check a received webhook; raise :class:`InvalidSignatureError` if it is not authentic.

    Useful in tests and in Python receivers::

        from pgdispatch import verify_signature

        verify_signature(secret, request.headers, await request.body())
    """
    lowered = {name.lower(): value for name, value in headers.items()}
    try:
        msg_id = lowered["webhook-id"]
        raw_timestamp = lowered["webhook-timestamp"]
        received = lowered["webhook-signature"]
    except KeyError as exc:
        msg = f"missing required header {exc.args[0]!r}"
        raise InvalidSignatureError(msg) from None

    try:
        timestamp = int(raw_timestamp)
    except ValueError:
        msg = "invalid webhook-timestamp header"
        raise InvalidSignatureError(msg) from None
    current = time.time() if now is None else now
    if abs(current - timestamp) > tolerance:
        msg = "webhook timestamp is outside the allowed tolerance"
        raise InvalidSignatureError(msg)

    expected = sign(secret, msg_id, timestamp, body).encode()
    for candidate in received.split():
        version, _, _ = candidate.partition(",")
        if version == SIGNATURE_VERSION and hmac.compare_digest(candidate.encode(), expected):
            return
    msg = "no matching signature found"
    raise InvalidSignatureError(msg)


def _decode_secret(secret: str) -> bytes:
    encoded = secret.removeprefix(SECRET_PREFIX)
    try:
        return base64.b64decode(encoded, validate=True)
    except binascii.Error as exc:
        msg = "secret is not valid base64"
        raise ValueError(msg) from exc
