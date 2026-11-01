from __future__ import annotations

import time
from datetime import datetime, timezone

import pytest
from hypothesis import given
from hypothesis import strategies as st
from standardwebhooks.webhooks import Webhook

from pgdispatch import InvalidSignatureError, ValidationError
from pgdispatch.core.signing import (
    generate_secret,
    sign,
    signature_header,
    validate_secret,
    verify,
)


def test_generated_secrets_are_valid_and_unique() -> None:
    secrets = {generate_secret() for _ in range(100)}
    assert len(secrets) == 100
    for secret in secrets:
        assert validate_secret(secret) == secret


@pytest.mark.parametrize(
    "secret", ["", "whsec_", "whsec_not base64!", "sk_" + "A" * 44, "whsec_QUJD"]
)
def test_malformed_secrets_are_rejected(secret: str) -> None:
    with pytest.raises(ValidationError):
        validate_secret(secret)


@given(msg_id=st.text(min_size=1, max_size=40), body=st.binary(max_size=2000))
def test_signature_matches_reference_implementation(msg_id: str, body: bytes) -> None:
    secret = generate_secret()
    timestamp = int(time.time())
    reference = Webhook(secret).sign(
        msg_id, datetime.fromtimestamp(timestamp, tz=timezone.utc), body.decode("latin-1")
    )
    # The reference signs text; compare on the same bytes.
    assert sign(secret, msg_id, timestamp, body.decode("latin-1").encode()) == reference


def test_reference_implementation_verifies_our_requests() -> None:
    secret = generate_secret()
    body = b'{"type":"payment.paid","data":{"id":1}}'
    timestamp = int(time.time())
    headers = {
        "webhook-id": "evt_1",
        "webhook-timestamp": str(timestamp),
        "webhook-signature": signature_header([secret], "evt_1", timestamp, body),
    }
    assert Webhook(secret).verify(body, headers) == {"type": "payment.paid", "data": {"id": 1}}


def test_verify_accepts_any_of_several_signatures() -> None:
    old, new = generate_secret(), generate_secret()
    body = b"{}"
    now = int(time.time())
    headers = {
        "Webhook-Id": "evt_1",
        "Webhook-Timestamp": str(now),
        "Webhook-Signature": signature_header([new, old], "evt_1", now, body),
    }
    verify(old, headers, body)
    verify(new, headers, body)
    with pytest.raises(InvalidSignatureError):
        verify(generate_secret(), headers, body)


def test_verify_rejects_tampering_and_stale_timestamps() -> None:
    secret = generate_secret()
    now = int(time.time())
    headers = {
        "webhook-id": "evt_1",
        "webhook-timestamp": str(now),
        "webhook-signature": sign(secret, "evt_1", now, b"{}"),
    }
    with pytest.raises(InvalidSignatureError):
        verify(secret, headers, b'{"tampered":true}')
    with pytest.raises(InvalidSignatureError, match="tolerance"):
        verify(secret, headers, b"{}", now=now + 3600)
    with pytest.raises(InvalidSignatureError, match="missing"):
        verify(secret, {"webhook-id": "evt_1"}, b"{}")
    with pytest.raises(InvalidSignatureError, match="timestamp"):
        verify(secret, {**headers, "webhook-timestamp": "soon"}, b"{}")
