"""Storage of endpoint signing secrets, including rotation and optional encryption.

An endpoint keeps a list of secrets in a ``jsonb`` column::

    [{"key": "whsec_...", "encrypted": false,
      "created_at": "2026-11-01T10:00:00+00:00", "expires_at": null}]

The newest secret comes first. When a secret is rotated the previous ones get
an ``expires_at`` timestamp; until then requests are signed with every active
secret, so receivers can switch keys without dropping a single webhook.

Secrets cannot be hashed because the worker needs them to sign requests. They
can, however, be encrypted at rest with any object implementing
:class:`SecretCipher` — ``cryptography.fernet.Fernet`` and ``MultiFernet`` fit.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol

from pgdispatch.errors import ConfigurationError

__all__ = ["SecretCipher", "SecretEntry", "SecretStore", "fernet_cipher"]


class SecretCipher(Protocol):
    """Symmetric encryption used for secrets at rest (``Fernet`` compatible)."""

    def encrypt(self, data: bytes) -> bytes: ...

    def decrypt(self, token: bytes) -> bytes: ...


@dataclass(frozen=True, slots=True)
class SecretEntry:
    key: str
    encrypted: bool
    created_at: datetime
    expires_at: datetime | None

    def is_active(self, now: datetime) -> bool:
        return self.expires_at is None or self.expires_at > now

    def to_json(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "encrypted": self.encrypted,
            "created_at": self.created_at.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
        }

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> SecretEntry:
        expires_at = data.get("expires_at")
        return cls(
            key=data["key"],
            encrypted=bool(data.get("encrypted", False)),
            created_at=datetime.fromisoformat(data["created_at"]),
            expires_at=datetime.fromisoformat(expires_at) if expires_at else None,
        )


class SecretStore:
    """Seals, opens and rotates the secrets of an endpoint."""

    def __init__(self, cipher: SecretCipher | None = None) -> None:
        self._cipher = cipher

    def seal(self, secret: str, *, now: datetime) -> SecretEntry:
        if self._cipher is None:
            return SecretEntry(key=secret, encrypted=False, created_at=now, expires_at=None)
        token = self._cipher.encrypt(secret.encode()).decode("ascii")
        return SecretEntry(key=token, encrypted=True, created_at=now, expires_at=None)

    def open(self, entry: SecretEntry) -> str:
        if not entry.encrypted:
            return entry.key
        if self._cipher is None:
            msg = (
                "endpoint secrets are encrypted but no decryption key is configured; "
                "set secret_encryption_keys (PGDISPATCH_SECRET_ENCRYPTION_KEYS)"
            )
            raise ConfigurationError(msg)
        return self._cipher.decrypt(entry.key.encode("ascii")).decode()

    def active_secrets(self, raw: str | Sequence[dict[str, Any]], *, now: datetime) -> list[str]:
        """Plaintext secrets that must sign a request at ``now``, newest first."""
        return [self.open(entry) for entry in self.load(raw) if entry.is_active(now)]

    @staticmethod
    def load(raw: str | Sequence[dict[str, Any]]) -> list[SecretEntry]:
        items = json.loads(raw) if isinstance(raw, str) else raw
        return [SecretEntry.from_json(item) for item in items]

    @staticmethod
    def dump(entries: Sequence[SecretEntry]) -> str:
        return json.dumps([entry.to_json() for entry in entries])

    def rotate(
        self,
        entries: Sequence[SecretEntry],
        new_secret: str,
        *,
        now: datetime,
        grace_period: timedelta,
    ) -> list[SecretEntry]:
        """Put ``new_secret`` first; currently active secrets expire after ``grace_period``."""
        deadline = now + grace_period
        kept = [
            SecretEntry(
                key=entry.key,
                encrypted=entry.encrypted,
                created_at=entry.created_at,
                expires_at=min(entry.expires_at, deadline) if entry.expires_at else deadline,
            )
            for entry in entries
            if entry.is_active(now) and grace_period > timedelta(0)
        ]
        return [self.seal(new_secret, now=now), *kept]


def fernet_cipher(keys: Sequence[str]) -> SecretCipher:
    """Build a ``MultiFernet`` cipher; the first key encrypts, all keys decrypt."""
    try:
        from cryptography.fernet import Fernet, MultiFernet
    except ImportError as exc:  # pragma: no cover - depends on the environment
        msg = (
            'secret encryption requires the encryption extra: pip install "pgdispatch[encryption]"'
        )
        raise ConfigurationError(msg) from exc
    if not keys:
        msg = "at least one Fernet key is required"
        raise ConfigurationError(msg)
    return MultiFernet([Fernet(key) for key in keys])
