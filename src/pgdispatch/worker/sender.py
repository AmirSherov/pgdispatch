"""Sending one HTTP request and classifying the receiver's answer."""

from __future__ import annotations

import asyncio
import enum
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta

import aiohttp

from pgdispatch.core.retry import parse_retry_after
from pgdispatch.security.ssrf import BlockedAddressError, SafeResolver, check_url_host

__all__ = ["AttemptResult", "SendOutcome", "Sender", "create_session"]


class AttemptResult(str, enum.Enum):
    """Coarse result of an attempt; used as the ``result`` metric label."""

    SUCCESS = "success"
    HTTP_ERROR = "http_error"
    TIMEOUT = "timeout"
    NETWORK = "network"


@dataclass(frozen=True, slots=True)
class SendOutcome:
    result: AttemptResult
    status_code: int | None
    response_body: str | None
    error: str | None
    duration_ms: int
    retry_after: timedelta | None = None

    @property
    def succeeded(self) -> bool:
        return self.result is AttemptResult.SUCCESS

    @property
    def gone(self) -> bool:
        """The receiver answered ``410 Gone``: it asks us to stop sending."""
        return self.status_code == 410


def create_session(
    *, concurrency: int, allow_private_ips: bool, timeout: timedelta
) -> aiohttp.ClientSession:
    """The HTTP session used by the worker.

    Unless ``allow_private_ips`` is set, host names are resolved through
    :class:`~pgdispatch.security.ssrf.SafeResolver`. Environment proxies are
    ignored (``trust_env=False``) because they would bypass that filter; use
    ``outbound_proxy`` instead.
    """
    connector = aiohttp.TCPConnector(
        limit=concurrency,
        resolver=None if allow_private_ips else SafeResolver(),
    )
    return aiohttp.ClientSession(
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=timeout.total_seconds()),
        trust_env=False,
    )


class Sender:
    """POSTs signed bodies; never follows redirects and never raises for HTTP failures."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        *,
        timeout: timedelta,
        store_response_body: bool,
        max_response_body_bytes: int,
        allow_private_ips: bool,
        proxy: str | None = None,
    ) -> None:
        self._session = session
        self._timeout = timeout.total_seconds()
        self._store_body = store_response_body
        self._max_body = max_response_body_bytes
        self._check_host = not allow_private_ips and proxy is None
        self._proxy = proxy

    async def send(self, url: str, headers: Mapping[str, str], body: bytes) -> SendOutcome:
        started = time.perf_counter()
        try:
            if self._check_host:
                check_url_host(url)
            # The session timeout covers the exchange; this also bounds DNS and
            # anything else that might hang.
            status, raw_body, retry_after = await asyncio.wait_for(
                self._post(url, headers, body), self._timeout
            )
        except asyncio.TimeoutError:
            return self._failure(
                AttemptResult.TIMEOUT, f"timed out after {self._timeout:g}s", started
            )
        except BlockedAddressError as exc:
            return self._failure(AttemptResult.NETWORK, f"blocked: {exc}", started)
        except (aiohttp.ClientError, OSError) as exc:
            return self._failure(AttemptResult.NETWORK, _describe(exc), started)

        duration_ms = _elapsed_ms(started)
        text = _decode(raw_body) if self._store_body else None
        if 200 <= status < 300:
            return SendOutcome(AttemptResult.SUCCESS, status, text, None, duration_ms)
        if 300 <= status < 400:
            error = f"HTTP {status}: redirects are not followed"
        else:
            error = f"HTTP {status}"
        return SendOutcome(
            AttemptResult.HTTP_ERROR,
            status,
            text,
            error,
            duration_ms,
            retry_after=parse_retry_after(retry_after) if status in (429, 503) else None,
        )

    async def _post(
        self, url: str, headers: Mapping[str, str], body: bytes
    ) -> tuple[int, bytes, str | None]:
        async with self._session.post(
            url, data=body, headers=headers, allow_redirects=False, proxy=self._proxy
        ) as response:
            # Read at most max_body bytes: a hostile receiver must not be able
            # to make the worker buffer an endless response.
            content = b""
            while len(content) < self._max_body:
                chunk = await response.content.read(self._max_body - len(content))
                if not chunk:
                    break
                content += chunk
            return response.status, content, response.headers.get("Retry-After")

    @staticmethod
    def _failure(result: AttemptResult, error: str, started: float) -> SendOutcome:
        return SendOutcome(result, None, None, error, _elapsed_ms(started))


def _describe(exc: BaseException) -> str:
    os_error = getattr(exc, "os_error", None)
    if isinstance(os_error, BlockedAddressError):
        return f"blocked: {os_error}"
    message = str(exc) or type(exc).__name__
    return f"{type(exc).__name__}: {message}"


def _decode(raw: bytes) -> str:
    # PostgreSQL text cannot hold NUL bytes.
    return raw.decode("utf-8", errors="replace").replace("\x00", "")


def _elapsed_ms(started: float) -> int:
    return round((time.perf_counter() - started) * 1000)
