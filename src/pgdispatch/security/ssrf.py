"""Server-side request forgery (SSRF) protection.

Endpoint URLs are supplied by your customers, so without protection anyone
could point the worker at ``http://169.254.169.254/`` (cloud metadata),
``http://localhost:5432`` or an internal admin panel. pgdispatch defends in two
places:

1. :func:`validate_endpoint_url` runs when an endpoint is created or updated:
   only ``https`` (``http`` with ``allow_http``), no credentials in the URL, and
   no literal private addresses.
2. :class:`SafeResolver` runs on every connection: the host name is resolved,
   *all* resulting addresses must be public, and aiohttp connects to exactly
   the addresses that were checked. Re-resolving the name later is impossible,
   which closes the DNS-rebinding hole. TLS still verifies the certificate
   against the original host name. Literal IP hosts never reach a resolver, so
   :func:`check_url_host` vets them before each request.

Redirects are never followed, so a public endpoint cannot bounce the worker to
a private one either.
"""

from __future__ import annotations

import ipaddress
import socket
from urllib.parse import urlsplit

import aiohttp
from aiohttp.abc import AbstractResolver, ResolveResult

from pgdispatch.errors import UnsafeURLError

__all__ = [
    "BlockedAddressError",
    "SafeResolver",
    "check_url_host",
    "is_public_address",
    "validate_endpoint_url",
]

IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address

MAX_URL_LENGTH = 2048
_LOCAL_HOST_SUFFIXES = (".localhost", ".local", ".internal", ".localdomain")


class BlockedAddressError(OSError):
    """The endpoint host resolves to an address the worker must not connect to."""


def is_public_address(address: IPAddress) -> bool:
    """Tell whether ``address`` is a globally routable unicast address.

    Rejects private, loopback, link-local, multicast, reserved, shared (CGNAT)
    and documentation ranges, and IPv6 addresses that embed a non-public IPv4
    address (IPv4-mapped, 6to4, Teredo).
    """
    if isinstance(address, ipaddress.IPv6Address):
        embedded = address.ipv4_mapped or address.sixtofour
        if embedded is not None:
            return is_public_address(embedded)
        if address.teredo is not None:
            return all(is_public_address(part) for part in address.teredo)
    return address.is_global and not address.is_multicast


def validate_endpoint_url(
    url: str, *, allow_http: bool = False, allow_private_ips: bool = False
) -> str:
    """Return ``url`` if it may be used as an endpoint, else raise :class:`UnsafeURLError`."""
    if len(url) > MAX_URL_LENGTH:
        msg = f"URL is longer than {MAX_URL_LENGTH} characters"
        raise UnsafeURLError(msg)
    try:
        parts = urlsplit(url)
        port = parts.port
    except ValueError as exc:
        msg = f"invalid URL: {exc}"
        raise UnsafeURLError(msg) from exc

    allowed_schemes = ("https", "http") if allow_http else ("https",)
    if parts.scheme not in allowed_schemes:
        hint = "" if allow_http else " (plain http needs allow_http=True, for development only)"
        msg = f"URL scheme must be {' or '.join(allowed_schemes)}{hint}"
        raise UnsafeURLError(msg)
    if parts.username is not None or parts.password is not None:
        msg = "URL must not contain credentials; use custom endpoint headers instead"
        raise UnsafeURLError(msg)
    host = parts.hostname
    if not host:
        msg = "URL must contain a host name"
        raise UnsafeURLError(msg)
    if port == 0:
        msg = "URL port must not be 0"
        raise UnsafeURLError(msg)

    if not allow_private_ips:
        address = _parse_ip(host)
        if address is not None and not is_public_address(address):
            msg = f"URL points to a non-public address ({address})"
            raise UnsafeURLError(msg)
        if address is None and (host == "localhost" or host.endswith(_LOCAL_HOST_SUFFIXES)):
            msg = f"URL points to a local host name ({host})"
            raise UnsafeURLError(msg)
    return url


class SafeResolver(AbstractResolver):
    """An aiohttp resolver that refuses host names resolving to non-public addresses."""

    def __init__(self, inner: AbstractResolver | None = None) -> None:
        self._inner = inner or aiohttp.ThreadedResolver()

    async def resolve(
        self, host: str, port: int = 0, family: socket.AddressFamily = socket.AF_INET
    ) -> list[ResolveResult]:
        results = await self._inner.resolve(host, port, family)
        for result in results:
            address = ipaddress.ip_address(result["host"].split("%", 1)[0])
            if not is_public_address(address):
                msg = f"refusing to connect to {host}: it resolves to non-public address {address}"
                raise BlockedAddressError(msg)
        return results

    async def close(self) -> None:
        await self._inner.close()


def check_url_host(url: str) -> None:
    """Raise :class:`BlockedAddressError` if ``url`` has a literal non-public IP host."""
    address = _parse_ip(urlsplit(url).hostname or "")
    if address is not None and not is_public_address(address):
        msg = f"refusing to connect to non-public address {address}"
        raise BlockedAddressError(msg)


def _parse_ip(host: str) -> IPAddress | None:
    try:
        return ipaddress.ip_address(host.strip("[]").split("%", 1)[0])
    except ValueError:
        return None
