from __future__ import annotations

import ipaddress
import socket

import pytest
from aiohttp.abc import AbstractResolver, ResolveResult

from pgdispatch import UnsafeURLError
from pgdispatch.security.ssrf import (
    BlockedAddressError,
    SafeResolver,
    check_url_host,
    is_public_address,
    validate_endpoint_url,
)

NON_PUBLIC = [
    "127.0.0.1",
    "10.0.0.5",
    "172.16.3.4",
    "192.168.1.1",
    "169.254.169.254",
    "100.64.0.1",
    "0.0.0.0",
    "224.0.0.1",
    "255.255.255.255",
    "192.0.2.10",
    "::1",
    "::",
    "fe80::1",
    "fc00::1",
    "ff02::1",
    "::ffff:127.0.0.1",
    "::ffff:10.0.0.1",
    "2002:7f00:1::",  # 6to4 wrapping 127.0.0.1
    "2001:db8::1",
]
PUBLIC = ["8.8.8.8", "1.1.1.1", "93.184.216.34", "2606:4700:4700::1111", "::ffff:8.8.8.8"]


@pytest.mark.parametrize("address", NON_PUBLIC)
def test_non_public_addresses(address: str) -> None:
    assert not is_public_address(ipaddress.ip_address(address))


@pytest.mark.parametrize("address", PUBLIC)
def test_public_addresses(address: str) -> None:
    assert is_public_address(ipaddress.ip_address(address))


@pytest.mark.parametrize(
    "url",
    [
        "https://127.0.0.1/hooks",
        "https://[::1]:8443/",
        "https://169.254.169.254/latest/meta-data",
        "https://localhost/",
        "https://api.localhost/",
        "https://db.internal/",
        "http://example.com/",
        "https://user:secret@example.com/",
        "https:///path-only",
        "javascript:alert(1)",
        "https://example.com:0/",
        "https://example.com:99999/",
        "https://" + "a" * 3000 + ".com/",
    ],
)
def test_unsafe_urls(url: str) -> None:
    with pytest.raises(UnsafeURLError):
        validate_endpoint_url(url)


def test_safe_urls_and_development_flags() -> None:
    assert validate_endpoint_url("https://shop.example.com/hooks?x=1")
    assert validate_endpoint_url("https://93.184.216.34:8443/")
    assert validate_endpoint_url("http://example.com/", allow_http=True)
    assert validate_endpoint_url("https://127.0.0.1/", allow_private_ips=True)


def test_check_url_host() -> None:
    check_url_host("https://example.com/")
    check_url_host("https://8.8.8.8/")
    for url in ("https://127.0.0.1/", "http://[::1]:80/", "https://10.1.2.3/"):
        with pytest.raises(BlockedAddressError):
            check_url_host(url)


class FakeResolver(AbstractResolver):
    def __init__(self, *addresses: str) -> None:
        self.addresses = addresses

    async def resolve(
        self, host: str, port: int = 0, family: socket.AddressFamily = socket.AF_INET
    ) -> list[ResolveResult]:
        return [
            {
                "hostname": host,
                "host": address,
                "port": port,
                "family": family,
                "proto": 0,
                "flags": 0,
            }
            for address in self.addresses
        ]

    async def close(self) -> None:
        pass


async def test_safe_resolver_passes_public_addresses() -> None:
    resolver = SafeResolver(FakeResolver("93.184.216.34"))
    [result] = await resolver.resolve("example.com", 443)
    assert result["host"] == "93.184.216.34"


@pytest.mark.parametrize("addresses", [("10.0.0.1",), ("93.184.216.34", "127.0.0.1"), ("::1",)])
async def test_safe_resolver_blocks_any_private_address(addresses: tuple[str, ...]) -> None:
    resolver = SafeResolver(FakeResolver(*addresses))
    with pytest.raises(BlockedAddressError):
        await resolver.resolve("rebind.example.com", 443)
