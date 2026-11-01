from __future__ import annotations

import uuid
from datetime import timedelta

import asyncpg
import pytest
from aiohttp import web

from pgdispatch import (
    ConflictError,
    DeliveryStatus,
    InvalidSignatureError,
    NotFoundError,
    UnsafeURLError,
    ValidationError,
    Webhooks,
    generate_secret,
    verify_signature,
)
from tests.conftest import Receiver, WebhooksFactory, wait_until


async def test_endpoint_crud(webhooks: Webhooks) -> None:
    endpoint = await webhooks.endpoints.create(
        "https://example.com/hooks",
        event_types=["payment.*", "order.shipped"],
        owner_id="merchant_42",
        description="Shop",
        headers={"X-Api-Key": "k"},
        rate_limit_rps=5,
    )
    assert endpoint.secret is not None and endpoint.secret.startswith("whsec_")
    assert "secret" not in repr(endpoint)

    fetched = await webhooks.endpoints.get(endpoint.id)
    assert fetched.secret is None
    assert fetched.event_types == ("payment.*", "order.shipped")
    assert fetched.headers == {"X-Api-Key": "k"}
    assert await webhooks.endpoints.get_secret(endpoint.id) == endpoint.secret

    updated = await webhooks.endpoints.update(
        endpoint.id, url="https://example.com/v2", description=None, rate_limit_rps=None
    )
    assert updated.url == "https://example.com/v2"
    assert updated.description is None
    assert updated.rate_limit_rps is None
    assert updated.event_types == fetched.event_types

    disabled = await webhooks.endpoints.disable(endpoint.id, reason="maintenance")
    assert not disabled.is_enabled
    assert disabled.disabled_reason == "maintenance"
    assert [e.id for e in await webhooks.endpoints.list(enabled=False)] == [endpoint.id]

    await webhooks.endpoints.delete(endpoint.id)
    with pytest.raises(NotFoundError):
        await webhooks.endpoints.get(endpoint.id)


async def test_owner_scoping_hides_other_tenants(webhooks: Webhooks) -> None:
    endpoint = await webhooks.endpoints.create("https://example.com/a", owner_id="a")
    await webhooks.endpoints.create("https://example.com/b", owner_id="b")

    assert [e.id for e in await webhooks.endpoints.list(owner_id="a")] == [endpoint.id]
    assert len(await webhooks.endpoints.list()) == 2
    with pytest.raises(NotFoundError):
        await webhooks.endpoints.get(endpoint.id, owner_id="b")
    with pytest.raises(NotFoundError):
        await webhooks.endpoints.delete(endpoint.id, owner_id="b")
    with pytest.raises(NotFoundError):
        await webhooks.endpoints.get(endpoint.id, owner_id=None)


async def test_list_endpoints_paginates(webhooks: Webhooks) -> None:
    created = {(await webhooks.endpoints.create(f"https://example.com/{n}")).id for n in range(5)}
    first = await webhooks.endpoints.list(limit=3)
    second = await webhooks.endpoints.list(limit=3, after=first[-1].id)
    assert len(first) == 3
    assert len(second) == 2
    assert {e.id for e in first + second} == created


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"url": "ftp://example.com"}, UnsafeURLError),
        ({"url": "https://user:pass@example.com"}, UnsafeURLError),
        ({"url": "not a url"}, UnsafeURLError),
        ({"url": "https://example.com", "event_types": []}, ValidationError),
        ({"url": "https://example.com", "event_types": "payment.*"}, ValidationError),
        ({"url": "https://example.com", "event_types": ["pay*"]}, ValidationError),
        ({"url": "https://example.com", "headers": {"webhook-id": "x"}}, ValidationError),
        ({"url": "https://example.com", "headers": {"X-A": "a\r\nb"}}, ValidationError),
        ({"url": "https://example.com", "rate_limit_rps": 0}, ValidationError),
        ({"url": "https://example.com", "secret": "whsec_short"}, ValidationError),
    ],
)
async def test_invalid_endpoints_are_rejected(
    webhooks: Webhooks, kwargs: dict[str, object], error: type[Exception]
) -> None:
    url = kwargs.pop("url")
    with pytest.raises(error):
        await webhooks.endpoints.create(url, **kwargs)  # type: ignore[arg-type]


async def test_private_urls_are_rejected_by_default(make_webhooks: WebhooksFactory) -> None:
    webhooks = await make_webhooks(allow_private_ips=False, allow_http=False)
    for url in ("https://127.0.0.1/", "https://10.0.0.5/", "https://[::1]/", "https://localhost/"):
        with pytest.raises(UnsafeURLError):
            await webhooks.endpoints.create(url)
    with pytest.raises(UnsafeURLError, match="http"):
        await webhooks.endpoints.create("http://example.com/")


async def test_rotated_secret_signs_with_both_keys_during_grace_period(
    webhooks: Webhooks, receiver: Receiver
) -> None:
    endpoint = await webhooks.endpoints.create(receiver.url())
    old_secret = endpoint.secret
    rotated = await webhooks.endpoints.rotate_secret(endpoint.id, grace_period=timedelta(hours=1))
    assert rotated.secret is not None and rotated.secret != old_secret
    assert await webhooks.endpoints.get_secret(endpoint.id) == rotated.secret

    await webhooks.publish(None, "order.created", {"id": 1})
    async with webhooks.running_worker():
        await wait_until(lambda: len(receiver.requests) == 1)

    request = receiver.requests[0]
    assert len(request.headers["webhook-signature"].split()) == 2
    assert old_secret is not None
    verify_signature(old_secret, request.headers, request.body)
    verify_signature(rotated.secret, request.headers, request.body)


async def test_rotation_without_grace_period_drops_old_secret(
    webhooks: Webhooks, receiver: Receiver
) -> None:
    endpoint = await webhooks.endpoints.create(receiver.url())
    new_secret = generate_secret()
    await webhooks.endpoints.rotate_secret(
        endpoint.id, grace_period=timedelta(0), secret=new_secret
    )
    await webhooks.publish(None, "order.created", {"id": 1})
    async with webhooks.running_worker():
        await wait_until(lambda: len(receiver.requests) == 1)

    request = receiver.requests[0]
    verify_signature(new_secret, request.headers, request.body)
    assert endpoint.secret is not None
    with pytest.raises(InvalidSignatureError):
        verify_signature(endpoint.secret, request.headers, request.body)


async def test_encrypted_secrets(
    make_webhooks: WebhooksFactory, receiver: Receiver, pool: asyncpg.Pool, schema: str
) -> None:
    from cryptography.fernet import Fernet

    webhooks = await make_webhooks(secret_encryption_keys=[Fernet.generate_key().decode()])
    endpoint = await webhooks.endpoints.create(receiver.url())
    stored = await pool.fetchval(f'SELECT secrets::text FROM "{schema}".endpoints')
    assert endpoint.secret is not None
    assert endpoint.secret not in stored
    assert await webhooks.endpoints.get_secret(endpoint.id) == endpoint.secret

    await webhooks.publish(None, "order.created", {"id": 1})
    async with webhooks.running_worker():
        await wait_until(lambda: len(receiver.requests) == 1)
    verify_signature(endpoint.secret, receiver.requests[0].headers, receiver.requests[0].body)


async def test_send_test_targets_one_endpoint(webhooks: Webhooks, receiver: Receiver) -> None:
    endpoint = await webhooks.endpoints.create(receiver.url("/one"), event_types=["never.sent"])
    await webhooks.endpoints.create(receiver.url("/two"))
    result = await webhooks.endpoints.send_test(endpoint.id)

    async with webhooks.running_worker():
        await wait_until(lambda: len(receiver.requests) == 1)

    assert receiver.requests[0].path == "/one"
    assert receiver.requests[0].json["type"] == "webhook.test"
    assert (await webhooks.events.get(result.event_id)).event_type == "webhook.test"

    await webhooks.endpoints.disable(endpoint.id)
    with pytest.raises(ConflictError):
        await webhooks.endpoints.send_test(endpoint.id)


async def test_replay_to_new_endpoint_and_to_one_endpoint(
    webhooks: Webhooks, receiver: Receiver
) -> None:
    result = await webhooks.publish(None, "order.created", {"id": 1}, owner_id="shop")
    assert result.deliveries == 0
    first = await webhooks.endpoints.create(receiver.url("/first"), owner_id="shop")
    await webhooks.endpoints.create(receiver.url("/second"), owner_id="shop")
    other = await webhooks.endpoints.create(receiver.url("/other"), owner_id="someone-else")

    assert len(await webhooks.events.replay(result.event_id)) == 2
    assert len(await webhooks.events.replay(result.event_id, endpoint_id=first.id)) == 1
    with pytest.raises(NotFoundError):
        await webhooks.events.replay(result.event_id, endpoint_id=other.id)
    with pytest.raises(NotFoundError):
        await webhooks.events.replay(result.event_id, owner_id="someone-else")
    with pytest.raises(NotFoundError):
        await webhooks.events.replay(uuid.uuid4())

    async with webhooks.running_worker():
        await wait_until(lambda: len(receiver.requests) == 3)

    assert sorted(r.path for r in receiver.requests) == ["/first", "/first", "/second"]
    assert {r.headers["webhook-id"] for r in receiver.requests} == {str(result.event_id)}


async def test_retry_all_and_cancel(make_webhooks: WebhooksFactory, receiver: Receiver) -> None:
    webhooks = await make_webhooks(retry_schedule=(timedelta(0),))
    receiver.responder = lambda _: web.Response(status=500)
    endpoint = await webhooks.endpoints.create(receiver.url())
    for number in range(3):
        await webhooks.publish(None, "tick", {"n": number})

    async with webhooks.running_worker():
        await wait_until(lambda: _count(webhooks, DeliveryStatus.FAILED, 3))
        receiver.responder = None
        assert await webhooks.deliveries.retry_all(endpoint_id=endpoint.id) == 3
        await wait_until(lambda: _count(webhooks, DeliveryStatus.SUCCEEDED, 3))

    succeeded = (await webhooks.deliveries.list())[0]
    with pytest.raises(ConflictError):
        await webhooks.deliveries.cancel(succeeded.id)

    await webhooks.publish(None, "tick", {})
    [pending] = await webhooks.deliveries.list(status="pending")
    cancelled = await webhooks.deliveries.cancel(pending.id)
    assert cancelled.status is DeliveryStatus.CANCELLED
    with pytest.raises(ValidationError):
        await webhooks.deliveries.list(status="bogus")


async def test_list_events_and_deliveries(webhooks: Webhooks) -> None:
    await webhooks.endpoints.create("https://example.com/hooks", owner_id="shop")
    ids = [
        (await webhooks.publish(None, "order.created", {"n": n}, owner_id="shop")).event_id
        for n in range(3)
    ]
    events = await webhooks.events.list(owner_id="shop")
    assert [e.id for e in events] == list(reversed(ids))
    page = await webhooks.events.list(owner_id="shop", before=events[0].id, limit=1)
    assert [e.id for e in page] == [ids[1]]
    assert await webhooks.events.list(event_type="other") == []

    deliveries = await webhooks.deliveries.list(owner_id="shop")
    assert len(deliveries) == 3
    assert all(d.event_type == "order.created" for d in deliveries)
    older = await webhooks.deliveries.list(before=deliveries[0].id)
    assert len(older) == 2
    assert await webhooks.deliveries.list(owner_id="nobody") == []


async def test_status_and_cleanup(webhooks: Webhooks, pool: asyncpg.Pool, schema: str) -> None:
    await webhooks.endpoints.create("https://example.com/hooks")
    for _ in range(3):
        await webhooks.publish(None, "tick", {})
    stats = await webhooks.status()
    assert stats.pending == 3
    assert stats.due == 3
    assert stats.endpoints_enabled == 1

    await pool.execute(f"UPDATE \"{schema}\".events SET created_at = now() - interval '30 days'")
    assert await webhooks.cleanup() == 3
    assert (await webhooks.status()).pending == 0


async def test_migrate_is_idempotent(webhooks: Webhooks) -> None:
    assert await webhooks.migrate() == []


async def _count(webhooks: Webhooks, status: DeliveryStatus, expected: int) -> bool:
    return len(await webhooks.deliveries.list(status=status)) == expected


async def test_lag_ignores_deliveries_held_back_on_purpose(
    webhooks: Webhooks, pool: asyncpg.Pool, schema: str
) -> None:
    await webhooks.endpoints.create("https://example.com/hooks")
    for _ in range(3):
        await webhooks.publish(None, "tick", {}, ordering_key="k")
    # The head of the chain is waiting for a retry in the future.
    await pool.execute(
        f"""UPDATE "{schema}".deliveries SET next_attempt_at = now() + interval '1 hour'
            WHERE id = (SELECT min(id) FROM "{schema}".deliveries)"""
    )
    await pool.execute(
        f"""UPDATE "{schema}".deliveries
            SET created_at = now() - interval '1 hour', next_attempt_at = now() - interval '1 hour'
            WHERE id <> (SELECT min(id) FROM "{schema}".deliveries)"""
    )
    stats = await webhooks.status()
    assert stats.pending == 3
    assert stats.due == 0
    assert stats.oldest_due_age == timedelta(0)

    await pool.execute(f"UPDATE \"{schema}\".endpoints SET circuit_state = 'open'")
    await pool.execute(f'UPDATE "{schema}".deliveries SET ordering_key = NULL')
    assert (await webhooks.status()).due == 0
