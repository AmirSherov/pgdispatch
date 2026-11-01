from __future__ import annotations

from datetime import timedelta

from aiohttp import web

from pgdispatch import DeliveryStatus, Webhooks, verify_signature
from tests.conftest import Receiver, WebhooksFactory, wait_until


async def delivered(webhooks: Webhooks, count: int) -> bool:
    succeeded = await webhooks.deliveries.list(status=DeliveryStatus.SUCCEEDED, limit=1000)
    return len(succeeded) >= count


async def test_published_event_is_delivered_signed(webhooks: Webhooks, receiver: Receiver) -> None:
    endpoint = await webhooks.endpoints.create(receiver.url(), event_types=["payment.*"])
    result = await webhooks.publish(None, "payment.paid", {"id": 42, "amount": 15000})
    assert result.deliveries == 1

    async with webhooks.running_worker():
        await wait_until(lambda: len(receiver.requests) == 1)
        await wait_until(lambda: delivered(webhooks, 1))

    request = receiver.requests[0]
    assert endpoint.secret is not None
    verify_signature(endpoint.secret, request.headers, request.body)
    assert request.headers["webhook-id"] == str(result.event_id)
    assert request.headers["Content-Type"] == "application/json"
    body = request.json
    assert body["type"] == "payment.paid"
    assert body["data"] == {"id": 42, "amount": 15000}
    assert body["timestamp"].endswith("Z")

    [delivery] = await webhooks.deliveries.list(event_id=result.event_id)
    assert delivery.status is DeliveryStatus.SUCCEEDED
    assert delivery.attempt_count == 1
    assert delivery.last_status_code == 200
    [attempt] = await webhooks.deliveries.attempts(delivery.id)
    assert attempt.succeeded
    assert attempt.response_body == "ok"
    assert attempt.request_headers["webhook-id"] == str(result.event_id)


async def test_failed_attempts_are_retried_on_schedule(
    make_webhooks: WebhooksFactory, receiver: Receiver
) -> None:
    webhooks = await make_webhooks(
        retry_schedule=(timedelta(0), timedelta(milliseconds=200), timedelta(milliseconds=200)),
    )
    statuses = iter([503, 500])
    receiver.responder = lambda _: web.Response(status=next(statuses, 200))
    await webhooks.endpoints.create(receiver.url())
    result = await webhooks.publish(None, "order.created", {"id": 1})

    async with webhooks.running_worker():
        await wait_until(lambda: delivered(webhooks, 1))

    [delivery] = await webhooks.deliveries.list(event_id=result.event_id)
    attempts = await webhooks.deliveries.attempts(delivery.id)
    assert [a.response_status for a in attempts] == [503, 500, 200]
    assert [a.attempt_number for a in attempts] == [1, 2, 3]
    assert len({r.headers["webhook-id"] for r in receiver.requests}) == 1


async def test_delivery_fails_after_last_attempt(
    make_webhooks: WebhooksFactory, receiver: Receiver
) -> None:
    webhooks = await make_webhooks(retry_schedule=(timedelta(0), timedelta(milliseconds=100)))
    failed = []
    webhooks.hooks.on_delivery_failed(failed.append)
    receiver.responder = lambda _: web.Response(status=500, text="boom")
    await webhooks.endpoints.create(receiver.url())
    result = await webhooks.publish(None, "order.created", {"id": 1})

    async with webhooks.running_worker():
        await wait_until(lambda: len(failed) == 1)

    [delivery] = await webhooks.deliveries.list(event_id=result.event_id)
    assert delivery.status is DeliveryStatus.FAILED
    assert delivery.attempt_count == 2
    assert delivery.last_error == "HTTP 500"
    assert failed[0].id == delivery.id


async def test_manual_retry_restarts_the_schedule(
    make_webhooks: WebhooksFactory, receiver: Receiver
) -> None:
    webhooks = await make_webhooks(retry_schedule=(timedelta(0),))
    receiver.responder = lambda _: web.Response(status=500)
    await webhooks.endpoints.create(receiver.url())
    result = await webhooks.publish(None, "order.created", {"id": 1})

    async with webhooks.running_worker():
        await wait_until(lambda: len(receiver.requests) == 1)
        [delivery] = await webhooks.deliveries.list(event_id=result.event_id)
        await wait_until(lambda: _status(webhooks, delivery.id, DeliveryStatus.FAILED))
        receiver.responder = None
        await webhooks.deliveries.retry(delivery.id)
        await wait_until(lambda: _status(webhooks, delivery.id, DeliveryStatus.SUCCEEDED))

    attempts = await webhooks.deliveries.attempts(delivery.id)
    assert [a.attempt_number for a in attempts] == [1, 2]


async def test_gone_disables_endpoint_and_cancels_delivery(
    webhooks: Webhooks, receiver: Receiver
) -> None:
    disabled = []
    webhooks.hooks.on_endpoint_disabled(lambda endpoint, reason: disabled.append(reason))
    receiver.responder = lambda _: web.Response(status=410)
    endpoint = await webhooks.endpoints.create(receiver.url())
    result = await webhooks.publish(None, "order.created", {"id": 1})

    async with webhooks.running_worker():
        await wait_until(lambda: len(disabled) == 1)

    [delivery] = await webhooks.deliveries.list(event_id=result.event_id)
    assert delivery.status is DeliveryStatus.CANCELLED
    endpoint = await webhooks.endpoints.get(endpoint.id)
    assert not endpoint.is_enabled
    assert endpoint.disabled_reason == "the endpoint answered 410 Gone"


async def test_retry_after_header_is_respected(
    make_webhooks: WebhooksFactory, receiver: Receiver
) -> None:
    webhooks = await make_webhooks(
        retry_schedule=(timedelta(0), timedelta(milliseconds=10), timedelta(seconds=5)),
    )
    responses = iter([web.Response(status=429, headers={"Retry-After": "1"})])
    receiver.responder = lambda _: next(responses, web.Response(text="ok"))
    await webhooks.endpoints.create(receiver.url())
    await webhooks.publish(None, "order.created", {"id": 1})

    async with webhooks.running_worker():
        await wait_until(lambda: len(receiver.requests) == 2, timeout=5)

    gap = receiver.requests[1].received_at - receiver.requests[0].received_at
    assert gap >= 0.9


async def test_redirects_are_not_followed(webhooks: Webhooks, receiver: Receiver) -> None:
    receiver.responder = lambda request: (
        web.Response(status=302, headers={"Location": receiver.url("/elsewhere")})
        if request.path == "/hooks"
        else web.Response(text="ok")
    )
    await webhooks.endpoints.create(receiver.url())
    result = await webhooks.publish(None, "order.created", {"id": 1})

    async with webhooks.running_worker():
        await wait_until(lambda: len(receiver.requests) >= 1)
        [delivery] = await webhooks.deliveries.list(event_id=result.event_id)
        await wait_until(lambda: _attempted(webhooks, delivery.id))

    assert receiver.for_path("/elsewhere") == []
    [attempt, *_] = await webhooks.deliveries.attempts(delivery.id)
    assert attempt.response_status == 302
    assert "redirect" in (attempt.error or "")


async def test_custom_headers_are_sent_and_redacted_in_log(
    webhooks: Webhooks, receiver: Receiver
) -> None:
    await webhooks.endpoints.create(receiver.url(), headers={"X-Api-Key": "s3cr3t"})
    result = await webhooks.publish(None, "order.created", {"id": 1})

    async with webhooks.running_worker():
        await wait_until(lambda: delivered(webhooks, 1))

    assert receiver.requests[0].headers["X-Api-Key"] == "s3cr3t"
    [delivery] = await webhooks.deliveries.list(event_id=result.event_id)
    [attempt] = await webhooks.deliveries.attempts(delivery.id)
    assert attempt.request_headers["x-api-key"] == "[redacted]"


async def test_events_reach_only_subscribed_endpoints_of_the_owner(
    webhooks: Webhooks, receiver: Receiver
) -> None:
    await webhooks.endpoints.create(receiver.url("/a"), event_types=["order.*"], owner_id="a")
    await webhooks.endpoints.create(receiver.url("/b"), event_types=["*"], owner_id="b")
    await webhooks.endpoints.create(receiver.url("/a-pay"), event_types=["payment.*"], owner_id="a")
    await webhooks.endpoints.create(receiver.url("/global"), event_types=["*"])

    result = await webhooks.publish(None, "order.shipped", {"id": 1}, owner_id="a")
    assert result.deliveries == 1
    result = await webhooks.publish(None, "order.shipped", {"id": 2})
    assert result.deliveries == 1

    async with webhooks.running_worker():
        await wait_until(lambda: len(receiver.requests) == 2)

    assert {r.path for r in receiver.requests} == {"/a", "/global"}


async def test_rate_limited_endpoint(make_webhooks: WebhooksFactory, receiver: Receiver) -> None:
    webhooks = await make_webhooks()
    await webhooks.endpoints.create(receiver.url(), rate_limit_rps=10)
    for number in range(15):
        await webhooks.publish(None, "tick", {"n": number})

    async with webhooks.running_worker():
        await wait_until(lambda: len(receiver.requests) == 15, timeout=10)

    times = sorted(r.received_at for r in receiver.requests)
    # Burst of 10, then 10 per second.
    assert times[-1] - times[0] >= 0.4


async def _status(webhooks: Webhooks, delivery_id: int, status: DeliveryStatus) -> bool:
    return (await webhooks.deliveries.get(delivery_id)).status is status


async def _attempted(webhooks: Webhooks, delivery_id: int) -> bool:
    return bool(await webhooks.deliveries.attempts(delivery_id))
