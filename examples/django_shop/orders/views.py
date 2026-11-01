"""A JSON API for merchants: create orders and mark them as shipped."""

from __future__ import annotations

import json
from collections.abc import Callable
from functools import wraps

from django import forms
from django.db import transaction
from django.http import HttpRequest, JsonResponse
from django.utils import timezone
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from pgdispatch.django import webhooks

from orders.models import Order

MerchantView = Callable[..., JsonResponse]


class OrderForm(forms.ModelForm):
    class Meta:
        model = Order
        fields = ("customer_email", "amount_cents")


def merchant_api(view: MerchantView) -> Callable[..., JsonResponse]:
    """Turn ``view(request, merchant_id, ...)`` into a JSON POST endpoint.

    The merchant is taken from the ``X-Merchant-Id`` header: a stand-in for
    real authentication, which would also make CSRF protection unnecessary.
    """

    @csrf_exempt
    @require_POST
    @wraps(view)
    def wrapper(request: HttpRequest, *args: object, **kwargs: object) -> JsonResponse:
        merchant_id = request.headers.get("X-Merchant-Id")
        if not merchant_id:
            return JsonResponse({"error": "missing X-Merchant-Id header"}, status=401)
        return view(request, merchant_id, *args, **kwargs)

    return wrapper


@merchant_api
def create_order(request: HttpRequest, merchant_id: str) -> JsonResponse:
    form = OrderForm(json.loads(request.body or b"{}"))
    if not form.is_valid():
        return JsonResponse({"errors": form.errors}, status=400)
    with transaction.atomic():
        order = form.save(commit=False)
        order.merchant_id = merchant_id
        order.save()
        # Same transaction as the INSERT: the event exists if, and only if,
        # the order does. pgdispatch_worker delivers it after the commit.
        webhooks.publish(
            "order.created",
            order.to_event(),
            owner_id=merchant_id,
            ordering_key=f"order:{order.pk}",
        )
    return JsonResponse(order.to_event(), status=201)


@merchant_api
def ship_order(request: HttpRequest, merchant_id: str, order_id: int) -> JsonResponse:
    with transaction.atomic():
        order = (
            Order.objects.select_for_update().filter(pk=order_id, merchant_id=merchant_id).first()
        )
        if order is None:
            return JsonResponse({"error": "order not found"}, status=404)
        if order.status != Order.Status.SHIPPED:
            order.status = Order.Status.SHIPPED
            order.shipped_at = timezone.now()
            order.save(update_fields=["status", "shipped_at"])
            webhooks.publish(
                "order.shipped",
                order.to_event(),
                owner_id=merchant_id,
                # Events with the same key reach each endpoint in publish
                # order: never `order.shipped` before `order.created`.
                ordering_key=f"order:{order.pk}",
                # A second `order.shipped` for the same order is never published.
                idempotency_key=f"order:{order.pk}:shipped",
            )
    return JsonResponse(order.to_event())
