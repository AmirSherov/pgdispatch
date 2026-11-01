"""The endpoint-management router. See :mod:`pgdispatch.fastapi`."""

# FastAPI inspects annotations at runtime, and the route handlers below are
# closures over the ``owner`` dependency, so annotations must stay eager here.

import uuid
from collections.abc import Awaitable, Callable, Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any

try:
    from fastapi import APIRouter, Depends, HTTPException, Query, status
except ImportError as exc:  # pragma: no cover - depends on the environment
    msg = 'pgdispatch.fastapi requires FastAPI: pip install "pgdispatch[fastapi]"'
    raise ImportError(msg) from exc
from pydantic import BaseModel, ConfigDict, Field

from pgdispatch.client import Webhooks
from pgdispatch.errors import ConflictError, NotFoundError, ValidationError
from pgdispatch.models import CircuitState, DeliveryStatus

__all__ = ["router"]

# Starlette renamed the 422 constant; the number is stable across versions.
_UNPROCESSABLE = 422

OwnerDependency = Callable[..., Awaitable[str | None] | str | None]


class EndpointCreate(BaseModel):
    url: str = Field(examples=["https://shop.example.com/webhooks"])
    event_types: list[str] = Field(default=["*"], examples=[["order.*", "payment.paid"]])
    description: str | None = None
    headers: dict[str, str] = Field(default_factory=dict)
    rate_limit_rps: float | None = Field(default=None, gt=0)


class EndpointUpdate(BaseModel):
    """Only the fields present in the request are changed."""

    url: str | None = None
    event_types: list[str] | None = None
    description: str | None = None
    headers: dict[str, str] | None = None
    rate_limit_rps: float | None = Field(default=None, gt=0)


class EndpointOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    url: str
    description: str | None
    event_types: list[str]
    headers: dict[str, str]
    rate_limit_rps: float | None
    is_enabled: bool
    disabled_reason: str | None
    circuit_state: CircuitState
    created_at: datetime
    updated_at: datetime


class EndpointWithSecret(EndpointOut):
    secret: str = Field(description="The signing secret. Store it now; it is shown only once.")


class SecretOut(BaseModel):
    secret: str


class RotateSecret(BaseModel):
    grace_period_seconds: int = Field(default=24 * 3600, ge=0, le=7 * 24 * 3600)


class TestEventOut(BaseModel):
    event_id: uuid.UUID


class AttemptOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    attempt_number: int
    response_status: int | None
    response_body: str | None
    duration_ms: int | None
    error: str | None
    created_at: datetime


class DeliveryOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    event_id: uuid.UUID
    endpoint_id: uuid.UUID
    event_type: str
    status: DeliveryStatus
    attempt_count: int
    next_attempt_at: datetime
    last_status_code: int | None
    last_error: str | None
    created_at: datetime
    finished_at: datetime | None


class DeliveryDetail(DeliveryOut):
    attempts: list[AttemptOut]


def router(
    webhooks: Webhooks,
    *,
    owner: OwnerDependency,
    tags: Sequence[str] = ("webhooks",),
) -> APIRouter:
    """Build an :class:`~fastapi.APIRouter` for managing endpoints and deliveries.

    :param owner: A FastAPI dependency that authenticates the request and
        returns the owner id (tenant) to scope every operation to.
    """
    api = APIRouter(tags=list(tags))
    owner_id = Depends(owner)

    @api.get("/endpoints", response_model=list[EndpointOut])
    async def list_endpoints(
        after: uuid.UUID | None = None,
        limit: int = Query(default=100, ge=1, le=1000),
        owner_id: str | None = owner_id,
    ) -> Any:
        return await webhooks.endpoints.list(owner_id=owner_id, after=after, limit=limit)

    @api.post("/endpoints", response_model=EndpointWithSecret, status_code=status.HTTP_201_CREATED)
    async def create_endpoint(body: EndpointCreate, owner_id: str | None = owner_id) -> Any:
        with _errors():
            return await webhooks.endpoints.create(
                body.url,
                event_types=body.event_types,
                owner_id=owner_id,
                description=body.description,
                headers=body.headers,
                rate_limit_rps=body.rate_limit_rps,
            )

    @api.get("/endpoints/{endpoint_id}", response_model=EndpointOut)
    async def get_endpoint(endpoint_id: uuid.UUID, owner_id: str | None = owner_id) -> Any:
        with _errors():
            return await webhooks.endpoints.get(endpoint_id, owner_id=owner_id)

    @api.patch("/endpoints/{endpoint_id}", response_model=EndpointOut)
    async def update_endpoint(
        endpoint_id: uuid.UUID, body: EndpointUpdate, owner_id: str | None = owner_id
    ) -> Any:
        changes = body.model_dump(exclude_unset=True)
        if changes.get("url", "") is None or changes.get("event_types", []) is None:
            raise HTTPException(_UNPROCESSABLE, "url and event_types cannot be null")
        if "headers" in changes and changes["headers"] is None:
            changes["headers"] = {}
        with _errors():
            return await webhooks.endpoints.update(endpoint_id, owner_id=owner_id, **changes)

    @api.delete("/endpoints/{endpoint_id}", status_code=status.HTTP_204_NO_CONTENT)
    async def delete_endpoint(endpoint_id: uuid.UUID, owner_id: str | None = owner_id) -> None:
        with _errors():
            await webhooks.endpoints.delete(endpoint_id, owner_id=owner_id)

    @api.post("/endpoints/{endpoint_id}/enable", response_model=EndpointOut)
    async def enable_endpoint(endpoint_id: uuid.UUID, owner_id: str | None = owner_id) -> Any:
        with _errors():
            return await webhooks.endpoints.enable(endpoint_id, owner_id=owner_id)

    @api.post("/endpoints/{endpoint_id}/disable", response_model=EndpointOut)
    async def disable_endpoint(endpoint_id: uuid.UUID, owner_id: str | None = owner_id) -> Any:
        with _errors():
            return await webhooks.endpoints.disable(
                endpoint_id, reason="disabled by the owner", owner_id=owner_id
            )

    @api.get("/endpoints/{endpoint_id}/secret", response_model=SecretOut)
    async def get_secret(endpoint_id: uuid.UUID, owner_id: str | None = owner_id) -> Any:
        with _errors():
            return {"secret": await webhooks.endpoints.get_secret(endpoint_id, owner_id=owner_id)}

    @api.post("/endpoints/{endpoint_id}/rotate-secret", response_model=EndpointWithSecret)
    async def rotate_secret(
        endpoint_id: uuid.UUID, body: RotateSecret | None = None, owner_id: str | None = owner_id
    ) -> Any:
        grace = timedelta(seconds=(body or RotateSecret()).grace_period_seconds)
        with _errors():
            return await webhooks.endpoints.rotate_secret(
                endpoint_id, grace_period=grace, owner_id=owner_id
            )

    @api.post("/endpoints/{endpoint_id}/test", response_model=TestEventOut, status_code=202)
    async def send_test(endpoint_id: uuid.UUID, owner_id: str | None = owner_id) -> Any:
        with _errors():
            result = await webhooks.endpoints.send_test(endpoint_id, owner_id=owner_id)
        return {"event_id": result.event_id}

    @api.get("/deliveries", response_model=list[DeliveryOut])
    async def list_deliveries(
        endpoint_id: uuid.UUID | None = None,
        status_filter: DeliveryStatus | None = Query(default=None, alias="status"),
        before: int | None = None,
        limit: int = Query(default=50, ge=1, le=1000),
        owner_id: str | None = owner_id,
    ) -> Any:
        return await webhooks.deliveries.list(
            endpoint_id=endpoint_id,
            status=status_filter,
            before=before,
            limit=limit,
            owner_id=owner_id,
        )

    @api.get("/deliveries/{delivery_id}", response_model=DeliveryDetail)
    async def get_delivery(delivery_id: int, owner_id: str | None = owner_id) -> Any:
        with _errors():
            delivery = await webhooks.deliveries.get(delivery_id, owner_id=owner_id)
            attempts = await webhooks.deliveries.attempts(delivery_id, owner_id=owner_id)
        detail = DeliveryOut.model_validate(delivery).model_dump()
        return {**detail, "attempts": [AttemptOut.model_validate(a) for a in attempts]}

    @api.post("/deliveries/{delivery_id}/retry", response_model=DeliveryOut)
    async def retry_delivery(delivery_id: int, owner_id: str | None = owner_id) -> Any:
        with _errors():
            return await webhooks.deliveries.retry(delivery_id, owner_id=owner_id)

    return api


@contextmanager
def _errors() -> Iterator[None]:
    """Translate library errors into HTTP responses."""
    try:
        yield
    except NotFoundError as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, str(exc)) from exc
    except ConflictError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    except ValidationError as exc:
        raise HTTPException(_UNPROCESSABLE, str(exc)) from exc
