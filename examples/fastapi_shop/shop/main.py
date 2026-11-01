"""A tiny multi-merchant shop that notifies its merchants about orders via webhooks.

Run it with ``uvicorn shop.main:app``. Configuration comes from the environment:

``DATABASE_URL``
    e.g. ``postgresql+asyncpg://shop:shop@localhost:5432/shop``.
``RUN_WORKER``
    ``true`` (default) delivers webhooks from inside this process; set it to
    ``false`` when you run ``pgdispatch worker`` as a separate service.
``PGDISPATCH_*``
    Any pgdispatch setting, e.g. ``PGDISPATCH_CONCURRENCY=100``.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Annotated

from fastapi import Depends, FastAPI, Header, HTTPException, status
from prometheus_client import make_asgi_app
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from pgdispatch import Webhooks
from pgdispatch.fastapi import router as webhooks_router
from pgdispatch.sqlalchemy import SQLAlchemyBackend

from .models import Base, Order

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/postgres"
)
RUN_WORKER = os.environ.get("RUN_WORKER", "true").lower() in {"1", "true", "yes"}

engine = create_async_engine(DATABASE_URL)
Session = async_sessionmaker(engine, expire_on_commit=False)
webhooks = Webhooks(SQLAlchemyBackend(engine))


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    # A real application would use Alembic; see pgdispatch.sqlalchemy.run_migrations.
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    # Applies the pgdispatch migrations and, unless disabled, runs a worker
    # in the background until the application shuts down.
    try:
        async with webhooks.lifespan(migrate=True, run_worker=RUN_WORKER)(app):
            yield
    finally:
        await engine.dispose()


async def current_merchant(x_merchant_id: Annotated[str, Header()]) -> str:
    """Identify the caller.

    A stand-in for real authentication: it trusts the ``X-Merchant-Id`` header.
    In your application, derive the merchant from a session or an API key.
    """
    return x_merchant_id


async def get_session() -> AsyncIterator[AsyncSession]:
    async with Session() as session:
        yield session


Merchant = Annotated[str, Depends(current_merchant)]
DBSession = Annotated[AsyncSession, Depends(get_session)]


class OrderIn(BaseModel):
    customer_email: str = Field(max_length=254)
    amount_cents: int = Field(gt=0)
    currency: str = Field(default="USD", pattern="^[A-Z]{3}$")


class OrderOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    status: str
    customer_email: str
    amount_cents: int
    currency: str
    created_at: datetime
    paid_at: datetime | None


app = FastAPI(title="pgdispatch example shop", lifespan=lifespan)

# Lets merchants manage their endpoints and inspect deliveries; every call is
# scoped to the merchant returned by `current_merchant`.
app.include_router(webhooks_router(webhooks, owner=current_merchant), prefix="/webhooks")

# Prometheus metrics, including the pgdispatch_* ones of the in-process worker.
app.mount("/metrics", make_asgi_app())


@app.post("/orders", response_model=OrderOut, status_code=status.HTTP_201_CREATED)
async def create_order(body: OrderIn, merchant: Merchant, session: DBSession) -> Order:
    async with session.begin():
        order = Order(merchant_id=merchant, **body.model_dump())
        session.add(order)
        await session.flush()  # assigns order.id
        # Same transaction as the INSERT above: the event exists if, and only
        # if, the order does.
        await webhooks.publish(
            session,
            "order.created",
            order.to_event(),
            owner_id=merchant,
            ordering_key=f"order:{order.id}",
        )
    return order


@app.post("/orders/{order_id}/pay", response_model=OrderOut)
async def pay_order(order_id: int, merchant: Merchant, session: DBSession) -> Order:
    async with session.begin():
        order = await session.get(Order, order_id, with_for_update=True)
        if order is None or order.merchant_id != merchant:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "order not found")
        if order.status == "paid":
            return order  # paying twice is a no-op
        order.status = "paid"
        order.paid_at = datetime.now(timezone.utc)
        await webhooks.publish(
            session,
            "order.paid",
            order.to_event(),
            owner_id=merchant,
            # Events with the same key reach each endpoint in publish order:
            # never `order.paid` before `order.created`.
            ordering_key=f"order:{order.id}",
            # A second `order.paid` for the same order is never published.
            idempotency_key=f"order:{order.id}:paid",
        )
    return order
