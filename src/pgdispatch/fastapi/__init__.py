"""FastAPI integration: a ready-made API for your customers to manage their endpoints.

::

    from fastapi import Depends, FastAPI
    from pgdispatch.fastapi import router

    async def current_merchant(user: User = Depends(current_user)) -> str:
        return user.merchant_id  # every request is scoped to this owner

    app = FastAPI(lifespan=webhooks.lifespan())
    app.include_router(router(webhooks, owner=current_merchant), prefix="/webhooks")

The ``owner`` dependency is mandatory: it authenticates the caller and returns
the owner id that every operation is restricted to, so one customer can never
see or touch another customer's endpoints.
"""

from __future__ import annotations

from pgdispatch.fastapi.router import router

__all__ = ["router"]
