"""The shop's own data: a single ``orders`` table."""

from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import DateTime, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Order(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(primary_key=True)
    merchant_id: Mapped[str] = mapped_column(String(64), index=True)
    customer_email: Mapped[str] = mapped_column(String(254))
    amount_cents: Mapped[int]
    currency: Mapped[str] = mapped_column(String(3))
    status: Mapped[str] = mapped_column(String(16), default="created")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    paid_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    def to_event(self) -> dict[str, object]:
        """The order as it appears in webhook payloads."""
        return {
            "id": self.id,
            "status": self.status,
            "customer_email": self.customer_email,
            "amount_cents": self.amount_cents,
            "currency": self.currency,
            "paid_at": self.paid_at,
        }
