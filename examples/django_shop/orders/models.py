from django.db import models


class Order(models.Model):
    class Status(models.TextChoices):
        CREATED = "created"
        SHIPPED = "shipped"

    merchant_id = models.CharField(max_length=64, db_index=True)
    customer_email = models.EmailField()
    amount_cents = models.PositiveIntegerField()
    status = models.CharField(max_length=16, choices=Status.choices, default=Status.CREATED)
    created_at = models.DateTimeField(auto_now_add=True)
    shipped_at = models.DateTimeField(null=True, blank=True)

    def to_event(self) -> dict[str, object]:
        """The order as it appears in webhook payloads and API responses."""
        return {
            "id": self.pk,
            "status": self.status,
            "customer_email": self.customer_email,
            "amount_cents": self.amount_cents,
            "created_at": self.created_at,
            "shipped_at": self.shipped_at,
        }
