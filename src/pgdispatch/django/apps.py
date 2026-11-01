from __future__ import annotations

from django.apps import AppConfig


class PgDispatchConfig(AppConfig):
    name = "pgdispatch.django"
    label = "pgdispatch"
    verbose_name = "pgdispatch webhooks"
    default_auto_field = "django.db.models.BigAutoField"
