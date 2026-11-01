"""``python manage.py create_endpoint URL``: subscribe a URL to webhooks and print its secret.

A real application would offer this in its merchant dashboard or API.
"""

from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser
from pgdispatch import PgDispatchError
from pgdispatch.django import webhooks


class Command(BaseCommand):
    help = "Register a webhook endpoint and print its id and signing secret."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("url")
        parser.add_argument("--owner", required=True, help="merchant id, e.g. merchant_1")
        parser.add_argument(
            "--events", nargs="+", default=["*"], help="event type patterns, e.g. 'order.*'"
        )
        parser.add_argument("--secret", help="use this whsec_ secret instead of a random one")

    def handle(self, *args: Any, **options: Any) -> None:
        try:
            endpoint = webhooks.endpoints.create(
                options["url"],
                owner_id=options["owner"],
                event_types=options["events"],
                secret=options["secret"],
            )
        except PgDispatchError as exc:  # an unsafe URL, a malformed secret, ...
            raise CommandError(str(exc)) from exc
        self.stdout.write(f"endpoint id: {endpoint.id}")
        self.stdout.write(f"secret:      {endpoint.secret}")
