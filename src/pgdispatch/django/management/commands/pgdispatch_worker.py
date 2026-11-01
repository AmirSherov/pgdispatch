from __future__ import annotations

import asyncio
import logging
from typing import Any

from django.core.management.base import BaseCommand, CommandParser

from pgdispatch.django.client import get_webhooks
from pgdispatch.worker.runner import run_until_signalled


class Command(BaseCommand):
    help = "Deliver pgdispatch webhooks until interrupted (SIGINT/SIGTERM)."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument("--concurrency", type=int, help="Simultaneous HTTP requests.")
        parser.add_argument("--batch-size", type=int, help="Deliveries claimed per query.")
        parser.add_argument(
            "--metrics-port", type=int, help="Serve Prometheus metrics on this port."
        )

    def handle(self, *args: Any, **options: Any) -> None:
        if not logging.getLogger().handlers:
            logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
        overrides = {
            name: options[name]
            for name in ("concurrency", "batch_size")
            if options.get(name) is not None
        }
        worker = get_webhooks().worker(**overrides)
        asyncio.run(run_until_signalled(worker, metrics_port=options.get("metrics_port")))
