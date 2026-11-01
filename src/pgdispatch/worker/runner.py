"""Running a worker as the main program of a process (CLI, Django command)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal

from pgdispatch.worker.worker import Worker

__all__ = ["run_until_signalled"]

logger = logging.getLogger("pgdispatch.worker")


async def run_until_signalled(worker: Worker, *, metrics_port: int | None = None) -> None:
    """Run ``worker`` until SIGINT or SIGTERM, then shut it down gracefully."""
    if metrics_port is not None:
        from prometheus_client import start_http_server

        start_http_server(metrics_port)
        logger.info("pgdispatch metrics are served on port %d", metrics_port)

    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, worker.stop)
        except (NotImplementedError, RuntimeError):  # pragma: no cover - Windows
            signal.signal(signum, lambda *_: loop.call_soon_threadsafe(worker.stop))
    try:
        await worker.run()
    finally:
        for signum in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):
                loop.remove_signal_handler(signum)
