"""The ``pgdispatch`` command-line tool.

::

    pgdispatch migrate                         # create or upgrade the schema
    pgdispatch worker --concurrency 50         # deliver webhooks
    pgdispatch status                          # queue, lag, open circuit breakers
    pgdispatch deliveries --status failed --since 1h
    pgdispatch deliveries show 18342           # every attempt with responses
    pgdispatch retry 18342                     # retry one delivery
    pgdispatch retry --endpoint <id>           # retry all failed deliveries of an endpoint
    pgdispatch replay --event <id> [--endpoint <id>]
    pgdispatch endpoints list --owner merchant_42
    pgdispatch endpoints enable <id>           # enable and reset the circuit breaker
    pgdispatch cleanup --older-than 14d

The database is taken from ``--dsn``, ``PGDISPATCH_DSN`` or ``DATABASE_URL``;
every other setting from ``PGDISPATCH_*`` environment variables.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import logging
import os
import sys
import uuid
from collections.abc import Callable, Coroutine, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any, TextIO

import asyncpg

from pgdispatch._version import __version__
from pgdispatch.backends.asyncpg import AsyncpgBackend
from pgdispatch.client import Webhooks
from pgdispatch.config import WebhooksConfig, parse_duration
from pgdispatch.core.common import UNSET, OwnerFilter
from pgdispatch.errors import PgDispatchError
from pgdispatch.worker.runner import run_until_signalled

__all__ = ["main"]

Handler = Callable[[Webhooks, argparse.Namespace, TextIO], Coroutine[Any, Any, int]]


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "handler"):
        parser.print_help()
        return 2
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    dsn = args.dsn or os.environ.get("PGDISPATCH_DSN") or os.environ.get("DATABASE_URL")
    if not dsn:
        parser.error("no database given: use --dsn, PGDISPATCH_DSN or DATABASE_URL")
    try:
        config = WebhooksConfig(**_config_overrides(args))
    except ValueError as exc:
        parser.error(str(exc))
    try:
        return asyncio.run(_run(args.handler, dsn, config, args))
    except PgDispatchError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except asyncpg.UndefinedTableError:
        print(
            f"error: schema {config.db_schema!r} is not set up; run 'pgdispatch migrate' first",
            file=sys.stderr,
        )
        return 1
    except (asyncpg.PostgresError, OSError) as exc:
        print(f"error: database: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:  # pragma: no cover - interactive use
        return 130


async def _run(handler: Handler, dsn: str, config: WebhooksConfig, args: argparse.Namespace) -> int:
    async with Webhooks(AsyncpgBackend(dsn), config) as webhooks:
        return await handler(webhooks, args, sys.stdout)


# --- commands -------------------------------------------------------------------


async def _migrate(webhooks: Webhooks, args: argparse.Namespace, out: TextIO) -> int:
    applied = await webhooks.migrate()
    schema = webhooks.config.db_schema
    if applied:
        print(f"Applied migrations {', '.join(map(str, applied))} to schema {schema!r}.", file=out)
    else:
        print(f"Schema {schema!r} is up to date.", file=out)
    return 0


async def _worker(webhooks: Webhooks, args: argparse.Namespace, out: TextIO) -> int:
    await run_until_signalled(webhooks.worker(), metrics_port=args.metrics_port)
    return 0


async def _status(webhooks: Webhooks, args: argparse.Namespace, out: TextIO) -> int:
    stats = await webhooks.status()
    if args.json:
        return _print_json(dataclasses.asdict(stats), out)
    rows = [
        ("pending", stats.pending),
        ("due now", stats.due),
        ("in flight", stats.in_flight),
        ("delivery lag", _format_duration(stats.oldest_due_age)),
        ("failed (24h)", stats.failed_last_24h),
        ("endpoints enabled", stats.endpoints_enabled),
        ("endpoints disabled", stats.endpoints_disabled),
        ("circuit breakers open", stats.circuits_open),
        ("active workers", stats.active_workers),
    ]
    width = max(len(name) for name, _ in rows)
    for name, value in rows:
        print(f"{name:<{width}}  {value}", file=out)
    return 0


async def _deliveries(webhooks: Webhooks, args: argparse.Namespace, out: TextIO) -> int:
    deliveries = await webhooks.deliveries.list(
        endpoint_id=args.endpoint,
        event_id=args.event,
        status=args.status,
        since=_since(args.since),
        limit=args.limit,
        owner_id=_owner(args.owner),
    )
    if args.json:
        return _print_json([dataclasses.asdict(d) for d in deliveries], out)
    _print_table(
        out,
        ["ID", "STATUS", "EVENT TYPE", "ENDPOINT", "ATTEMPTS", "LAST", "CREATED"],
        [
            [
                d.id,
                d.status.value,
                d.event_type,
                d.endpoint_id,
                d.attempt_count,
                d.last_status_code or d.last_error or "",
                _format_time(d.created_at),
            ]
            for d in deliveries
        ],
    )
    return 0


async def _delivery_show(webhooks: Webhooks, args: argparse.Namespace, out: TextIO) -> int:
    delivery = await webhooks.deliveries.get(args.delivery_id)
    attempts = await webhooks.deliveries.attempts(args.delivery_id)
    if args.json:
        data = dataclasses.asdict(delivery)
        data["attempts"] = [dataclasses.asdict(a) for a in attempts]
        return _print_json(data, out)
    for name, value in dataclasses.asdict(delivery).items():
        print(f"{name:<17} {_plain(value)}", file=out)
    for attempt in attempts:
        outcome = attempt.response_status or attempt.error
        print(
            f"\n#{attempt.attempt_number}  {_format_time(attempt.created_at)}  {outcome}  "
            f"{attempt.duration_ms} ms",
            file=out,
        )
        if attempt.error and attempt.response_status:
            print(f"   error: {attempt.error}", file=out)
        if attempt.response_body:
            print(f"   {attempt.response_body[:500]}", file=out)
    return 0


async def _retry(webhooks: Webhooks, args: argparse.Namespace, out: TextIO) -> int:
    if args.delivery_id is not None:
        delivery = await webhooks.deliveries.retry(args.delivery_id)
        print(f"Delivery {delivery.id} scheduled for an immediate attempt.", file=out)
        return 0
    count = await webhooks.deliveries.retry_all(
        endpoint_id=args.endpoint, status=args.status, since=_since(args.since)
    )
    print(f"Scheduled {count} deliveries for retry.", file=out)
    return 0


async def _replay(webhooks: Webhooks, args: argparse.Namespace, out: TextIO) -> int:
    ids = await webhooks.events.replay(args.event, endpoint_id=args.endpoint)
    print(f"Created {len(ids)} deliveries: {', '.join(map(str, ids)) or '-'}", file=out)
    return 0


async def _endpoints_list(webhooks: Webhooks, args: argparse.Namespace, out: TextIO) -> int:
    endpoints = await webhooks.endpoints.list(
        owner_id=_owner(args.owner), enabled=args.enabled, limit=args.limit
    )
    if args.json:
        return _print_json([dataclasses.asdict(e) for e in endpoints], out)
    _print_table(
        out,
        ["ID", "OWNER", "URL", "EVENTS", "ENABLED", "CIRCUIT", "FAILURES"],
        [
            [
                e.id,
                e.owner_id or "",
                e.url,
                ",".join(e.event_types),
                "yes" if e.is_enabled else f"no ({e.disabled_reason})",
                e.circuit_state.value,
                e.consecutive_failures,
            ]
            for e in endpoints
        ],
    )
    return 0


async def _endpoints_enable(webhooks: Webhooks, args: argparse.Namespace, out: TextIO) -> int:
    endpoint = await webhooks.endpoints.enable(args.endpoint_id)
    print(f"Endpoint {endpoint.id} enabled; circuit breaker reset.", file=out)
    return 0


async def _endpoints_disable(webhooks: Webhooks, args: argparse.Namespace, out: TextIO) -> int:
    endpoint = await webhooks.endpoints.disable(args.endpoint_id, reason=args.reason)
    print(f"Endpoint {endpoint.id} disabled.", file=out)
    return 0


async def _cleanup(webhooks: Webhooks, args: argparse.Namespace, out: TextIO) -> int:
    deleted = await webhooks.cleanup(older_than=args.older_than)
    print(f"Deleted {deleted} events with their deliveries and attempts.", file=out)
    return 0


# --- argument parsing -------------------------------------------------------------


def _global_options(*, top_level: bool) -> argparse.ArgumentParser:
    # Global options are accepted before and after the command. Sub-commands
    # use SUPPRESS so that an option given before the command is not
    # overwritten by the sub-command's default.
    def default(value: Any) -> Any:
        return value if top_level else argparse.SUPPRESS

    options = argparse.ArgumentParser(add_help=False)
    options.add_argument(
        "--dsn",
        default=default(None),
        help="PostgreSQL URL (default: $PGDISPATCH_DSN or $DATABASE_URL)",
    )
    options.add_argument(
        "--schema", default=default(None), help="database schema (default: pgdispatch)"
    )
    options.add_argument(
        "--log-level", default=default("info"), choices=["debug", "info", "warning", "error"]
    )
    return options


def _build_parser() -> argparse.ArgumentParser:  # noqa: PLR0915 - a flat list of options
    common = _global_options(top_level=False)
    parser = argparse.ArgumentParser(
        prog="pgdispatch",
        description="Reliable webhook delivery using only PostgreSQL.",
        parents=[_global_options(top_level=True)],
    )
    parser.add_argument("--version", action="version", version=f"pgdispatch {__version__}")
    commands = parser.add_subparsers(title="commands", metavar="<command>")

    def command(name: str, handler: Handler, help_text: str) -> argparse.ArgumentParser:
        sub = commands.add_parser(name, help=help_text, description=help_text, parents=[common])
        sub.set_defaults(handler=handler)
        return sub

    command("migrate", _migrate, "Create or upgrade the database schema.")

    worker = command("worker", _worker, "Deliver webhooks until interrupted.")
    worker.add_argument("--concurrency", type=int, help="simultaneous HTTP requests")
    worker.add_argument("--batch-size", type=int, help="deliveries claimed per query")
    worker.add_argument("--metrics-port", type=int, help="serve Prometheus metrics on this port")

    status = command("status", _status, "Show queue size, delivery lag and open circuit breakers.")
    status.add_argument("--json", action="store_true", help="machine-readable output")

    deliveries = command("deliveries", _deliveries, "List recent deliveries.")
    deliveries.add_argument(
        "--status", choices=["pending", "in_flight", "succeeded", "failed", "cancelled"]
    )
    deliveries.add_argument("--endpoint", type=uuid.UUID, help="endpoint id")
    deliveries.add_argument("--event", type=uuid.UUID, help="event id")
    deliveries.add_argument("--owner", help="owner id")
    deliveries.add_argument("--since", type=_duration, help="e.g. 30m, 1h, 7d")
    deliveries.add_argument("--limit", type=int, default=50)
    deliveries.add_argument("--json", action="store_true", help="machine-readable output")
    show = deliveries.add_subparsers(metavar="show").add_parser(
        "show", help="Show a delivery with every attempt.", parents=[common]
    )
    show.add_argument("delivery_id", type=int)
    show.add_argument("--json", action="store_true", help="machine-readable output")
    show.set_defaults(handler=_delivery_show)

    retry = command(
        "retry", _retry, "Retry one delivery, or every failed delivery matching filters."
    )
    retry.add_argument("delivery_id", type=int, nargs="?")
    retry.add_argument("--endpoint", type=uuid.UUID, help="endpoint id")
    retry.add_argument("--status", default="failed", choices=["failed", "cancelled", "succeeded"])
    retry.add_argument("--since", type=_duration, help="only deliveries created this recently")

    replay = command("replay", _replay, "Deliver an event again.")
    replay.add_argument("--event", type=uuid.UUID, required=True, help="event id")
    replay.add_argument("--endpoint", type=uuid.UUID, help="only to this endpoint")

    endpoints = commands.add_parser("endpoints", help="Manage endpoints.", parents=[common])
    endpoint_commands = endpoints.add_subparsers(
        title="commands", metavar="<command>", required=True
    )
    listing = endpoint_commands.add_parser("list", help="List endpoints.", parents=[common])
    listing.add_argument("--owner", help="owner id")
    listing.add_argument("--enabled", action=argparse.BooleanOptionalAction, default=None)
    listing.add_argument("--limit", type=int, default=100)
    listing.add_argument("--json", action="store_true", help="machine-readable output")
    listing.set_defaults(handler=_endpoints_list)
    enable = endpoint_commands.add_parser(
        "enable", help="Enable an endpoint and reset its circuit breaker.", parents=[common]
    )
    enable.add_argument("endpoint_id", type=uuid.UUID)
    enable.set_defaults(handler=_endpoints_enable)
    disable = endpoint_commands.add_parser("disable", help="Disable an endpoint.", parents=[common])
    disable.add_argument("endpoint_id", type=uuid.UUID)
    disable.add_argument("--reason")
    disable.set_defaults(handler=_endpoints_disable)

    cleanup = command("cleanup", _cleanup, "Delete data older than the retention period.")
    cleanup.add_argument("--older-than", type=_duration, help="default: retention_days")
    return parser


def _config_overrides(args: argparse.Namespace) -> dict[str, Any]:
    names = {"schema": "db_schema", "concurrency": "concurrency", "batch_size": "batch_size"}
    return {
        setting: getattr(args, option)
        for option, setting in names.items()
        if getattr(args, option, None) is not None
    }


def _duration(value: str) -> timedelta:
    try:
        return parse_duration(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None


def _owner(owner: str | None) -> OwnerFilter:
    # Without --owner, show every owner (None would mean "endpoints without an owner").
    return UNSET if owner is None else owner


def _since(duration: timedelta | None) -> datetime | None:
    return datetime.now(timezone.utc) - duration if duration else None


# --- output -------------------------------------------------------------------------


def _print_table(out: TextIO, headers: list[str], rows: list[list[Any]]) -> None:
    if not rows:
        print("(none)", file=out)
        return
    cells = [headers, *[[_plain(value) for value in row] for row in rows]]
    widths = [max(len(row[column]) for row in cells) for column in range(len(headers))]
    for row in cells:
        print(
            "  ".join(cell.ljust(width) for cell, width in zip(row, widths, strict=True)).rstrip(),
            file=out,
        )


def _print_json(data: Any, out: TextIO) -> int:
    json.dump(data, out, indent=2, default=_plain)
    out.write("\n")
    return 0


def _plain(value: Any) -> str:
    if isinstance(value, datetime):
        return _format_time(value)
    if isinstance(value, timedelta):
        return _format_duration(value)
    if hasattr(value, "value"):  # enums
        return str(value.value)
    return "" if value is None else str(value)


def _format_time(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _format_duration(duration: timedelta) -> str:
    seconds = int(duration.total_seconds())
    if seconds < 60:
        return f"{seconds}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m{seconds:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h{minutes:02d}m"


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
