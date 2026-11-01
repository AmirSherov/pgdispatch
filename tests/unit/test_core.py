from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone

import pytest
from hypothesis import given
from hypothesis import strategies as st

from pgdispatch import ValidationError, WebhooksConfig, exponential_backoff
from pgdispatch.backends.base import (
    Query,
    render_schema,
    run_sync,
    to_numeric_style,
    to_pyformat_style,
)
from pgdispatch.core.circuit import BreakerPolicy, BreakerState, record_failure, record_success
from pgdispatch.core.matching import (
    candidate_patterns,
    matches,
    validate_event_type,
    validate_pattern,
)
from pgdispatch.core.payload import build_body, serialize_payload, uuid7
from pgdispatch.core.retry import DEFAULT_RETRY_SCHEDULE, next_retry_delay, parse_retry_after
from pgdispatch.core.secrets import SecretEntry, SecretStore
from pgdispatch.migrations import load_migrations
from pgdispatch.models import CircuitState
from pgdispatch.worker.ratelimit import RateLimiter

NOW = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)

# --- matching -----------------------------------------------------------------

segment = st.from_regex(r"[a-z0-9_\-]{1,8}", fullmatch=True)
event_types = st.lists(segment, min_size=1, max_size=5).map(".".join)


@given(event_types)
def test_candidate_patterns_agree_with_matches(event_type: str) -> None:
    candidates = candidate_patterns(event_type)
    assert all(matches(pattern, event_type) for pattern in candidates)
    for pattern in candidates:
        validate_pattern(pattern)


@given(event_types, event_types)
def test_matches_is_exactly_candidate_membership(pattern_base: str, event_type: str) -> None:
    for pattern in (pattern_base, pattern_base + ".*", "*"):
        assert matches(pattern, event_type) == (pattern in candidate_patterns(event_type))


@pytest.mark.parametrize(
    ("pattern", "event_type", "expected"),
    [
        ("*", "anything.at.all", True),
        ("payment.*", "payment.paid", True),
        ("payment.*", "payment.refund.created", True),
        ("payment.*", "payment", False),
        ("payment.*", "payments.paid", False),
        ("payment.paid", "payment.paid", True),
        ("payment.paid", "payment.paid.late", False),
    ],
)
def test_matches(pattern: str, event_type: str, expected: bool) -> None:
    assert matches(pattern, event_type) is expected


@pytest.mark.parametrize("value", ["", ".", "a..b", "a.", "pay ment", "a.*", "*", "x" * 256])
def test_invalid_event_types(value: str) -> None:
    with pytest.raises(ValidationError):
        validate_event_type(value)


@pytest.mark.parametrize("value", ["", "pay*", "*.paid", "a.*.b", "**", "a b"])
def test_invalid_patterns(value: str) -> None:
    with pytest.raises(ValidationError):
        validate_pattern(value)


# --- retries --------------------------------------------------------------------


def test_default_schedule_spans_about_27_hours() -> None:
    assert len(DEFAULT_RETRY_SCHEDULE) == 8
    total = sum(DEFAULT_RETRY_SCHEDULE, timedelta())
    assert timedelta(hours=27) < total < timedelta(hours=28)


def test_next_retry_delay_walks_the_schedule() -> None:
    schedule = (timedelta(0), timedelta(seconds=5), timedelta(minutes=5))
    assert next_retry_delay(schedule, 1) == timedelta(seconds=5)
    assert next_retry_delay(schedule, 2) == timedelta(minutes=5)
    assert next_retry_delay(schedule, 3) is None


@given(st.floats(min_value=0, max_value=0.99))
def test_jitter_stays_within_bounds(jitter: float) -> None:
    delay = next_retry_delay(
        (timedelta(0), timedelta(seconds=100)), 1, jitter=jitter, rng=random.Random(1)
    )
    assert delay is not None
    assert timedelta(seconds=100 * (1 - jitter)) <= delay <= timedelta(seconds=100 * (1 + jitter))


def test_exponential_backoff() -> None:
    assert exponential_backoff(4, initial=timedelta(seconds=10)) == (
        timedelta(0),
        timedelta(seconds=10),
        timedelta(seconds=20),
        timedelta(seconds=40),
    )
    capped = exponential_backoff(30, max_delay=timedelta(hours=1))
    assert max(capped) == timedelta(hours=1)
    with pytest.raises(ValueError):
        exponential_backoff(0)


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("120", timedelta(seconds=120)),
        ("Sun, 01 Nov 2026 12:01:00 GMT", timedelta(minutes=1)),
        ("Sun, 01 Nov 2026 11:00:00 GMT", timedelta(0)),
        ("soon", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_retry_after(header: str | None, expected: timedelta | None) -> None:
    assert parse_retry_after(header, now=NOW) == expected


# --- circuit breaker ------------------------------------------------------------

POLICY = BreakerPolicy(
    failure_threshold=3,
    cooldown=timedelta(minutes=5),
    max_cooldown=timedelta(hours=1),
    auto_disable_after=timedelta(days=5),
)


def test_breaker_opens_at_threshold() -> None:
    state = BreakerState()
    for _ in range(2):
        update = record_failure(state, now=NOW, policy=POLICY)
        state = update.state
        assert not update.opened
    update = record_failure(state, now=NOW, policy=POLICY)
    assert update.opened
    assert update.state.state is CircuitState.OPEN
    assert update.state.retry_at == NOW + timedelta(minutes=5)
    assert update.state.failing_since == NOW


def test_breaker_cooldown_grows_and_caps() -> None:
    cooldowns = [POLICY.cooldown_for(trips) for trips in range(1, 7)]
    assert cooldowns == [
        timedelta(minutes=5),
        timedelta(minutes=10),
        timedelta(minutes=20),
        timedelta(minutes=40),
        timedelta(hours=1),
        timedelta(hours=1),
    ]
    assert POLICY.cooldown_for(10_000) == timedelta(hours=1)


def test_failed_probe_reopens_and_success_closes() -> None:
    half_open = BreakerState(
        state=CircuitState.HALF_OPEN, consecutive_failures=3, failing_since=NOW, trips=1
    )
    update = record_failure(half_open, now=NOW, policy=POLICY)
    assert update.opened
    assert update.state.trips == 2
    assert update.state.retry_at == NOW + timedelta(minutes=10)
    assert record_success(update.state).state == BreakerState()


def test_failure_while_open_keeps_schedule() -> None:
    opened = BreakerState(
        state=CircuitState.OPEN, consecutive_failures=3, failing_since=NOW, retry_at=NOW, trips=1
    )
    update = record_failure(opened, now=NOW + timedelta(seconds=1), policy=POLICY)
    assert not update.opened
    assert update.state.retry_at == NOW
    assert update.state.consecutive_failures == 4


def test_auto_disable_after_continuous_failures() -> None:
    state = BreakerState(consecutive_failures=1, failing_since=NOW - timedelta(days=5))
    assert record_failure(state, now=NOW, policy=POLICY).disable
    assert not record_failure(BreakerState(), now=NOW, policy=POLICY).disable


# --- payload ----------------------------------------------------------------------


def test_build_body_wraps_payload_verbatim() -> None:
    body = build_body("order.created", NOW, '{"id": 1}')
    assert (
        body == b'{"type":"order.created","timestamp":"2026-11-01T12:00:00.000Z","data":{"id": 1}}'
    )
    assert json.loads(body)["data"] == {"id": 1}


def test_serialize_payload_handles_pydantic_and_dataclasses() -> None:
    from dataclasses import dataclass

    from pydantic import BaseModel

    @dataclass
    class Point:
        x: int

    class Model(BaseModel):
        at: datetime

    text = serialize_payload({"p": Point(1), "m": Model(at=NOW), "s": {3}}, max_bytes=1000)
    assert json.loads(text) == {"p": {"x": 1}, "m": {"at": "2026-11-01T12:00:00Z"}, "s": [3]}


def test_uuid7_is_time_ordered_and_versioned() -> None:
    ids = [uuid7() for _ in range(1000)]
    assert all(value.version == 7 for value in ids)
    assert len(set(ids)) == 1000
    timestamps = [value.int >> 80 for value in ids]
    assert timestamps == sorted(timestamps)


# --- secrets ------------------------------------------------------------------------


def test_secret_rotation_keeps_old_secret_for_grace_period() -> None:
    store = SecretStore()
    original = [store.seal("whsec_old", now=NOW)]
    rotated = store.rotate(original, "whsec_new", now=NOW, grace_period=timedelta(hours=1))
    assert [entry.key for entry in rotated] == ["whsec_new", "whsec_old"]
    assert store.active_secrets(SecretStore.dump(rotated), now=NOW) == ["whsec_new", "whsec_old"]
    later = NOW + timedelta(hours=2)
    assert store.active_secrets(SecretStore.dump(rotated), now=later) == ["whsec_new"]

    # Rotating again never extends an existing expiry, and drops expired secrets.
    again = store.rotate(rotated, "whsec_newest", now=later, grace_period=timedelta(days=1))
    assert [entry.key for entry in again] == ["whsec_newest", "whsec_new"]


def test_secret_entries_roundtrip() -> None:
    entry = SecretEntry("whsec_x", encrypted=False, created_at=NOW, expires_at=NOW)
    assert SecretStore.load(SecretStore.dump([entry])) == [entry]


# --- SQL rendering ------------------------------------------------------------------


def test_placeholder_styles() -> None:
    sql = "SELECT CAST(:a AS int), x::text, CAST(:b AS text), CAST(:a AS int) FROM {schema}.t"
    rendered = render_schema(sql, "pgd")
    assert rendered.endswith('FROM "pgd".t')
    numeric, names = to_numeric_style(rendered)
    assert names == ("a", "b")
    assert numeric.startswith("SELECT CAST($1 AS int), x::text, CAST($2 AS text), CAST($1 AS int)")
    assert to_pyformat_style("SELECT :a, '100%'").startswith("SELECT %(a)s, '100%%'")


def test_run_sync_drives_operations() -> None:
    def operation():  # type: ignore[no-untyped-def]
        rows = yield Query("SELECT :x", {"x": 1})
        more = yield Query("SELECT :y", {"y": rows[0]["value"] + 1})
        return more[0]["value"]

    class Echo:
        def fetch(self, sql: str, params: dict[str, int]) -> list[dict[str, int]]:
            return [{"value": next(iter(params.values()))}]

    assert run_sync(operation(), Echo(), schema="s") == 2


def test_migrations_are_split_into_statements() -> None:
    [initial, *_] = load_migrations()
    assert initial.version == 1
    assert all(statement.strip() for statement in initial.statements)
    assert not any(statement.lstrip().startswith("--") for statement in initial.statements)
    assert any("CREATE TABLE {schema}.deliveries" in s for s in initial.statements)


# --- configuration ------------------------------------------------------------------


def test_config_reads_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PGDISPATCH_CONCURRENCY", "7")
    monkeypatch.setenv("PGDISPATCH_RETRY_SCHEDULE", "[0, 10, 60]")
    config = WebhooksConfig()
    assert config.concurrency == 7
    assert config.retry_schedule == (timedelta(0), timedelta(seconds=10), timedelta(minutes=1))
    assert WebhooksConfig(concurrency=3).concurrency == 3


@pytest.mark.parametrize(
    "kwargs",
    [
        {"db_schema": "Bad-Name"},
        {"lease_duration": 5, "request_timeout": 10},
        {"retry_schedule": []},
        {"retry_schedule": [-1]},
        {"poll_interval": 0},
        {"circuit_cooldown": 600, "circuit_max_cooldown": 60},
        {"unknown_option": 1},
    ],
)
def test_invalid_config(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        WebhooksConfig(**kwargs)  # type: ignore[arg-type]


# --- rate limiting --------------------------------------------------------------------


def test_rate_limiter_allows_burst_then_spaces_requests() -> None:
    clock = [0.0]
    limiter = RateLimiter(clock=lambda: clock[0])
    delays = [limiter.reserve("e", rate=5) for _ in range(7)]
    assert delays[:5] == [0.0] * 5
    assert delays[5] == pytest.approx(0.2)
    assert delays[6] == pytest.approx(0.4)

    limiter.release("e", rate=5)
    assert limiter.reserve("e", rate=5) == pytest.approx(0.4)

    clock[0] = 100.0
    limiter.prune()
    assert limiter.reserve("e", rate=5) == 0.0


def test_slow_rate_spaces_every_request() -> None:
    limiter = RateLimiter(clock=lambda: 0.0)
    assert limiter.reserve("e", rate=0.5) == 0.0
    assert limiter.reserve("e", rate=0.5) == pytest.approx(2.0)
