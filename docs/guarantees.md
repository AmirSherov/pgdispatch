# Reliability and guarantees

pgdispatch promises **at-least-once delivery of committed events**. This page
explains what that covers, and where it ends.

| Guarantee | Yes / No | Why |
| --- | --- | --- |
| An event from a rolled-back transaction is never sent | **yes** | The event, its deliveries and the `NOTIFY` belong to your transaction. |
| A committed event is never lost | **yes** | Delivery state lives in PostgreSQL. Claims are leases that expire when a worker dies. |
| Exactly-once delivery | **no** | A worker can crash after the receiver answered `200` but before the result was stored. Receivers deduplicate by `webhook-id`. |
| Order | **per `ordering_key`** | Deliveries with the same key go to an endpoint one at a time. |
| Delivery while the receiver is down | **about 27 hours** | That is the length of the default schedule. After it, the delivery is `failed` and can be retried or replayed. |
| Extra infrastructure | **none** | PostgreSQL 13+. |

## How a delivery stays safe

1. **Publish.** One statement inserts the event and one delivery row for each subscribed endpoint, in your transaction.
2. **Claim.** A worker runs a short transaction that picks due rows with
   `FOR UPDATE SKIP LOCKED`, marks them `in_flight` with
   `locked_until = now() + lease_duration`, increments `attempt_count`, and
   commits. No lock is held while the HTTP request runs.
3. **Send.** The worker signs and sends the request.
4. **Record.** The result is written in a batch. Each update is *fenced* by
   `attempt_count`: if the lease expired and another worker already retried the
   delivery, the stale result is discarded. The attempt itself is still logged.

If a worker dies at any point, the lease expires and step 2 picks the delivery up again.

## Speed versus durability

The worker's own transactions (claims and results) run with
`synchronous_commit = off`. If the *database server* crashes, the last few
hundred milliseconds of worker bookkeeping can be lost. This affects only
bookkeeping: a lost result means the request is sent once more, which
at-least-once already allows. **`publish()` is not affected.** It runs in your
transaction, with your durability settings.

## Retries

Element *n* of `retry_schedule` is the delay before attempt *n + 1*. With the
default schedule, attempts run immediately and then after 5 s, 5 min, 30 min, 2 h,
5 h, 10 h and 10 h, with ±20% jitter, so that retries for many failed
deliveries don't arrive all at once. A `Retry-After` header on `429`/`503` can
lengthen a delay, but never beyond the longest delay in the schedule.

A manual retry (`deliveries.retry`) starts the schedule again from the beginning.
Attempt numbers keep growing, so the log stays clear.

## Circuit breaker

Each endpoint has its own breaker:

- **closed**: normal operation. After `failure_threshold` (10) failures in a row → **open**.
- **open**: none of the endpoint's deliveries are claimed, so they don't use up
  their attempts while the endpoint is down. After `circuit_cooldown` (5 min) → **half-open**.
- **half-open**: one probe delivery is sent. If it succeeds → **closed**. If it fails → **open** again,
  with a doubled cool-down (up to `circuit_max_cooldown`, 1 h).

Separately, an endpoint that has failed without a break for `auto_disable_after`
(5 days) is disabled, and your `on_endpoint_disabled` hook is called. You can,
for example, email the customer.

## Ordering in detail

With an `ordering_key`, a delivery is claimed only if no **earlier** delivery
with the same key to the same endpoint is still `pending` or `in_flight`.
"Earlier" means earlier insertion. Events published one after another, the
usual case, arrive in exactly that order. If two transactions publish with the
same key **at the same time**, the one that started inserting first counts as
earlier.

## Proven by tests

These tests run against real PostgreSQL and real worker processes, on every
supported Python version and PostgreSQL 13–17
([`tests/chaos`](https://github.com/AmirSherov/pgdispatch/blob/main/tests/chaos/test_guarantees.py)):

- a rolled-back transaction creates 0 deliveries;
- `kill -9` of a worker with 1,000 deliveries in flight: after a restart all 1,000 arrive, and none are lost;
- 4 worker processes deliver 10,000 events, each exactly once;
- an endpoint outage opens the breaker, and after recovery every event arrives;
- 1,000 events with one `ordering_key`, handled by two competing workers, arrive strictly in order.
