-- pgdispatch schema, version 1.
--
-- Statements end with a semicolon at the end of a line; the migrator splits on
-- that, so keep function bodies and dollar quoting out of migrations.

CREATE TABLE {schema}.endpoints (
    id                   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id             text,
    url                  text NOT NULL,
    description          text,
    event_types          text[] NOT NULL DEFAULT '{*}',
    secrets              jsonb NOT NULL,
    headers              jsonb NOT NULL DEFAULT '{}',
    rate_limit_rps       double precision CHECK (rate_limit_rps > 0),
    is_enabled           boolean NOT NULL DEFAULT true,
    disabled_reason      text,
    circuit_state        text NOT NULL DEFAULT 'closed'
                         CHECK (circuit_state IN ('closed', 'open', 'half_open')),
    consecutive_failures integer NOT NULL DEFAULT 0,
    failing_since        timestamptz,
    circuit_opened_at    timestamptz,
    circuit_retry_at     timestamptz,
    circuit_trips        integer NOT NULL DEFAULT 0,
    created_at           timestamptz NOT NULL DEFAULT now(),
    updated_at           timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX endpoints_owner_idx ON {schema}.endpoints (owner_id) WHERE is_enabled;

CREATE INDEX endpoints_event_types_idx ON {schema}.endpoints USING gin (event_types) WHERE is_enabled;

-- Workers look for tripped breakers whose cooldown has elapsed on every poll.
CREATE INDEX endpoints_circuit_idx ON {schema}.endpoints (circuit_retry_at)
    WHERE is_enabled AND circuit_state <> 'closed';

CREATE TABLE {schema}.events (
    id              uuid PRIMARY KEY,
    event_type      text NOT NULL,
    owner_id        text,
    payload         jsonb NOT NULL,
    idempotency_key text,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX events_idempotency_key_idx ON {schema}.events (idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX events_created_at_idx ON {schema}.events (created_at);

CREATE TABLE {schema}.deliveries (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id         uuid NOT NULL REFERENCES {schema}.events (id) ON DELETE CASCADE,
    endpoint_id      uuid NOT NULL REFERENCES {schema}.endpoints (id) ON DELETE CASCADE,
    ordering_key     text,
    status           text NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'in_flight', 'succeeded', 'failed', 'cancelled')),
    attempt_count    integer NOT NULL DEFAULT 0,
    -- attempt_count when the current retry round began; a manual retry starts
    -- a new round with the full schedule while attempt numbers keep growing.
    schedule_offset  integer NOT NULL DEFAULT 0,
    next_attempt_at  timestamptz NOT NULL DEFAULT now(),
    locked_until     timestamptz,
    last_status_code integer,
    last_error       text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz
);

-- The hot path: workers claim due deliveries in next_attempt_at order.
CREATE INDEX deliveries_due_idx ON {schema}.deliveries (next_attempt_at)
    WHERE status IN ('pending', 'in_flight');

-- Ordered delivery: "is there an earlier unfinished delivery with this key?"
CREATE INDEX deliveries_ordering_idx ON {schema}.deliveries (endpoint_id, ordering_key, id)
    WHERE status IN ('pending', 'in_flight') AND ordering_key IS NOT NULL;

CREATE INDEX deliveries_endpoint_idx ON {schema}.deliveries (endpoint_id, id);

CREATE INDEX deliveries_event_idx ON {schema}.deliveries (event_id);

CREATE TABLE {schema}.attempts (
    id              bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id     bigint NOT NULL REFERENCES {schema}.deliveries (id) ON DELETE CASCADE,
    attempt_number  integer NOT NULL,
    request_headers jsonb,
    response_status integer,
    response_body   text,
    duration_ms     integer,
    error           text,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX attempts_delivery_idx ON {schema}.attempts (delivery_id);

CREATE TABLE {schema}.workers (
    id           uuid PRIMARY KEY,
    hostname     text NOT NULL,
    pid          integer NOT NULL,
    version      text NOT NULL,
    concurrency  integer NOT NULL,
    started_at   timestamptz NOT NULL DEFAULT now(),
    heartbeat_at timestamptz NOT NULL DEFAULT now()
);
