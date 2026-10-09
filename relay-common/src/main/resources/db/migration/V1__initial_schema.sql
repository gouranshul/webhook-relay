-- Shared schema for relay-api (owner, runs Flyway) and relay-dispatcher (reads endpoints, writes delivery state).

CREATE TABLE endpoint_secrets (
    id          uuid PRIMARY KEY,
    key_id      text        NOT NULL,                -- which encryption key produced the ciphertext (rotation)
    ciphertext  bytea       NOT NULL,                -- AES-256-GCM: iv(12) || ciphertext || tag(16)
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE endpoints (
    id                 uuid PRIMARY KEY,
    url                text        NOT NULL,
    description        text,
    secret_ref         uuid        NOT NULL REFERENCES endpoint_secrets (id),
    event_types        text[]      NOT NULL CHECK (cardinality(event_types) > 0),
    rate_limit_per_sec integer     NOT NULL DEFAULT 50 CHECK (rate_limit_per_sec > 0),
    status             text        NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'PAUSED', 'DELETED')),
    consecutive_dlt    integer     NOT NULL DEFAULT 0,
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now()
);

-- Fan-out lookup: endpoints whose event_types overlap {type, '*'}.
CREATE INDEX endpoints_event_types_idx ON endpoints USING gin (event_types) WHERE status <> 'DELETED';

CREATE TABLE events (
    id          uuid PRIMARY KEY,
    event_type  text        NOT NULL,
    payload     jsonb       NOT NULL,
    traceparent text,                                -- W3C trace context of the ingest request
    created_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE idempotency_keys (
    idempotency_key text PRIMARY KEY,
    request_hash    text        NOT NULL,            -- SHA-256 of the request; a reused key with a new body is rejected
    response_status integer     NOT NULL,
    response_body   text        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    expires_at      timestamptz NOT NULL
);

CREATE INDEX idempotency_keys_expires_at_idx ON idempotency_keys (expires_at);

CREATE TABLE deliveries (
    id               uuid PRIMARY KEY,
    webhook_id       uuid        NOT NULL,           -- sent as Webhook-Id; shared by a delivery and its replays
    event_id         uuid        NOT NULL REFERENCES events (id),
    endpoint_id      uuid        NOT NULL REFERENCES endpoints (id),
    replay_of        uuid REFERENCES deliveries (id),
    status           text        NOT NULL CHECK (status IN
                         ('PENDING', 'IN_FLIGHT', 'RETRYING', 'SUCCEEDED', 'FAILED', 'PARKED', 'CANCELLED')),
    attempt_count    integer     NOT NULL DEFAULT 0,
    last_status_code integer,
    last_error       text,
    lease_until      timestamptz,                    -- set while IN_FLIGHT; an expired lease may be reclaimed
    not_before       timestamptz,                    -- earliest next attempt (honours Retry-After)
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    completed_at     timestamptz
);

CREATE INDEX deliveries_endpoint_status_created_idx ON deliveries (endpoint_id, status, created_at DESC);
CREATE INDEX deliveries_status_created_idx ON deliveries (status, created_at DESC);
CREATE INDEX deliveries_event_idx ON deliveries (event_id);

CREATE TABLE outbox (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id  uuid        NOT NULL REFERENCES deliveries (id),
    endpoint_id  uuid        NOT NULL,
    shard        smallint    NOT NULL,               -- see OutboxShards
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);

-- The relay only ever reads unpublished rows, in id order, per shard.
CREATE INDEX outbox_unpublished_idx ON outbox (shard, id) WHERE published_at IS NULL;
CREATE INDEX outbox_published_at_idx ON outbox (published_at) WHERE published_at IS NOT NULL;

CREATE TABLE delivery_attempts (
    id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    delivery_id    uuid        NOT NULL REFERENCES deliveries (id),
    attempt_number integer     NOT NULL,
    outcome        text        NOT NULL,             -- SUCCESS, RETRYABLE, PERMANENT, TIMEOUT, ...
    status_code    integer,
    latency_ms     integer     NOT NULL,
    error          text,                             -- truncated
    attempted_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX delivery_attempts_delivery_idx ON delivery_attempts (delivery_id, attempt_number);
