-- +goose Up
CREATE TABLE companies (
    company_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE payment_events (
    id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL,
    source TEXT NOT NULL,
    company_id TEXT NOT NULL REFERENCES companies(company_id),
    payment_id TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('payment.created', 'payment.succeeded', 'payment.failed')),
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    currency CHAR(3) NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    schema_version INTEGER NOT NULL,
    payload JSONB NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT payment_events_source_event_key UNIQUE (source, event_id)
);
CREATE INDEX payment_events_payment_idx ON payment_events (payment_id, occurred_at);
CREATE INDEX payment_events_company_received_idx ON payment_events (company_id, received_at DESC);

CREATE TABLE payments (
    payment_id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL REFERENCES companies(company_id),
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    currency CHAR(3) NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('created', 'succeeded', 'failed')),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX payments_company_status_idx ON payments (company_id, status, created_at DESC);

CREATE TABLE outbox_events (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    event_id TEXT NOT NULL,
    payment_id TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    CONSTRAINT outbox_events_source_event_key UNIQUE (source, event_id),
    CONSTRAINT outbox_payment_event_fk FOREIGN KEY (source, event_id)
        REFERENCES payment_events(source, event_id)
);
CREATE INDEX outbox_unpublished_idx ON outbox_events (id) WHERE published_at IS NULL;

CREATE TABLE processed_messages (
    consumer_name TEXT NOT NULL,
    source TEXT NOT NULL,
    event_id TEXT NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer_name, source, event_id)
);

CREATE TABLE payment_history (
    id BIGSERIAL PRIMARY KEY,
    payment_id TEXT NOT NULL REFERENCES payments(payment_id),
    source TEXT NOT NULL,
    event_id TEXT NOT NULL,
    from_status TEXT CHECK (from_status IN ('created', 'succeeded', 'failed')),
    to_status TEXT NOT NULL CHECK (to_status IN ('created', 'succeeded', 'failed')),
    occurred_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT payment_history_source_event_key UNIQUE (source, event_id),
    CONSTRAINT payment_history_event_fk FOREIGN KEY (source, event_id)
        REFERENCES payment_events(source, event_id)
);
CREATE INDEX payment_history_payment_idx ON payment_history (payment_id, occurred_at);

CREATE TABLE ingestion_conflicts (
    id BIGSERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    event_id TEXT NOT NULL,
    incoming_hash CHAR(64) NOT NULL,
    incoming_payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ingestion_conflicts_event_fk FOREIGN KEY (source, event_id)
        REFERENCES payment_events(source, event_id)
);
CREATE INDEX ingestion_conflicts_event_idx ON ingestion_conflicts (source, event_id, received_at DESC);

CREATE TABLE company_daily_metrics (
    company_id TEXT NOT NULL REFERENCES companies(company_id),
    metric_date DATE NOT NULL,
    total_events BIGINT NOT NULL DEFAULT 0,
    total_payments BIGINT NOT NULL DEFAULT 0,
    total_amount_minor BIGINT NOT NULL DEFAULT 0,
    succeeded_count BIGINT NOT NULL DEFAULT 0,
    failed_count BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (company_id, metric_date)
);

-- +goose Down
DROP TABLE IF EXISTS company_daily_metrics;
DROP TABLE IF EXISTS ingestion_conflicts;
DROP TABLE IF EXISTS payment_history;
DROP TABLE IF EXISTS processed_messages;
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS payment_events;
DROP TABLE IF EXISTS companies;
