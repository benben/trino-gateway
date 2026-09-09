CREATE TABLE transaction_backend (
    incarnation UUID PRIMARY KEY,
    name VARCHAR(256) NOT NULL UNIQUE,
    backend_url TEXT NOT NULL UNIQUE,
    external_url TEXT NOT NULL,
    routing_group VARCHAR(256) NOT NULL,
    node_id VARCHAR(256),
    coordinator_id VARCHAR(256),
    state VARCHAR(16) NOT NULL CHECK (state IN ('ACTIVE', 'DRAINING', 'SEALED')),
    generation BIGINT NOT NULL DEFAULT 0 CHECK (generation >= 0),
    UNIQUE (node_id, coordinator_id)
);

CREATE TABLE transaction_binding (
    transaction_id VARCHAR(256) PRIMARY KEY,
    owner_hash TEXT NOT NULL,
    incarnation UUID NOT NULL REFERENCES transaction_backend(incarnation),
    start_query_id VARCHAR(256) NOT NULL,
    state VARCHAR(16) NOT NULL CHECK (state IN ('OPEN', 'CLOSED'))
);
CREATE INDEX transaction_binding_backend_idx ON transaction_binding(incarnation, state);

CREATE TABLE transaction_query (
    query_id VARCHAR(256) PRIMARY KEY,
    owner_hash TEXT NOT NULL,
    incarnation UUID NOT NULL REFERENCES transaction_backend(incarnation),
    transaction_id VARCHAR(256) REFERENCES transaction_binding(transaction_id),
    terminal BOOLEAN NOT NULL DEFAULT FALSE,
    retain_until TIMESTAMPTZ,
    CHECK (NOT terminal OR retain_until IS NOT NULL)
);
CREATE INDEX transaction_query_backend_idx ON transaction_query(incarnation);

CREATE TABLE transaction_admission (
    admission_id UUID PRIMARY KEY,
    incarnation UUID NOT NULL REFERENCES transaction_backend(incarnation),
    owner_hash TEXT NOT NULL,
    transaction_id VARCHAR(256) REFERENCES transaction_binding(transaction_id),
    query_id VARCHAR(256) REFERENCES transaction_query(query_id),
    state VARCHAR(16) NOT NULL CHECK (state IN ('PENDING', 'UNCERTAIN', 'COMPLETE')),
    observation TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (state <> 'COMPLETE' OR observation IS NOT NULL)
);
CREATE INDEX transaction_admission_backend_idx ON transaction_admission(incarnation, state);

CREATE TABLE transaction_route (
    routing_group VARCHAR(256) PRIMARY KEY,
    backend_name VARCHAR(256) REFERENCES transaction_backend(name),
    generation BIGINT NOT NULL DEFAULT 0 CHECK (generation >= 0)
);
