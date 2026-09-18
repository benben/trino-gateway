CREATE TABLE pool (
    pool_id VARCHAR(256) PRIMARY KEY REFERENCES transaction_route(routing_group),
    epoch BIGINT NOT NULL DEFAULT 0 CHECK (epoch >= 0),
    -- Owner identity is bound to the epoch: an equal-epoch mutation from a different
    -- controller identity is refused, so two controllers cannot both act at epoch N.
    owner_identity VARCHAR(256),
    api_mode VARCHAR(16) NOT NULL DEFAULT 'LEGACY' CHECK (api_mode IN ('LEGACY', 'POOLED')),
    min_serving INT NOT NULL DEFAULT 3 CHECK (min_serving >= 1),
    desired_members INT NOT NULL DEFAULT 3 CHECK (desired_members >= 1),
    max_surge INT NOT NULL DEFAULT 1 CHECK (max_surge >= 0),
    max_repair INT NOT NULL DEFAULT 1 CHECK (max_repair >= 0),
    membership_generation BIGINT NOT NULL DEFAULT 0 CHECK (membership_generation >= 0),
    desired_revision VARCHAR(128) NOT NULL DEFAULT '',
    admitted_revision VARCHAR(128) NOT NULL DEFAULT '',
    tenant_admission_enabled BOOLEAN NOT NULL DEFAULT FALSE
);

ALTER TABLE transaction_backend DROP CONSTRAINT transaction_backend_state_check;
ALTER TABLE transaction_backend ADD CONSTRAINT transaction_backend_state_check
    CHECK (state IN ('PREPARING', 'ACTIVE', 'DRAINING', 'SEALED', 'RETIRING', 'RETIRED', 'SUSPECT', 'LOST'));

ALTER TABLE transaction_backend ADD COLUMN pool_id VARCHAR(256) REFERENCES pool(pool_id);
ALTER TABLE transaction_backend ADD COLUMN instance_id VARCHAR(256);
ALTER TABLE transaction_backend ADD COLUMN pod_uid VARCHAR(256);
ALTER TABLE transaction_backend ADD COLUMN boot_id VARCHAR(256);
ALTER TABLE transaction_backend ADD COLUMN config_revision VARCHAR(128);
ALTER TABLE transaction_backend ADD COLUMN certified_revision VARCHAR(128);
ALTER TABLE transaction_backend ADD COLUMN auth_revision VARCHAR(128);
ALTER TABLE transaction_backend ADD COLUMN retirement_kind VARCHAR(16) CHECK (retirement_kind IN ('DRAINED', 'FAILED'));
ALTER TABLE transaction_backend ADD COLUMN repair BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE transaction_backend ADD COLUMN repair_for VARCHAR(256);
ALTER TABLE transaction_backend ADD CONSTRAINT transaction_backend_pool_identity_check
    CHECK ((pool_id IS NULL) = (instance_id IS NULL)
        AND (pool_id IS NULL) = (pod_uid IS NULL)
        AND (pool_id IS NULL) = (boot_id IS NULL)
        AND (pool_id IS NOT NULL OR NOT repair)
        AND (repair OR repair_for IS NULL)
        AND (NOT repair OR repair_for IS NOT NULL));

-- Instance identities, pooled endpoints and pooled process identities are never reused, including by
-- retired or lost members, so a retirement claim can never be undone by re-registration and an
-- endpoint can never be rebound while any obligation of its previous incarnation is unresolved.
--
-- This is safe because the operator names every instance-scoped object, including the coordinator
-- Service, after a never-reused instance identity: a replacement gets a new instance id and
-- therefore a new endpoint, so permanent uniqueness cannot deadlock repair or replacement.
CREATE UNIQUE INDEX transaction_backend_pool_instance_idx ON transaction_backend (pool_id, instance_id) WHERE pool_id IS NOT NULL;
CREATE UNIQUE INDEX transaction_backend_pool_endpoint_idx ON transaction_backend (backend_url) WHERE pool_id IS NOT NULL;
CREATE UNIQUE INDEX transaction_backend_pool_process_idx ON transaction_backend (pod_uid, boot_id) WHERE pool_id IS NOT NULL;
CREATE INDEX transaction_backend_pool_state_idx ON transaction_backend (pool_id, state) WHERE pool_id IS NOT NULL;

CREATE TABLE pool_operation (
    operation_id VARCHAR(256) NOT NULL,
    step_id VARCHAR(64) NOT NULL,
    pool_id VARCHAR(256) NOT NULL REFERENCES pool(pool_id),
    payload_hash CHAR(64) NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    epoch BIGINT NOT NULL CHECK (epoch >= 0),
    outcome VARCHAR(16) NOT NULL CHECK (outcome IN ('OK', 'FAILED')),
    result JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (pool_id, operation_id, step_id)
);
CREATE INDEX pool_operation_operation_idx ON pool_operation (operation_id, step_id);

CREATE TABLE pool_member_certificate (
    incarnation UUID NOT NULL REFERENCES transaction_backend(incarnation),
    config_revision VARCHAR(128) NOT NULL,
    certificate_hash CHAR(64) NOT NULL CHECK (certificate_hash ~ '^[0-9a-f]{64}$'),
    auth_revision VARCHAR(128) NOT NULL,
    pod_uid VARCHAR(256) NOT NULL,
    boot_id VARCHAR(256) NOT NULL,
    node_id VARCHAR(256) NOT NULL,
    coordinator_id VARCHAR(256) NOT NULL,
    ready_workers INT NOT NULL CHECK (ready_workers >= 1),
    checks JSONB NOT NULL,
    operation_id VARCHAR(256) NOT NULL,
    issued_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (incarnation, config_revision)
);

CREATE TABLE pool_failure_receipt (
    incarnation UUID PRIMARY KEY REFERENCES transaction_backend(incarnation),
    pool_id VARCHAR(256) NOT NULL REFERENCES pool(pool_id),
    operation_id VARCHAR(256) NOT NULL,
    evidence VARCHAR(32) NOT NULL CHECK (evidence IN ('PROCESS_TERMINATED', 'DESTRUCTIVE_OVERRIDE')),
    detail JSONB NOT NULL,
    outstanding_admissions BIGINT NOT NULL CHECK (outstanding_admissions >= 0),
    outstanding_transactions BIGINT NOT NULL CHECK (outstanding_transactions >= 0),
    outstanding_queries BIGINT NOT NULL CHECK (outstanding_queries >= 0),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE pool_publication (
    publication_id VARCHAR(256) PRIMARY KEY,
    pool_id VARCHAR(256) NOT NULL REFERENCES pool(pool_id),
    tenant VARCHAR(256) NOT NULL,
    target_revision VARCHAR(128) NOT NULL,
    membership_generation BIGINT NOT NULL CHECK (membership_generation >= 0),
    phase VARCHAR(16) NOT NULL CHECK (phase IN ('OPEN', 'ADMITTED', 'ABANDONED')),
    payload_hash CHAR(64) NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    required_members JSONB NOT NULL,
    operation_id VARCHAR(256) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE UNIQUE INDEX pool_publication_open_idx ON pool_publication (pool_id, tenant) WHERE phase = 'OPEN';
CREATE INDEX pool_publication_pool_idx ON pool_publication (pool_id, phase);

CREATE TABLE pool_publication_receipt (
    publication_id VARCHAR(256) NOT NULL REFERENCES pool_publication(publication_id),
    incarnation UUID NOT NULL REFERENCES transaction_backend(incarnation),
    pod_uid VARCHAR(256) NOT NULL,
    boot_id VARCHAR(256) NOT NULL,
    applied_revision VARCHAR(128) NOT NULL,
    auth_fingerprint CHAR(64) NOT NULL CHECK (auth_fingerprint ~ '^[0-9a-f]{64}$'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (publication_id, incarnation)
);

CREATE TABLE pool_tenant_admission (
    pool_id VARCHAR(256) NOT NULL REFERENCES pool(pool_id),
    tenant VARCHAR(256) NOT NULL,
    state VARCHAR(16) NOT NULL CHECK (state IN ('PENDING', 'ADMITTED', 'REVOKED')),
    admitted_revision VARCHAR(128),
    publication_id VARCHAR(256),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (pool_id, tenant),
    CHECK (state <> 'ADMITTED' OR (admitted_revision IS NOT NULL AND publication_id IS NOT NULL))
);
