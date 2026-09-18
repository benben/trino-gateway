-- The authoritative principal to tenant mapping for the pooled admission restriction.
--
-- The restriction has to key on exactly the principal the coordinator authenticates, and the Gateway
-- cannot derive that string: a tenant's principals are one flat namespace produced by the operator's
-- own projection, they are not a function of the tenant identifier, and a tenant's root login carries
-- no separator at all. Guessing would either refuse a legitimate login or admit a principal the
-- coordinator rejects, so the controller publishes the mapping instead.
--
-- One principal belongs to at most one tenant in a pool: the primary key makes a second tenant
-- claiming the same login a conflict rather than an ambiguous admission.
CREATE TABLE pool_tenant_principal (
    pool_id VARCHAR(256) NOT NULL REFERENCES pool(pool_id),
    principal VARCHAR(512) NOT NULL,
    tenant VARCHAR(256) NOT NULL,
    revision VARCHAR(64) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (pool_id, principal)
);
CREATE INDEX pool_tenant_principal_tenant_idx ON pool_tenant_principal (pool_id, tenant);
