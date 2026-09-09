CREATE TABLE transaction_query_capability (
    query_id VARCHAR(256) NOT NULL REFERENCES transaction_query(query_id),
    capability_hash CHAR(64) NOT NULL CHECK (capability_hash ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (query_id, capability_hash)
);
