ALTER TABLE transaction_backend ADD COLUMN current_name VARCHAR(256);
UPDATE transaction_backend SET current_name = name;
ALTER TABLE transaction_backend ADD CONSTRAINT transaction_backend_current_name_key UNIQUE (current_name);

ALTER TABLE transaction_route DROP CONSTRAINT transaction_route_backend_name_fkey;
ALTER TABLE transaction_route ADD CONSTRAINT transaction_route_backend_name_fkey
    FOREIGN KEY (backend_name) REFERENCES transaction_backend(current_name);

ALTER TABLE transaction_backend DROP CONSTRAINT transaction_backend_name_key;
ALTER TABLE transaction_backend DROP CONSTRAINT transaction_backend_backend_url_key;
ALTER TABLE transaction_backend DROP CONSTRAINT transaction_backend_node_id_coordinator_id_key;

CREATE UNIQUE INDEX transaction_backend_current_url_idx
    ON transaction_backend (backend_url) WHERE current_name IS NOT NULL;
CREATE UNIQUE INDEX transaction_backend_current_process_idx
    ON transaction_backend (node_id, coordinator_id) WHERE current_name IS NOT NULL;
