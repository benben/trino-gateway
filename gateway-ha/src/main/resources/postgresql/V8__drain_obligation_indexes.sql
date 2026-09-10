CREATE INDEX transaction_admission_pending_idx
    ON transaction_admission(incarnation) WHERE state <> 'COMPLETE';

CREATE INDEX transaction_query_running_idx
    ON transaction_query(incarnation) WHERE NOT terminal;

CREATE INDEX transaction_query_retained_idx
    ON transaction_query(incarnation, retain_until) WHERE terminal;
