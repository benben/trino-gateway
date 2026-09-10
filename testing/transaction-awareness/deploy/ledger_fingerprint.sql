WITH q AS MATERIALIZED (
 SELECT q.query_id,q.incarnation::text AS incarnation,b.current_name,
   length(q.query_id)::text||':'||q.query_id||length(q.incarnation::text)::text||':'||q.incarnation::text||length(b.current_name)::text||':'||b.current_name AS encoded
 FROM transaction_query q JOIN transaction_backend b USING(incarnation) WHERE NOT q.terminal
), a AS MATERIALIZED (
 SELECT admission_id::text AS id,incarnation::text AS incarnation,state,
   length(admission_id::text)::text||':'||admission_id::text||length(incarnation::text)::text||':'||incarnation::text||length(state)::text||':'||state AS encoded
 FROM transaction_admission WHERE state<>'COMPLETE'
), t AS MATERIALIZED (
 SELECT transaction_id AS id,incarnation::text AS incarnation,state,
   length(transaction_id)::text||':'||transaction_id||length(incarnation::text)::text||':'||incarnation::text||length(state)::text||':'||state AS encoded
 FROM transaction_binding WHERE state='OPEN'
), b AS MATERIALIZED (
 SELECT current_name,incarnation::text AS incarnation,state,generation,encode(sha256(convert_to(incarnation::text,'UTF8')),'hex') AS incarnation_hash,
   length(current_name)::text||':'||current_name||length(incarnation::text)::text||':'||incarnation::text AS encoded
 FROM transaction_backend WHERE current_name IS NOT NULL
), groups AS (
 SELECT encode(sha256(convert_to(incarnation,'UTF8')),'hex') AS incarnation_hash,count(*) AS count,
   encode(sha256(convert_to(string_agg(encoded,'' ORDER BY query_id COLLATE "C"),'UTF8')),'hex') AS sha256
 FROM q GROUP BY incarnation
)
SELECT jsonb_build_object('sample_time',to_char(statement_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
 'ascii_valid', (SELECT coalesce(bool_and(encoded IS NOT NULL AND query_id~'^[A-Za-z0-9_]+$' AND incarnation~'^[0-9a-f-]{36}$' AND current_name~'^[a-z0-9-]+$'),true) FROM q)
   AND (SELECT coalesce(bool_and(id~'^[0-9a-f-]{36}$' AND incarnation~'^[0-9a-f-]{36}$' AND state IN ('PENDING','UNCERTAIN')),true) FROM a)
   AND (SELECT coalesce(bool_and(id~'^[A-Za-z0-9_-]+$' AND incarnation~'^[0-9a-f-]{36}$' AND state='OPEN'),true) FROM t)
   AND (SELECT coalesce(bool_and(encoded IS NOT NULL AND current_name~'^[a-z0-9-]+$' AND incarnation~'^[0-9a-f-]{36}$'),true) FROM b),
 'nonterminal_queries',(SELECT jsonb_build_object('count',count(*),'sha256',encode(sha256(convert_to(coalesce(string_agg(encoded,'' ORDER BY query_id COLLATE "C"),''),'UTF8')),'hex')) FROM q),
 'pending_admissions',(SELECT jsonb_build_object('count',count(*),'sha256',encode(sha256(convert_to(coalesce(string_agg(encoded,'' ORDER BY id COLLATE "C"),''),'UTF8')),'hex')) FROM a),
 'open_transactions',(SELECT jsonb_build_object('count',count(*),'sha256',encode(sha256(convert_to(coalesce(string_agg(encoded,'' ORDER BY id COLLATE "C"),''),'UTF8')),'hex')) FROM t),
 'query_groups',coalesce((SELECT jsonb_agg(groups ORDER BY incarnation_hash) FROM groups),'[]'::jsonb),
 'backends',coalesce((SELECT jsonb_agg(jsonb_build_object('name',current_name,'state',state,'generation',generation,'incarnation_hash',incarnation_hash) ORDER BY current_name) FROM b),'[]'::jsonb),
 'backend_set_sha256',(SELECT encode(sha256(convert_to(coalesce(string_agg(encoded,'' ORDER BY current_name COLLATE "C"),''),'UTF8')),'hex') FROM b),
 'retained_terminal_counts',coalesce((SELECT jsonb_agg(x ORDER BY x.current_name) FROM
   (SELECT b.current_name,count(*) AS count FROM transaction_query q JOIN transaction_backend b USING(incarnation)
    WHERE q.terminal AND q.retain_until>statement_timestamp() GROUP BY b.current_name) x),'[]'::jsonb));
