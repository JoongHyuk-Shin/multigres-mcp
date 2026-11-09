package diag

import "github.com/JoongHyuk-Shin/multigres-mcp/internal/db"

const (
	maxTotalConnections = 500
	maxIdleConnections  = 100
	hitRateThreshold    = 95.0
	maxIndexScans       = 50
	minIndexBloat       = 100 * 1024 * 1024
	sequenceThreshold   = 0.9
)

const (
	ourSessionsQuery     db.Query = `SELECT pg_catalog.count(*) FROM pg_catalog.pg_stat_activity WHERE application_name = $1`
	extensionSchemaQuery db.Query = `SELECT n.nspname FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = $1`

	deadRowsQuery db.Query = `SELECT schemaname, relname, n_dead_tup, n_live_tup,
       pg_catalog.round(n_dead_tup::numeric / nullif(n_live_tup + n_dead_tup, 0), 4)::float8 AS dead_fraction,
       pg_catalog.pg_total_relation_size(relid) AS total_bytes
FROM pg_catalog.pg_stat_user_tables
ORDER BY n_dead_tup DESC, relname
LIMIT 20`
	freeSpaceTemplate db.Query = `SELECT n.nspname AS schema, c.relname AS table, s.table_len, s.dead_tuple_count, s.dead_tuple_len,
       s.approx_free_space, s.approx_free_percent
FROM (SELECT c.oid FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg\_toast%%'
      ORDER BY pg_catalog.pg_relation_size(c.oid) DESC LIMIT 10) big
JOIN pg_catalog.pg_class c ON c.oid = big.oid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL %[1]s.pgstattuple_approx(c.oid::regclass) s
ORDER BY s.table_len DESC`

	wraparoundQuery db.Query = `SELECT n.nspname AS schema, c.relname AS table,
       2146483648 - greatest(pg_catalog.age(c.relfrozenxid), pg_catalog.age(t.relfrozenxid)) AS transactions_left
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_class t ON c.reltoastrelid = t.oid
WHERE c.relkind = 'r' AND (2146483648 - greatest(pg_catalog.age(c.relfrozenxid), pg_catalog.age(t.relfrozenxid))) < 10000000
ORDER BY 3, 1, 2`
	autovacuumQuery db.Query = `SELECT schemaname, relname, last_autovacuum, last_autoanalyze, autovacuum_count, autoanalyze_count, n_dead_tup
FROM pg_catalog.pg_stat_user_tables
ORDER BY last_autovacuum NULLS FIRST, relname
LIMIT 20`
	vacuumProgressQuery db.Query = `SELECT p.pid, p.relid::regclass::text AS relation, p.phase, p.heap_blks_total, p.heap_blks_scanned, p.heap_blks_vacuumed
FROM pg_catalog.pg_stat_progress_vacuum p`

	locksQuery db.Query = `SELECT w.pid AS waiting_pid, w.application_name AS waiting_application, w.wait_event_type, w.wait_event,
       (pg_catalog.now() - w.query_start)::text AS waiting_for, pg_catalog.left(w.query, 500) AS waiting_query,
       b.pid AS blocking_pid, b.application_name AS blocking_application, b.state AS blocking_state, pg_catalog.left(b.query, 500) AS blocking_query
FROM pg_catalog.pg_stat_activity w
CROSS JOIN LATERAL pg_catalog.unnest(pg_catalog.pg_blocking_pids(w.pid)) AS blocker(pid)
JOIN pg_catalog.pg_stat_activity b ON b.pid = blocker.pid
WHERE w.application_name <> $1
ORDER BY w.query_start`

	connectionsQuery db.Query = `SELECT pg_catalog.count(*), pg_catalog.count(*) FILTER (WHERE state = 'idle in transaction'),
       pg_catalog.current_setting('max_connections')::bigint
FROM pg_catalog.pg_stat_activity`
	sessionsQuery db.Query = `SELECT pid, application_name, state, backend_type, (pg_catalog.now() - query_start)::text AS running_for,
       (pg_catalog.now() - xact_start)::text AS transaction_for, wait_event_type, wait_event, pg_catalog.left(query, 500) AS query
FROM pg_catalog.pg_stat_activity
WHERE state IN ('active', 'idle in transaction', 'idle in transaction (aborted)') AND application_name <> $1 AND pid <> pg_catalog.pg_backend_pid()
ORDER BY coalesce(xact_start, query_start) NULLS LAST
LIMIT 30`
	waitsQuery db.Query = `SELECT wait_event_type, wait_event, pg_catalog.count(*) AS sessions
FROM pg_catalog.pg_stat_activity
WHERE state = 'active' AND wait_event IS NOT NULL AND application_name <> $1
GROUP BY 1, 2
ORDER BY 3 DESC, 1, 2`

	seqScanQuery db.Query = `SELECT schemaname, relname, seq_scan, seq_tup_read, idx_scan, n_live_tup
FROM pg_catalog.pg_stat_user_tables
WHERE seq_scan > 0
ORDER BY seq_tup_read DESC, relname
LIMIT 10`

	hitRatesQuery db.Query = `SELECT
  (SELECT blks_hit::float8 / nullif(blks_hit + blks_read, 0) FROM pg_catalog.pg_stat_database WHERE datname = pg_catalog.current_database()),
  (SELECT pg_catalog.sum(heap_blks_hit)::float8 / nullif(pg_catalog.sum(heap_blks_hit + heap_blks_read), 0) FROM pg_catalog.pg_statio_user_tables),
  (SELECT pg_catalog.sum(idx_blks_hit)::float8 / nullif(pg_catalog.sum(idx_blks_hit + idx_blks_read), 0) FROM pg_catalog.pg_statio_user_indexes)`

	invalidConstraintsQuery db.Query = `SELECT nsp.nspname AS schema, rel.relname AS table, con.conname AS name,
       fnsp.nspname AS referenced_schema, frel.relname AS referenced_table
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid
LEFT JOIN pg_catalog.pg_class frel ON frel.oid = con.confrelid
LEFT JOIN pg_catalog.pg_namespace nsp ON nsp.oid = con.connamespace
LEFT JOIN pg_catalog.pg_namespace fnsp ON fnsp.oid = frel.relnamespace
WHERE NOT con.convalidated
ORDER BY 1, 2, 3`

	replicationLagQuery db.Query = `SELECT pg_catalog.pg_is_in_recovery(),
       CASE WHEN NOT pg_catalog.pg_is_in_recovery() THEN NULL
            WHEN pg_catalog.pg_last_wal_receive_lsn() = pg_catalog.pg_last_wal_replay_lsn() THEN 0
            ELSE extract(epoch FROM pg_catalog.now() - pg_catalog.pg_last_xact_replay_timestamp())::float8 END`
	slotsQuery db.Query = `SELECT slot_name, slot_type, database, active, wal_status, restart_lsn::text
FROM pg_catalog.pg_replication_slots
ORDER BY slot_name`
	walSendersQuery db.Query = `SELECT application_name, state, sync_state, sent_lsn::text, replay_lsn::text, replay_lag::text
FROM pg_catalog.pg_stat_replication
ORDER BY application_name`

	indexesQuery db.Query = `SELECT schemaname, t.relname, ix.relname,
       pg_catalog.regexp_replace(pg_catalog.pg_get_indexdef(i.indexrelid), '^[^\(]*\((.*)\)$', '\1'),
       pg_catalog.regexp_replace(pg_catalog.pg_get_indexdef(i.indexrelid), '.* USING ([^ ]*) \(.*', '\1'),
       i.indisunique, i.indisprimary, i.indisvalid, i.indexprs::text, i.indpred::text
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class t ON t.oid = i.indrelid
JOIN pg_catalog.pg_class ix ON ix.oid = i.indexrelid
LEFT JOIN pg_catalog.pg_stat_user_indexes ui ON ui.indexrelid = i.indexrelid
WHERE schemaname IS NOT NULL
ORDER BY 1, 2, 3`
	unusedIndexesQuery db.Query = `SELECT schemaname AS schema, relname AS table, indexrelname AS index,
       pg_catalog.pg_relation_size(i.indexrelid) AS size_bytes, idx_scan AS index_scans,
       pg_catalog.pg_get_indexdef(i.indexrelid) AS definition, indisprimary AS primary
FROM pg_catalog.pg_stat_user_indexes ui
JOIN pg_catalog.pg_index i ON ui.indexrelid = i.indexrelid
WHERE NOT indisunique AND idx_scan <= $1
ORDER BY pg_catalog.pg_relation_size(i.indexrelid) DESC, relname`
	indexBloatQuery db.Query = `WITH btree_index_atts AS (
    SELECT nspname, relname, reltuples, relpages, indrelid, relam,
           pg_catalog.regexp_split_to_table(indkey::text, ' ')::smallint AS attnum, indexrelid AS index_oid
    FROM pg_catalog.pg_index
    JOIN pg_catalog.pg_class ON pg_class.oid = pg_index.indexrelid
    JOIN pg_catalog.pg_namespace ON pg_namespace.oid = pg_class.relnamespace
    JOIN pg_catalog.pg_am ON pg_class.relam = pg_am.oid
    WHERE pg_am.amname = 'btree'
),
index_item_sizes AS (
    SELECT i.nspname, i.relname, i.reltuples, i.relpages, i.relam,
           (pg_catalog.quote_ident(s.schemaname) || '.' || pg_catalog.quote_ident(s.tablename))::regclass AS starelid,
           a.attrelid AS table_oid, index_oid,
           pg_catalog.current_setting('block_size')::numeric AS bs,
           CASE WHEN pg_catalog.version() ~ 'mingw32' OR pg_catalog.version() ~ '64-bit' THEN 8 ELSE 4 END AS maxalign,
           24 AS pagehdr,
           CASE WHEN pg_catalog.max(coalesce(s.null_frac, 0)) = 0 THEN 2 ELSE 6 END AS index_tuple_hdr,
           pg_catalog.sum((1 - coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 2048)) AS nulldatawidth
    FROM pg_catalog.pg_attribute AS a
    JOIN pg_catalog.pg_stats AS s ON (pg_catalog.quote_ident(s.schemaname) || '.' || pg_catalog.quote_ident(s.tablename))::regclass = a.attrelid AND s.attname = a.attname
    JOIN btree_index_atts AS i ON i.indrelid = a.attrelid AND a.attnum = i.attnum
    WHERE a.attnum > 0
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),
index_aligned AS (
    SELECT maxalign, bs, nspname, relname AS index_name, reltuples, relpages, relam, table_oid, index_oid,
           (2 + maxalign - CASE WHEN index_tuple_hdr % maxalign = 0 THEN maxalign ELSE index_tuple_hdr % maxalign END
              + nulldatawidth + maxalign - CASE WHEN nulldatawidth::integer % maxalign = 0 THEN maxalign ELSE nulldatawidth::integer % maxalign END
           )::numeric AS nulldatahdrwidth, pagehdr
    FROM index_item_sizes AS s1
),
otta_calc AS (
    SELECT bs, nspname, table_oid, index_oid, index_name, relpages,
           coalesce(pg_catalog.ceil((reltuples * (4 + nulldatahdrwidth)) / (bs - pagehdr::float)) + CASE WHEN am.amname IN ('hash', 'btree') THEN 1 ELSE 0 END, 0) AS otta
    FROM index_aligned AS s2
    LEFT JOIN pg_catalog.pg_am am ON s2.relam = am.oid
),
raw_bloat AS (
    SELECT nspname, c.relname AS table_name, index_name,
           bs * (sub.relpages)::bigint AS totalbytes,
           CASE WHEN sub.relpages <= otta THEN 0 ELSE bs * (sub.relpages - otta)::bigint END AS wastedbytes,
           stat.indexrelid
    FROM otta_calc AS sub
    JOIN pg_catalog.pg_class AS c ON c.oid = sub.table_oid
    JOIN pg_catalog.pg_stat_user_indexes AS stat ON sub.index_oid = stat.indexrelid
)
SELECT nspname AS schema, table_name AS table, index_name AS index, wastedbytes::bigint AS bloat_bytes, totalbytes::bigint AS index_bytes,
       pg_catalog.pg_get_indexdef(rb.indexrelid) AS definition, indisprimary AS primary
FROM raw_bloat rb
JOIN pg_catalog.pg_index i ON i.indexrelid = rb.indexrelid
WHERE wastedbytes >= $1
ORDER BY wastedbytes DESC, index_name`

	sequencesQuery db.Query = `WITH defaults AS (
    SELECT n.nspname AS table_schema, c.relname AS table, a.attname AS column,
           pg_catalog.format_type(a.atttypid, a.atttypmod) AS column_type,
           (pg_catalog.regexp_match(pg_catalog.pg_get_expr(d.adbin, d.adrelid), 'nextval\(\(?''([^'']+)'''))[1] AS sequence_text
    FROM pg_catalog.pg_attribute a
    JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    JOIN pg_catalog.pg_attrdef d ON (a.attrelid, a.attnum) = (d.adrelid, d.adnum)
    WHERE NOT a.attisdropped AND a.attnum > 0
      AND pg_catalog.pg_get_expr(d.adbin, d.adrelid) LIKE 'nextval%'
      AND n.nspname NOT LIKE 'pg\_temp\_%'
)
SELECT d.table_schema, d.table, d.column, d.column_type, s.schemaname AS sequence_schema, s.sequencename AS sequence,
       pg_catalog.has_sequence_privilege(sc.oid, 'SELECT') AS readable, s.last_value,
       (s.last_value::float8 / CASE d.column_type WHEN 'smallint' THEN 32767 WHEN 'integer' THEN 2147483647 ELSE 9223372036854775807 END) AS fraction_used
FROM defaults d
JOIN pg_catalog.pg_class sc ON sc.oid = d.sequence_text::regclass
JOIN pg_catalog.pg_namespace sn ON sn.oid = sc.relnamespace
JOIN pg_catalog.pg_sequences s ON s.schemaname = sn.nspname AND s.sequencename = sc.relname
ORDER BY fraction_used DESC NULLS LAST, 1, 2, 3`

	nodeRoleQuery        db.Query = `SELECT pg_catalog.pg_is_in_recovery()`
	latestPromotionQuery db.Query = `SELECT coordinator_term, leader_id, created_at FROM multigres.rule_history WHERE event_type = 'promotion' ORDER BY coordinator_term DESC, leader_subterm DESC LIMIT 1`
	statementsResetQuery db.Query = `SELECT stats_reset FROM %[1]s.pg_stat_statements_info`
	databaseResetQuery   db.Query = `SELECT stats_reset FROM pg_catalog.pg_stat_database WHERE datname = pg_catalog.current_database()`
	restartedTablesQuery db.Query = `SELECT pg_catalog.count(*) FROM pg_catalog.pg_stat_user_tables s JOIN pg_catalog.pg_class c ON c.oid = s.relid WHERE s.n_live_tup = 0 AND c.reltuples > 0`
)

var (
	roleQuery            = nodeRoleQuery
	extensionLookupQuery = extensionSchemaQuery
	restartedQuery       = restartedTablesQuery
)
