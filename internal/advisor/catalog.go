package advisor

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/db"
)

const (
	relationQuery db.Query = `SELECT c.oid, n.nspname, c.relname,
       c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'multigres', 'pg_toast')
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.oid = pg_catalog.to_regclass($1)`
	columnsQuery db.Query = `SELECT a.attrelid, a.attname,
       t.typname IN ('text', 'varchar', 'bpchar') AND (t.typname = 'text' OR a.atttypmod < 0 OR a.atttypmod - 4 > $2),
       s.avg_width
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_stats s
  ON s.schemaname = n.nspname AND s.tablename = c.relname AND s.attname = a.attname AND NOT s.inherited
WHERE a.attrelid = ANY($1) AND a.attnum > 0 AND NOT a.attisdropped`
	existingIndexesQuery db.Query = `SELECT i.indrelid, am.amname,
       ARRAY(SELECT a.attname
             FROM pg_catalog.unnest(i.indkey::pg_catalog.int2[]) WITH ORDINALITY AS k(attnum, ord)
             JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
             WHERE k.ord <= i.indnkeyatts
             ORDER BY k.ord)
FROM pg_catalog.pg_index i
JOIN pg_catalog.pg_class ic ON ic.oid = i.indexrelid
JOIN pg_catalog.pg_am am ON am.oid = ic.relam
WHERE i.indrelid = ANY($1) AND i.indisvalid AND i.indpred IS NULL
  AND NOT (0 = ANY (i.indkey::pg_catalog.int2[]))`
	tableSizeQuery   db.Query = "SELECT pg_catalog.pg_total_relation_size($1::pg_catalog.oid)"
	workloadTemplate db.Query = `SELECT query, calls, total_exec_time / calls
FROM %[1]s.pg_stat_statements
WHERE dbid = (SELECT oid FROM pg_catalog.pg_database WHERE datname = pg_catalog.current_database())
  AND calls >= $1 AND total_exec_time / calls >= $2
ORDER BY total_exec_time DESC
LIMIT $3`
)

const (
	maxTextLength   = 100
	workloadCalls   = 50
	workloadMeanMs  = 5.0
	workloadQueries = 100
)

type table struct {
	oid          uint32
	schema, name string
	ordinary     bool
}

func (t table) String() string { return t.schema + "." + t.name }

func (t table) sanitized() string { return pgx.Identifier{t.schema, t.name}.Sanitize() }

type column struct {
	longText bool
	avgWidth *int32
}

func Workload(ctx context.Context, tx *db.Tx, schema string) ([]Query, error) {
	template := db.Query(fmt.Sprintf(string(workloadTemplate), pgx.Identifier{schema}.Sanitize()))
	rows, err := tx.Query(ctx, template, workloadCalls, workloadMeanMs, workloadQueries)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var queries []Query
	for rows.Next() {
		var text string
		var calls int64
		var mean float64
		if err := rows.Scan(&text, &calls, &mean); err != nil {
			return nil, err
		}
		queries = append(queries, Query{Text: text, Weight: float64(calls) * mean})
	}
	return queries, rows.Err()
}

func resolve(ctx context.Context, tx *db.Tx, rels []relation) (map[relation]table, error) {
	tables := map[relation]table{}
	for _, rel := range rels {
		if _, done := tables[rel]; done {
			continue
		}
		parts := []string{rel.name}
		if rel.schema != "" {
			parts = []string{rel.schema, rel.name}
		}
		var found table
		err := tx.QueryRow(ctx, relationQuery, pgx.Identifier(parts).Sanitize()).Scan(&found.oid, &found.schema, &found.name, &found.ordinary)
		if err == pgx.ErrNoRows {
			tables[rel] = table{}
			continue
		}
		if err != nil {
			return nil, err
		}
		tables[rel] = found
	}
	return tables, nil
}

func loadColumns(ctx context.Context, tx *db.Tx, oids []uint32) (map[uint32]map[string]column, error) {
	rows, err := tx.Query(ctx, columnsQuery, oids, maxTextLength)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns := map[uint32]map[string]column{}
	for rows.Next() {
		var oid uint32
		var name string
		var info column
		if err := rows.Scan(&oid, &name, &info.longText, &info.avgWidth); err != nil {
			return nil, err
		}
		if columns[oid] == nil {
			columns[oid] = map[string]column{}
		}
		columns[oid][name] = info
	}
	return columns, rows.Err()
}

func loadExisting(ctx context.Context, tx *db.Tx, oids []uint32) (map[string]bool, error) {
	rows, err := tx.Query(ctx, existingIndexesQuery, oids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	existing := map[string]bool{}
	for rows.Next() {
		var oid uint32
		var method string
		var names []string
		if err := rows.Scan(&oid, &method, &names); err != nil {
			return nil, err
		}
		existing[indexKey(oid, method, names)] = true
	}
	return existing, rows.Err()
}

func tableSize(ctx context.Context, tx *db.Tx, oid uint32) (int64, error) {
	var size int64
	err := tx.QueryRow(ctx, tableSizeQuery, oid).Scan(&size)
	return size, err
}

func indexKey(oid uint32, method string, columns []string) string {
	return fmt.Sprintf("%d/%s/%q", oid, method, columns)
}
