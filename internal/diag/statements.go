package diag

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/db"
)

const (
	topTotalTemplate db.Query = `SELECT pg_catalog.left(query, $2) AS query, pg_catalog.length(query) AS query_length, queryid,
       calls, total_exec_time, mean_exec_time, rows
FROM %[1]s.pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT $1`
	topMeanTemplate db.Query = `SELECT pg_catalog.left(query, $2) AS query, pg_catalog.length(query) AS query_length, queryid,
       calls, total_exec_time, mean_exec_time, rows
FROM %[1]s.pg_stat_statements
ORDER BY mean_exec_time DESC
LIMIT $1`
	topResourcesTemplate db.Query = `WITH resource_fractions AS (
    SELECT query, queryid, calls, rows, total_exec_time, mean_exec_time, stddev_exec_time,
           shared_blks_hit, shared_blks_read, shared_blks_dirtied, wal_bytes,
           total_exec_time / NULLIF(pg_catalog.sum(total_exec_time) OVER (), 0) AS total_exec_time_frac,
           (shared_blks_hit + shared_blks_read) / NULLIF(pg_catalog.sum(shared_blks_hit + shared_blks_read) OVER (), 0) AS shared_blks_accessed_frac,
           shared_blks_read / NULLIF(pg_catalog.sum(shared_blks_read) OVER (), 0) AS shared_blks_read_frac,
           shared_blks_dirtied / NULLIF(pg_catalog.sum(shared_blks_dirtied) OVER (), 0) AS shared_blks_dirtied_frac,
           wal_bytes / NULLIF(pg_catalog.sum(wal_bytes) OVER (), 0) AS total_wal_bytes_frac
    FROM %[1]s.pg_stat_statements
)
SELECT pg_catalog.left(query, $2) AS query, pg_catalog.length(query) AS query_length, queryid,
       calls, rows, total_exec_time, mean_exec_time, stddev_exec_time,
       total_exec_time_frac, shared_blks_accessed_frac, shared_blks_read_frac,
       shared_blks_dirtied_frac, total_wal_bytes_frac,
       shared_blks_hit, shared_blks_read, shared_blks_dirtied, wal_bytes
FROM resource_fractions
WHERE total_exec_time_frac > $1
   OR shared_blks_accessed_frac > $1
   OR shared_blks_read_frac > $1
   OR shared_blks_dirtied_frac > $1
   OR total_wal_bytes_frac > $1
ORDER BY total_exec_time DESC`
)

const (
	resourceFraction = 0.05
	textPiece        = 20000
)

var ErrNoStatements = errors.New("the pg_stat_statements extension is not installed in this database; add pg_stat_statements to shared_preload_libraries on every node (a restart is needed) and run CREATE EXTENSION pg_stat_statements")

func TopStatements(ctx context.Context, tx *db.Tx, sink Sink, sortBy string, limit, preview int) error {
	schema, err := ExtensionSchema(ctx, tx, "pg_stat_statements")
	if err != nil {
		return err
	}
	if schema == "" {
		return ErrNoStatements
	}
	FullTextGuide(sink, preview, schema)
	switch sortBy {
	case "total_time":
		_, err = show(ctx, tx, sink, fmt.Sprintf("pg_stat_statements, %d queries with the most total time", limit), withSchema(topTotalTemplate, schema), limit, preview)
	case "mean_time":
		_, err = show(ctx, tx, sink, fmt.Sprintf("pg_stat_statements, %d queries with the highest mean time", limit), withSchema(topMeanTemplate, schema), limit, preview)
	default:
		_, err = show(ctx, tx, sink, "pg_stat_statements, queries using more than 5% of any resource (time, blocks accessed, read, dirtied, WAL bytes; postgres-mcp threshold)", withSchema(topResourcesTemplate, schema), resourceFraction, preview)
	}
	return err
}

func FullTextGuide(sink Sink, preview int, schema string) {
	sink.Line("query is the first %d characters of each statement and query_length its full length; read a longer one with execute_sql in pieces of %d characters: SELECT substr(query, 1, %d) FROM %s.pg_stat_statements WHERE queryid = <queryid> LIMIT 1, then substr(query, %d, %d) and so on",
		preview, textPiece, textPiece, pgx.Identifier{schema}.Sanitize(), textPiece+1, textPiece)
}
