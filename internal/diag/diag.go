package diag

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/db"
)

type Sink interface {
	Line(format string, args ...any) bool
	JSON(label string, value any) bool
}

type Callbacks struct {
	SlowQueries  func(ctx context.Context, tx *db.Tx, sink Sink) error
	Log          func(ctx context.Context, sink Sink) error
	Describe     func(error) string
	CheckTimeout time.Duration
}

type check struct {
	name string
	run  func(ctx context.Context, tx *db.Tx, sink Sink, cb Callbacks) error
}

var checks = []check{
	{"bloat", bloat},
	{"vacuum", vacuum},
	{"locks", locks},
	{"activity", activity},
	{"waits", waits},
	{"slow_queries", func(ctx context.Context, tx *db.Tx, sink Sink, cb Callbacks) error {
		return cb.SlowQueries(ctx, tx, sink)
	}},
	{"missing_indexes", missingIndexes},
	{"cache", cache},
	{"log", func(ctx context.Context, _ *db.Tx, sink Sink, cb Callbacks) error { return cb.Log(ctx, sink) }},
	{"index_health", indexHealth},
	{"sequences", sequences},
	{"replication", replication},
	{"constraints", constraints},
}

func Names() []string {
	names := make([]string, len(checks))
	for i, c := range checks {
		names[i] = c.name
	}
	return names
}

func Validate(selected []string) error {
	for _, name := range selected {
		if !isCheck(name) {
			return fmt.Errorf("unknown check %q; checks: %s", name, strings.Join(Names(), ", "))
		}
	}
	return nil
}

func Run(ctx context.Context, tx *db.Tx, selected []string, sink Sink, cb Callbacks) error {
	want := map[string]bool{}
	for _, name := range selected {
		want[name] = true
	}
	filtered, err := ourSessions(ctx, tx)
	if err != nil {
		return err
	}
	sink.Line("sessions of this server left out of activity, lock and wait checks: %d", filtered)
	for i, c := range checks {
		if len(want) > 0 && !want[c.name] {
			continue
		}
		if !sink.Line("") || !sink.Line("%d. %s", i+1, c.name) {
			return nil
		}
		checkCtx, cancel := context.WithTimeout(ctx, cb.CheckTimeout)
		err := tx.Try(checkCtx, func() error { return c.run(checkCtx, tx, sink, cb) })
		cancel()
		if err != nil {
			sink.Line("could not check: %s", cb.Describe(err))
		}
	}
	return nil
}

func Count(selected []string) int {
	if len(selected) == 0 {
		return len(checks)
	}
	return len(selected)
}

func isCheck(name string) bool {
	for _, c := range checks {
		if c.name == name {
			return true
		}
	}
	return false
}

func ourSessions(ctx context.Context, tx *db.Tx) (int64, error) {
	var n int64
	err := tx.QueryRow(ctx, ourSessionsQuery, db.ApplicationName).Scan(&n)
	return n, err
}

func rowsOf(ctx context.Context, tx *db.Tx, query db.Query, args ...any) ([]map[string]any, error) {
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToMap)
}

func show(ctx context.Context, tx *db.Tx, sink Sink, label string, query db.Query, args ...any) ([]map[string]any, error) {
	rows, err := rowsOf(ctx, tx, query, args...)
	if err != nil {
		return nil, err
	}
	sink.JSON(label, rows)
	return rows, nil
}

func ExtensionSchema(ctx context.Context, tx *db.Tx, name string) (string, error) {
	var schema *string
	err := tx.QueryRow(ctx, extensionLookupQuery, name).Scan(&schema)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if schema == nil {
		return "", nil
	}
	return *schema, nil
}

func withSchema(template db.Query, schema string) db.Query {
	return db.Query(fmt.Sprintf(string(template), pgx.Identifier{schema}.Sanitize()))
}

func bloat(ctx context.Context, tx *db.Tx, sink Sink, _ Callbacks) error {
	sink.Line("verdict: none (no threshold in postgres-mcp; ranked values only)")
	sink.Line("dead rows are per-node statistics estimates, not the free space left in the table")
	if _, err := show(ctx, tx, sink, "(a) dead row pressure, by dead rows", deadRowsQuery); err != nil {
		return err
	}
	schema, err := ExtensionSchema(ctx, tx, "pgstattuple")
	if err != nil {
		return err
	}
	if schema == "" {
		sink.Line("(b) free space: the pgstattuple extension is not installed; install it to measure free space with pgstattuple_approx")
		return nil
	}
	err = tx.Try(ctx, func() error {
		_, err := show(ctx, tx, sink, "(b) free space of the 10 largest tables (pgstattuple_approx skips pages marked all-visible)", withSchema(freeSpaceTemplate, schema))
		return err
	})
	if err != nil {
		sink.Line("(b) free space could not be measured (needs superuser or the pg_stat_scan_tables role): %v", err)
	}
	return nil
}

func vacuum(ctx context.Context, tx *db.Tx, sink Sink, _ Callbacks) error {
	danger, err := show(ctx, tx, sink, "tables under 10,000,000 transactions before wraparound", wraparoundQuery)
	if err != nil {
		return err
	}
	if len(danger) > 0 {
		sink.Line("verdict: danger (postgres-mcp threshold: fewer than 10,000,000 transactions left)")
	} else {
		sink.Line("verdict: healthy (no table under the postgres-mcp threshold of 10,000,000 transactions left)")
	}
	if _, err := show(ctx, tx, sink, "autovacuum and autoanalyze, oldest first", autovacuumQuery); err != nil {
		return err
	}
	_, err = show(ctx, tx, sink, "vacuums in progress", vacuumProgressQuery)
	return err
}

func locks(ctx context.Context, tx *db.Tx, sink Sink, _ Callbacks) error {
	sink.Line("verdict: none (no threshold in postgres-mcp)")
	_, err := show(ctx, tx, sink, "waiting sessions and the sessions blocking them", locksQuery, db.ApplicationName)
	return err
}

func activity(ctx context.Context, tx *db.Tx, sink Sink, _ Callbacks) error {
	var total, idle, maxConnections int64
	if err := tx.QueryRow(ctx, connectionsQuery).Scan(&total, &idle, &maxConnections); err != nil {
		return err
	}
	sink.Line("connections: %d of max_connections %d; idle in transaction: %d", total, maxConnections, idle)
	switch {
	case total > maxTotalConnections:
		sink.Line("verdict: warning (more than %d connections, postgres-mcp threshold)", maxTotalConnections)
	case idle > maxIdleConnections:
		sink.Line("verdict: warning (more than %d connections idle in transaction, postgres-mcp threshold)", maxIdleConnections)
	default:
		sink.Line("verdict: healthy (postgres-mcp thresholds: %d connections, %d idle in transaction)", maxTotalConnections, maxIdleConnections)
	}
	sink.Line("sessions that came through the gateway have no client_addr and show the query as the gateway rewrote it; application_name is shown instead")
	_, err := show(ctx, tx, sink, "active and idle-in-transaction sessions, longest first", sessionsQuery, db.ApplicationName)
	return err
}

func waits(ctx context.Context, tx *db.Tx, sink Sink, _ Callbacks) error {
	sink.Line("verdict: none (no threshold in postgres-mcp)")
	_, err := show(ctx, tx, sink, "wait events of active sessions", waitsQuery, db.ApplicationName)
	return err
}

func missingIndexes(ctx context.Context, tx *db.Tx, sink Sink, _ Callbacks) error {
	sink.Line("verdict: none (no threshold in postgres-mcp)")
	if _, err := show(ctx, tx, sink, "tables with the most rows read by sequential scans", seqScanQuery); err != nil {
		return err
	}
	sink.Line("for index suggestions, call analyze_workload_indexes or analyze_query_indexes")
	return nil
}

func cache(ctx context.Context, tx *db.Tx, sink Sink, _ Callbacks) error {
	var database, tables, indexes *float64
	if err := tx.QueryRow(ctx, hitRatesQuery).Scan(&database, &tables, &indexes); err != nil {
		return err
	}
	low := false
	for _, rate := range []struct {
		label string
		value *float64
	}{{"database", database}, {"table", tables}, {"index", indexes}} {
		if rate.value == nil {
			sink.Line("%s cache hit rate: no statistics", rate.label)
			continue
		}
		percent := *rate.value * 100
		low = low || percent < hitRateThreshold
		sink.Line("%s cache hit rate: %.1f%%", rate.label, percent)
	}
	if low {
		sink.Line("verdict: warning (below %.0f%%, postgres-mcp threshold)", hitRateThreshold)
	} else {
		sink.Line("verdict: healthy (postgres-mcp threshold %.0f%%)", hitRateThreshold)
	}
	return nil
}

func constraints(ctx context.Context, tx *db.Tx, sink Sink, _ Callbacks) error {
	invalid, err := show(ctx, tx, sink, "constraints that are not validated (NOT VALID)", invalidConstraintsQuery)
	if err != nil {
		return err
	}
	if len(invalid) > 0 {
		sink.Line("verdict: warning (postgres-mcp reports any invalid constraint)")
	} else {
		sink.Line("verdict: healthy")
	}
	return nil
}

func replication(ctx context.Context, tx *db.Tx, sink Sink, _ Callbacks) error {
	var replica bool
	var lag *float64
	if err := tx.QueryRow(ctx, replicationLagQuery).Scan(&replica, &lag); err != nil {
		return err
	}
	sink.Line("this node is a replica: %t", replica)
	if lag != nil {
		sink.Line("replication lag: %.1f seconds", *lag)
	}
	if _, err := show(ctx, tx, sink, "replication slots", slotsQuery); err != nil {
		return err
	}
	if _, err := show(ctx, tx, sink, "replication connections from this node", walSendersQuery); err != nil {
		return err
	}
	sink.Line("verdict: none (postgres-mcp reports lag and slots without a threshold); for the Multigres view of primaries, replicas and lag, call cluster_status")
	return nil
}
