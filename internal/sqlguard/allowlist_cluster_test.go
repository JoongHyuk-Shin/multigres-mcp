//go:build cluster

package sqlguard

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/testenv"
)

const catalogBaseline = "testdata/allowed_functions_pg17.tsv"

var keptVolatile = []string{
	"array_sample", "array_shuffle", "clock_timestamp", "current_query", "currval", "gen_random_uuid", "lastval",
	"pg_available_wal_summaries", "pg_blocking_pids", "pg_control_checkpoint", "pg_control_init", "pg_control_recovery",
	"pg_control_system", "pg_current_logfile", "pg_database_size", "pg_get_wal_summarizer_state", "pg_indexes_size",
	"pg_is_in_recovery", "pg_jit_available", "pg_notification_queue_usage", "pg_relation_size",
	"pg_safe_snapshot_blocking_pids", "pg_table_size", "pg_tablespace_size", "pg_total_relation_size",
	"pg_wal_summary_contents", "pg_xact_commit_timestamp", "timeofday",
}

func allowedNames() []string {
	names := make([]string, 0, len(allowedFunctions))
	for name := range allowedFunctions {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func TestAllowedFunctionsMatchTheReviewedCatalog(t *testing.T) {
	conn := testenv.Connect(t, testenv.NodeURL(testenv.PrimaryPort(t)))
	defer conn.Close(context.Background())
	rows, err := conn.Query(context.Background(), `SELECT p.proname, pg_get_function_identity_arguments(p.oid), p.prosrc, p.provolatile::text, coalesce(p.proargnames, '{}')
FROM pg_proc p
WHERE p.pronamespace = 'pg_catalog'::regnamespace AND p.proname = ANY($1)
ORDER BY 1, 2`, allowedNames())
	if err != nil {
		t.Fatal(err)
	}
	var lines []string
	found := map[string]bool{}
	volatile := map[string]bool{}
	for rows.Next() {
		var name, args, source, volatility string
		var argNames []string
		if err := rows.Scan(&name, &args, &source, &volatility, &argNames); err != nil {
			t.Fatal(err)
		}
		found[name] = true
		if volatility == "v" {
			volatile[name] = true
		}
		if slices.Contains(argNames, "query") {
			t.Errorf("%s(%s) takes an argument named query", name, args)
		}
		lines = append(lines, strings.Join([]string{name, args, strconv.Quote(source), volatility}, "\t"))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, name := range allowedNames() {
		if !found[name] {
			t.Errorf("allowed function %s is not in pg_catalog", name)
		}
	}
	for _, name := range []string{"pg_current_xact_id", "txid_current"} {
		if allowedFunctions[name] {
			t.Errorf("%s takes a transaction id but is allowed", name)
		}
	}
	gotVolatile := make([]string, 0, len(volatile))
	for name := range volatile {
		gotVolatile = append(gotVolatile, name)
	}
	sort.Strings(gotVolatile)
	if !slices.Equal(gotVolatile, keptVolatile) {
		t.Errorf("volatile allowed functions differ from the 28 reviewed ones:\n got %v\nwant %v", gotVolatile, keptVolatile)
	}

	current := strings.Join(lines, "\n") + "\n"
	if os.Getenv("MULTIGRES_MCP_UPDATE_BASELINE") == "1" {
		if err := os.WriteFile(catalogBaseline, []byte(current), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	saved, err := os.ReadFile(catalogBaseline)
	if err != nil {
		t.Fatal(err)
	}
	if added, removed := lineDiff(string(saved), current); len(added)+len(removed) > 0 {
		t.Fatalf("the catalog rows of allowed functions changed; review them, then rerun with MULTIGRES_MCP_UPDATE_BASELINE=1\nnew:\n%s\ngone:\n%s", strings.Join(added, "\n"), strings.Join(removed, "\n"))
	}
}

func lineDiff(before, after string) (added, removed []string) {
	old := map[string]bool{}
	for _, line := range strings.Split(before, "\n") {
		old[line] = true
	}
	now := map[string]bool{}
	for _, line := range strings.Split(after, "\n") {
		now[line] = true
		if !old[line] {
			added = append(added, line)
		}
	}
	for line := range old {
		if !now[line] {
			removed = append(removed, line)
		}
	}
	return added, removed
}

func TestKeptVolatileFunctionsLeaveNoSessionState(t *testing.T) {
	schema := testenv.Schema(t, "volatile")
	setup := testenv.Connect(t, testenv.GatewayURL())
	testenv.Exec(t, setup, fmt.Sprintf("CREATE TABLE %[1]s.t (a int PRIMARY KEY); CREATE SEQUENCE %[1]s.seq", schema))
	setup.Close(context.Background())
	table := fmt.Sprintf("'%s.t'::regclass", schema)
	samples := map[string]string{
		"array_sample":                   "array_sample(ARRAY[1, 2, 3], 2)",
		"array_shuffle":                  "array_shuffle(ARRAY[1, 2, 3])",
		"clock_timestamp":                "clock_timestamp()",
		"current_query":                  "current_query()",
		"currval":                        fmt.Sprintf("currval('%s.seq'::regclass)", schema),
		"gen_random_uuid":                "gen_random_uuid()",
		"lastval":                        "lastval()",
		"pg_available_wal_summaries":     "pg_available_wal_summaries()",
		"pg_blocking_pids":               "pg_blocking_pids(pg_backend_pid())",
		"pg_control_checkpoint":          "pg_control_checkpoint()",
		"pg_control_init":                "pg_control_init()",
		"pg_control_recovery":            "pg_control_recovery()",
		"pg_control_system":              "pg_control_system()",
		"pg_current_logfile":             "pg_current_logfile()",
		"pg_database_size":               "pg_database_size(current_database())",
		"pg_get_wal_summarizer_state":    "pg_get_wal_summarizer_state()",
		"pg_indexes_size":                "pg_indexes_size(" + table + ")",
		"pg_is_in_recovery":              "pg_is_in_recovery()",
		"pg_jit_available":               "pg_jit_available()",
		"pg_notification_queue_usage":    "pg_notification_queue_usage()",
		"pg_relation_size":               "pg_relation_size(" + table + ")",
		"pg_safe_snapshot_blocking_pids": "pg_safe_snapshot_blocking_pids(pg_backend_pid())",
		"pg_table_size":                  "pg_table_size(" + table + ")",
		"pg_tablespace_size":             "pg_tablespace_size('pg_default')",
		"pg_total_relation_size":         "pg_total_relation_size(" + table + ")",
		"pg_wal_summary_contents":        "pg_wal_summary_contents(1, '0/0'::pg_lsn, '0/0'::pg_lsn)",
		"pg_xact_commit_timestamp":       "pg_xact_commit_timestamp('3'::xid)",
		"timeofday":                      "timeofday()",
	}
	sampled := make([]string, 0, len(samples))
	for name := range samples {
		sampled = append(sampled, name)
	}
	sort.Strings(sampled)
	if !slices.Equal(sampled, keptVolatile) {
		t.Fatalf("samples cover %v, want %v", sampled, keptVolatile)
	}

	conn := testenv.Connect(t, testenv.NodeURL(testenv.PrimaryPort(t)))
	defer conn.Close(context.Background())
	baseline, _ := afterSeed(t, conn, "")
	for _, name := range sampled {
		value, assigned := afterSeed(t, conn, samples[name])
		if value != baseline || assigned {
			t.Errorf("%s: random() after the seed %v (baseline %v), transaction id assigned %t", name, value, baseline, assigned)
		}
		if locks := testenv.Count(t, conn, "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid()"); locks != 0 {
			t.Errorf("%s: %d advisory locks on the session", name, locks)
		}
		if hypothetical := testenv.Count(t, conn, "SELECT count(*) FROM hypopg_list_indexes"); hypothetical != 0 {
			t.Errorf("%s: %d hypothetical indexes on the session", name, hypothetical)
		}
	}
	if value, _ := afterSeed(t, conn, "random()"); value == baseline {
		t.Fatalf("control: random() inside the transaction did not move the sequence (%v)", value)
	}
}

func afterSeed(t *testing.T, conn *pgx.Conn, expression string) (float64, bool) {
	t.Helper()
	ctx := context.Background()
	testenv.Exec(t, conn, "BEGIN TRANSACTION READ ONLY")
	testenv.Exec(t, conn, "SELECT setseed(0.5)")
	if expression != "" {
		testenv.Exec(t, conn, "SAVEPOINT sample")
		if _, err := conn.Exec(ctx, "SELECT "+expression); err != nil {
			t.Logf("%s: %v", expression, err)
			testenv.Exec(t, conn, "ROLLBACK TO SAVEPOINT sample")
		}
	}
	var assigned bool
	if err := conn.QueryRow(ctx, "SELECT pg_current_xact_id_if_assigned() IS NOT NULL").Scan(&assigned); err != nil {
		t.Fatal(err)
	}
	testenv.Exec(t, conn, "ROLLBACK")
	var value float64
	if err := conn.QueryRow(ctx, "SELECT random()").Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value, assigned
}
