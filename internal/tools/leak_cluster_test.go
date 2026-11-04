//go:build cluster

package tools

import (
	"context"
	"fmt"
	"testing"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/config"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/db"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/testenv"
)

type ending struct {
	name string
	run  func(t *testing.T)
}

func TestNoHypotheticalIndexReachesOtherClients(t *testing.T) {
	schema := ordersSchema(t, "r6")
	query := fmt.Sprintf("SELECT * FROM %s.orders WHERE customer_id = 5", schema)
	workload := fmt.Sprintf("SELECT count(*) FROM %[1]s.orders a JOIN %[1]s.orders b ON a.customer_id = b.customer_id WHERE a.status = 'open'", schema)
	warm := gateway(t)
	for i := 0; i < 60; i++ {
		testenv.Count(t, warm, workload)
	}
	h := newHarness(t, clusterConfig(config.Restricted))
	sees := planUsesHypothetical(query)
	resetClients(t)
	if seen := countClients(t, sees); seen != (visibility{}) {
		t.Fatalf("hypothetical indexes are visible before the test starts: %+v", seen)
	}

	for _, busy := range []int{0, 6} {
		stop := busySessions(t, busy)
		for _, end := range leakEndings(h, schema, query) {
			end.run(t)
			if seen := countClients(t, sees); seen != (visibility{}) {
				resetClients(t)
				t.Errorf("%s with %d busy sessions: other clients saw a hypothetical index on %d autocommit and %d transaction connections of %d", end.name, busy, seen.autocommit, seen.transaction, clientsPerMode)
			}
		}
		stop()
	}

	create := fmt.Sprintf("SELECT * FROM hypopg_create_index('CREATE INDEX ON %s.orders (customer_id)')", schema)
	for _, inTransaction := range []bool{false, true} {
		disconnect := leaveOnPooledConnection(t, create, inTransaction)
		seen := countClients(t, sees)
		disconnect()
		resetClients(t)
		found := seen.autocommit
		if inTransaction {
			found = seen.transaction
		}
		if found != clientsPerMode {
			t.Errorf("control (left in a transaction: %t): the counter saw the left index on %d of %d connections, %+v", inTransaction, found, clientsPerMode, seen)
		}
	}
}

func leakEndings(h *harness, schema, query string) []ending {
	index := []hypotheticalIndex{{Table: schema + ".orders", Columns: []string{"customer_id"}}}
	broken := append(append([]hypotheticalIndex(nil), index...), hypotheticalIndex{Table: schema + ".orders", Columns: []string{"no_such_column"}})
	joined := fmt.Sprintf("SELECT * FROM %[1]s.orders o JOIN %[1]s.lines l ON l.order_id = o.id WHERE o.customer_id = 5", schema)
	return []ending{
		{"explain_query normal", func(t *testing.T) {
			h.mustCall("explain_query", explainInput{SQL: query, HypotheticalIndexes: index})
		}},
		{"explain_query error", func(t *testing.T) {
			expectError(t, h, "explain_query", explainInput{SQL: query, HypotheticalIndexes: broken}, "no_such_column")
		}},
		{"explain_query cancel", func(t *testing.T) {
			release := lockTable(t, schema+".lines")
			defer release()
			expectCancel(t, h, "l.order_id = o.id", func(ctx context.Context, out *output) error {
				return h.deps.explainQuery(ctx, explainInput{SQL: joined, HypotheticalIndexes: index}, out)
			})
		}},
		{"analyze_query_indexes normal", func(t *testing.T) {
			mustContain(t, h.mustCall("analyze_query_indexes", queryIndexesInput{Queries: []string{query}}), `(\"customer_id\")`)
		}},
		{"analyze_query_indexes error", func(t *testing.T) {
			defer db.BreakIndexSize()()
			expectError(t, h, "analyze_query_indexes", queryIndexesInput{Queries: []string{query}}, "division by zero")
		}},
		{"analyze_query_indexes cancel", func(t *testing.T) {
			defer db.SlowIndexSize()()
			expectCancel(t, h, "pg_sleep(5)", func(ctx context.Context, out *output) error {
				return h.deps.queryIndexes(ctx, queryIndexesInput{Queries: []string{query}}, out)
			})
		}},
		{"analyze_workload_indexes normal", func(t *testing.T) {
			mustContain(t, h.mustCall("analyze_workload_indexes", workloadIndexesInput{}), schema+".orders")
		}},
		{"analyze_workload_indexes error", func(t *testing.T) {
			defer db.BreakIndexSize()()
			expectError(t, h, "analyze_workload_indexes", workloadIndexesInput{}, "division by zero")
		}},
		{"analyze_workload_indexes cancel", func(t *testing.T) {
			defer db.SlowIndexSize()()
			expectCancel(t, h, "pg_sleep(5)", func(ctx context.Context, out *output) error {
				return h.deps.workloadIndexes(ctx, workloadIndexesInput{}, out)
			})
		}},
	}
}

func expectError(t *testing.T, h *harness, name string, args any, cause string) {
	t.Helper()
	text, isError := h.call(name, args)
	if !isError {
		t.Fatalf("%s did not fail:\n%s", name, text)
	}
	mustContain(t, text, cause, cleanEnd)
}

func expectCancel(t *testing.T, h *harness, marker string, run func(context.Context, *output) error) {
	t.Helper()
	text, err := h.cancelWhileRunning(marker, run)
	if err == nil {
		t.Fatalf("the call finished before it was cancelled:\n%s", text)
	}
	mustContain(t, text, cleanEnd)
}
