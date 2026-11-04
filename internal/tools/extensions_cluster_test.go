//go:build cluster

package tools

import (
	"context"
	"fmt"
	"testing"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/config"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/db"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/diag"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/testenv"
)

func TestToolsNameTheMissingExtension(t *testing.T) {
	schema := ordersSchema(t, "noext")
	setup := gateway(t)
	extensions := []string{"hypopg", "pg_stat_statements"}
	versions := make([]string, len(extensions))
	for i, name := range extensions {
		if err := setup.QueryRow(context.Background(), "SELECT extversion FROM pg_extension WHERE extname = $1", name).Scan(&versions[i]); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		testenv.Exec(t, setup, "DROP EXTENSION "+name)
	}
	t.Cleanup(func() {
		for i, name := range extensions {
			testenv.Exec(t, setup, fmt.Sprintf("CREATE EXTENSION %s SCHEMA public VERSION '%s'", name, versions[i]))
		}
	})
	h := newHarness(t, clusterConfig(config.Restricted))
	query := fmt.Sprintf("SELECT * FROM %s.orders WHERE customer_id = 5", schema)

	for _, call := range []toolCall{
		{"explain_query", explainInput{SQL: query, HypotheticalIndexes: []hypotheticalIndex{{Table: schema + ".orders", Columns: []string{"customer_id"}}}}},
		{"analyze_query_indexes", queryIndexesInput{Queries: []string{query}}},
		{"analyze_workload_indexes", workloadIndexesInput{}},
	} {
		text, isError := h.call(call.name, call.args)
		if !isError {
			t.Fatalf("%s worked without hypopg:\n%s", call.name, text)
		}
		mustContain(t, text, db.ErrNoHypopg.Error())
	}
	mustContain(t, h.mustCall("explain_query", explainInput{SQL: query}), "Seq Scan")

	text, isError := h.call("get_top_queries", topQueriesInput{Source: "pg_stat_statements"})
	if !isError {
		t.Fatalf("pg_stat_statements answered without the extension:\n%s", text)
	}
	mustContain(t, text, diag.ErrNoStatements.Error())
	mustContain(t, h.mustCall("get_top_queries", topQueriesInput{}), diag.ErrNoStatements.Error(), `"normalized_sql"`)
	mustContain(t, checkSection(t, h.mustCall("diagnose", diagnoseInput{Checks: []string{"slow_queries"}}), "slow_queries"), "pg_stat_statements: "+diag.ErrNoStatements.Error(), `"normalized_sql"`)
}
