//go:build cluster

package tools

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/multiadmin/multiadminconnect"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/admin"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/config"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/testenv"
)

var readProcedures = map[string]bool{
	multiadminconnect.MultiadminServiceGetCellNamesProcedure:       true,
	multiadminconnect.MultiadminServiceGetCellProcedure:            true,
	multiadminconnect.MultiadminServiceGetDatabaseNamesProcedure:   true,
	multiadminconnect.MultiadminServiceGetDatabaseProcedure:        true,
	multiadminconnect.MultiadminServiceGetGatewaysProcedure:        true,
	multiadminconnect.MultiadminServiceGetOrchsProcedure:           true,
	multiadminconnect.MultiadminServiceGetPoolersProcedure:         true,
	multiadminconnect.MultiadminServiceGetPoolerStatusProcedure:    true,
	multiadminconnect.MultiadminServiceGetBackupsProcedure:         true,
	multiadminconnect.MultiadminServiceVerifyBackupsProcedure:      true,
	multiadminconnect.MultiadminServiceGetBackupJobStatusProcedure: true,
	multiadminconnect.MultiadminServiceGetGatewayQueriesProcedure:  true,
}

type toolCall struct {
	name string
	args any
}

func readToolCalls(schema string) []toolCall {
	query := fmt.Sprintf("SELECT * FROM %s.orders WHERE customer_id = 5", schema)
	return []toolCall{
		{"list_schemas", noInput{}},
		{"list_objects", objectsInput{SchemaName: schema}},
		{"list_objects", objectsInput{SchemaName: schema, ObjectType: "nope"}},
		{"get_object_details", objectInput{SchemaName: schema, ObjectName: "orders"}},
		{"get_object_details", objectInput{SchemaName: schema, ObjectName: "nope", ObjectType: "nope"}},
		{"execute_sql", sqlInput{SQL: "SELECT 1"}},
		{"execute_sql", sqlInput{SQL: fmt.Sprintf("UPDATE %s.orders SET amount = 0", schema)}},
		{"explain_query", explainInput{SQL: query, HypotheticalIndexes: []hypotheticalIndex{{Table: schema + ".orders", Columns: []string{"customer_id"}}}}},
		{"explain_query", explainInput{SQL: "SELECT * FROM no_such_table"}},
		{"get_top_queries", topQueriesInput{}},
		{"get_top_queries", topQueriesInput{Source: "nope"}},
		{"get_top_queries", topQueriesInput{Node: "nope"}},
		{"analyze_workload_indexes", workloadIndexesInput{}},
		{"analyze_query_indexes", queryIndexesInput{Queries: []string{query}}},
		{"analyze_query_indexes", queryIndexesInput{Queries: []string{"DELETE FROM x"}, MaxIndexSizeMB: -1}},
		{"diagnose", diagnoseInput{}},
		{"diagnose", diagnoseInput{Node: "primary", Checks: []string{"cache", "log"}}},
		{"diagnose", diagnoseInput{Checks: []string{"nope"}}},
		{"diagnose", diagnoseInput{Node: "nope"}},
		{"cluster_status", clusterStatusInput{}},
		{"list_backups", listBackupsInput{}},
		{"list_backups", listBackupsInput{shardInput: shardInput{Shard: "nope"}}},
		{"verify_backups", shardInput{}},
		{"verify_backups", shardInput{Shard: "nope"}},
		{"get_backup_job_status", jobInput{JobID: "no-such-job"}},
		{"get_backup_job_status", jobInput{}},
		{"read_node_log", nodeLogInput{MaxBytes: 4096}},
		{"read_node_log", nodeLogInput{Node: "nope"}},
		{"read_node_log", nodeLogInput{Pattern: "("}},
	}
}

func TestReadOnlyToolsCallOnlyReadProcedures(t *testing.T) {
	schema := ordersSchema(t, "adminscope")
	proxy := newAdminProxy(t)
	cfg := clusterConfig(config.Restricted)
	cfg.AdminURL = proxy.URL()
	h := newHarness(t, cfg)
	tools := map[string]bool{}
	for _, call := range readToolCalls(schema) {
		tools[call.name] = true
		h.call(call.name, call.args)
	}
	for name, kind := range designAnnotations {
		if kind == "r" && !tools[name] {
			t.Errorf("read tool %s was not called", name)
		}
	}
	calls := proxy.recorded()
	if len(proxy.callsTo(multiadminconnect.MultiadminServiceGetPoolersProcedure)) == 0 {
		t.Fatalf("the proxy saw no GetPoolers call; recorded %v", calls)
	}
	for _, call := range calls {
		if !readProcedures[call.procedure] {
			t.Errorf("restricted mode called %s", call.procedure)
		}
	}
}

func poolerList(t *testing.T) []*clustermetadatapb.Multipooler {
	t.Helper()
	poolers, err := admin.NewReader(testenv.AdminURL(), "").Poolers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return poolers
}

func TestNodeDirectFollowsTheHostMap(t *testing.T) {
	poolers := poolerList(t)
	if len(poolers) < 2 {
		t.Fatalf("expected two poolers, found %d", len(poolers))
	}
	proxy := newAdminProxy(t)
	proxy.set(func(p *adminProxy) {
		p.rewrite = func(list []*clustermetadatapb.Multipooler) {
			for _, pooler := range list {
				pooler.Hostname = "pooler-1.svc"
			}
		}
	})
	mapped := clusterConfig(config.Restricted)
	mapped.AdminURL = proxy.URL()
	mapped.NodeHostMap = map[string]string{"pooler-1.svc": testenv.NodeHost()}
	withMap := newHarness(t, mapped)
	unmapped := mapped
	unmapped.NodeHostMap = nil
	withoutMap := newHarness(t, unmapped)

	for _, pooler := range poolers {
		node := admin.NodeName(pooler)
		mustContain(t, withMap.mustCall("read_node_log", nodeLogInput{Node: node, MaxBytes: 1024}), "node: "+node)
		mustContain(t, withMap.mustCall("diagnose", diagnoseInput{Node: node, Checks: []string{"cache"}}), node)
		text, isError := withoutMap.call("read_node_log", nodeLogInput{Node: node, MaxBytes: 1024})
		if !isError {
			t.Fatalf("%s: an unreachable reported host worked without a map entry:\n%s", node, text)
		}
		mustContain(t, text, "node "+node+" (direct connection)", "pooler-1.svc")
	}

	refused := admin.NodeName(poolers[0])
	proxy.set(func(p *adminProxy) {
		p.rewrite = func(list []*clustermetadatapb.Multipooler) {
			for _, pooler := range list {
				pooler.Hostname = "pooler-1.svc"
				if admin.NodeName(pooler) == refused {
					pooler.PortMap["postgres"] = 1
				}
			}
		}
	})
	text, isError := withMap.call("read_node_log", nodeLogInput{Node: refused, MaxBytes: 1024})
	if !isError {
		t.Fatalf("a closed node port did not fail:\n%s", text)
	}
	mustContain(t, text, "node "+refused+" (direct connection)", "refused")
	other := admin.NodeName(poolers[1])
	mustContain(t, withMap.mustCall("read_node_log", nodeLogInput{Node: other, MaxBytes: 1024}), "node: "+other)
}

func TestSQLToolsDoNotWaitForAStalledAdminServer(t *testing.T) {
	proxy := newAdminProxy(t)
	proxy.set(func(p *adminProxy) { p.hold[multiadminconnect.MultiadminServiceGetPoolersProcedure] = 8 * time.Second })
	cfg := clusterConfig(config.Restricted)
	cfg.AdminURL = proxy.URL()
	h := newHarness(t, cfg)
	started := time.Now()
	text := h.mustCall("execute_sql", sqlInput{SQL: "SELECT 1"})
	if elapsed := time.Since(started); elapsed > 3*time.Second {
		t.Fatalf("execute_sql took %s while the admin server held the pooler list:\n%s", elapsed, text)
	}
	mustContain(t, text, "rows: 1", "answered_by: unknown (admin API")
}

func TestAnsweredByNeedsExactlyOneMatchingNode(t *testing.T) {
	proxy := newAdminProxy(t)
	cfg := clusterConfig(config.Restricted)
	cfg.AdminURL = proxy.URL()
	h := newHarness(t, cfg)
	var primaryName, primaryDir string
	for _, pooler := range poolerList(t) {
		if admin.IsPrimary(pooler) {
			primaryName, primaryDir = admin.NodeName(pooler), pooler.GetPgDataDir()
		}
	}
	mustContain(t, h.mustCall("execute_sql", sqlInput{SQL: "SELECT 1"}), "answered_by: "+primaryName+" (primary)")

	proxy.set(func(p *adminProxy) {
		p.rewrite = func(list []*clustermetadatapb.Multipooler) {
			for _, pooler := range list {
				pooler.PgDataDir = ""
			}
		}
	})
	mustContain(t, h.mustCall("execute_sql", sqlInput{SQL: "SELECT 1"}), "answered_by: unknown (0 nodes report this data directory)")

	proxy.set(func(p *adminProxy) {
		p.rewrite = func(list []*clustermetadatapb.Multipooler) {
			for _, pooler := range list {
				pooler.PgDataDir = primaryDir
			}
		}
	})
	text := h.mustCall("execute_sql", sqlInput{SQL: "SELECT 1"})
	mustContain(t, text, "answered_by: unknown (2 nodes report this data directory)")
	if strings.Contains(text, primaryName) {
		t.Fatalf("a pooler was named although two report the same data directory:\n%s", text)
	}
}
