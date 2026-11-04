//go:build cluster

package tools

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/multigres/multigres/go/pb/multiadmin/multiadminconnect"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/config"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/testenv"
)

func TestNoTransactionWaitsForTheAdminServer(t *testing.T) {
	poolersCall := multiadminconnect.MultiadminServiceGetPoolersProcedure
	proxy := newAdminProxy(t)
	proxy.set(func(p *adminProxy) { p.hold[poolersCall] = 1500 * time.Millisecond })
	cfg := clusterConfig(config.Restricted)
	cfg.AdminURL = proxy.URL()
	h := newHarness(t, cfg)
	watcher := primary(t)
	asked := len(proxy.callsTo(poolersCall))
	done := make(chan string, 1)
	go func() {
		out := output{redact: h.deps.redact}
		_ = h.deps.executeSQL(context.Background(), sqlInput{SQL: "SELECT 1"}, &out)
		done <- out.finish()
	}()
	for deadline := time.Now().Add(5 * time.Second); len(proxy.callsTo(poolersCall)) == asked; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("execute_sql never asked the admin server for the pooler list")
		}
	}
	time.Sleep(500 * time.Millisecond)
	waiting := testenv.Count(t, watcher, "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE application_name = 'multigres-mcp' AND state LIKE 'idle in transaction%'")
	text := <-done
	if waiting != 0 {
		t.Fatalf("%d sessions of this server sat idle in a transaction while the admin server held the pooler list:\n%s", waiting, text)
	}
	mustContain(t, text, "answered_by: "+primaryName(t))
}

func TestSlowQueriesAreNotJudgedWhenNeitherSourceAnswers(t *testing.T) {
	proxy := newAdminProxy(t)
	proxy.set(func(p *adminProxy) { p.swallow[multiadminconnect.MultiadminServiceGetGatewayQueriesProcedure] = true })
	cfg := clusterConfig(config.Restricted)
	cfg.AdminURL = proxy.URL()
	locker := primary(t)
	var schema string
	if err := locker.QueryRow(context.Background(), "SELECT n.nspname FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = 'pg_stat_statements'").Scan(&schema); err != nil {
		t.Fatalf("pg_stat_statements is not installed: %v", err)
	}
	lock, err := locker.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(context.Background())
	if _, err := lock.Exec(context.Background(), "LOCK TABLE "+pgx.Identifier{schema, "pg_stat_statements"}.Sanitize()+" IN ACCESS EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, cfg)
	slow := sectionOf(t, h.mustCall("diagnose", diagnoseInput{Checks: []string{"slow_queries"}}), "slow_queries")
	mustContain(t, slow, "could not check", "lock timeout", "swallowed by the test proxy")
	if strings.Contains(slow, "verdict:") {
		t.Fatalf("a slow query check that neither source answered has a verdict:\n%s", slow)
	}
}
