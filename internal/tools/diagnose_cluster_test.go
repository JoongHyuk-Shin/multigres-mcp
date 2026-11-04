//go:build cluster

package tools

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/admin"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/config"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/diag"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/testenv"
)

var cacheRate = regexp.MustCompile(`(\w+) cache hit rate: ([0-9.]+)%`)

func namedClient(t *testing.T, name string) *pgx.Conn {
	t.Helper()
	conn := testenv.Connect(t, testenv.GatewayURL()+"?application_name="+name)
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

func checkSection(t *testing.T, text, name string) string {
	t.Helper()
	body := sectionOf(t, text, name)
	if strings.Contains(body, "could not check") {
		t.Fatalf("%s could not run:\n%s", name, body)
	}
	return body
}

func sectionOf(t *testing.T, text, name string) string {
	t.Helper()
	for i, check := range diag.Names() {
		if check != name {
			continue
		}
		start := strings.Index(text, fmt.Sprintf("\n%d. %s\n", i+1, name))
		if start < 0 {
			t.Fatalf("no %s section:\n%s", name, text)
		}
		body := text[start+1:]
		if end := strings.Index(body[1:], fmt.Sprintf("\n\n%d. ", i+2)); end >= 0 {
			body = body[:end+1]
		}
		return body
	}
	t.Fatalf("%s is not a check", name)
	return ""
}

func TestDiagnoseShowsInducedState(t *testing.T) {
	schema := testenv.Schema(t, "diag")
	setup := gateway(t)
	testenv.Exec(t, setup, "SELECT pg_stat_statements_reset()")
	testenv.Exec(t, setup, fmt.Sprintf(`CREATE TABLE %[1]s.dead (id int, pad text) WITH (autovacuum_enabled = off);
INSERT INTO %[1]s.dead SELECT g, repeat('x', 100) FROM generate_series(1, 20000) g;
DELETE FROM %[1]s.dead WHERE id > 1000;
CREATE TABLE %[1]s.scanned (id int, v int);
INSERT INTO %[1]s.scanned SELECT g, g %% 1000 FROM generate_series(1, 50000) g;
CREATE TABLE %[1]s.dup (a int, b int);
CREATE INDEX dup_first ON %[1]s.dup (a);
CREATE INDEX dup_second ON %[1]s.dup (a);
CREATE TABLE %[1]s.seqfull (id serial, v int);
SELECT setval('%[1]s.seqfull_id_seq', 2000000000);
CREATE TABLE %[1]s.checked (a int);
ALTER TABLE %[1]s.checked ADD CONSTRAINT checked_positive CHECK (a > 0) NOT VALID;
CREATE TABLE %[1]s.slowmark (a int);
INSERT INTO %[1]s.slowmark VALUES (1);
CREATE TABLE %[1]s.lockme (a int);
INSERT INTO %[1]s.lockme VALUES (1)`, schema))
	for i := 0; i < 40; i++ {
		testenv.Count(t, setup, fmt.Sprintf("SELECT count(*) FROM %s.scanned WHERE v = 5", schema))
	}
	for i := 0; i < 3; i++ {
		testenv.Exec(t, setup, fmt.Sprintf("SELECT pg_sleep(3) FROM %s.slowmark", schema))
	}

	holder := namedClient(t, "diag-holder")
	testenv.Exec(t, holder, "BEGIN")
	testenv.Exec(t, holder, fmt.Sprintf("UPDATE %s.lockme SET a = a", schema))
	waiter := namedClient(t, "diag-waiter")
	waited := make(chan error, 1)
	go func() {
		_, err := waiter.Exec(context.Background(), fmt.Sprintf("BEGIN; LOCK TABLE %s.lockme IN SHARE MODE; ROLLBACK", schema))
		waited <- err
	}()
	idler := namedClient(t, "diag-idle")
	testenv.Exec(t, idler, "BEGIN")
	testenv.Exec(t, idler, "SELECT 1")
	watcher := primary(t)
	for deadline := time.Now().Add(10 * time.Second); testenv.Count(t, watcher, "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'diag-waiter' AND wait_event_type = 'Lock'") == 0; {
		if time.Now().After(deadline) {
			t.Fatal("the waiting session never started waiting")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := setup.Exec(context.Background(), "SELECT 1/0"); err == nil {
		t.Fatal("division by zero did not fail")
	}
	time.Sleep(1500 * time.Millisecond)

	h := newHarness(t, clusterConfig(config.Restricted))
	text := h.mustCall("diagnose", diagnoseInput{})
	testenv.Exec(t, holder, "ROLLBACK")
	if err := <-waited; err != nil {
		t.Fatalf("waiting session: %v", err)
	}
	testenv.Exec(t, idler, "ROLLBACK")

	mustContain(t, checkSection(t, text, "bloat"), `"relname": "dead"`, `"n_dead_tup": 19000`, "pgstattuple extension is not installed")
	mustContain(t, checkSection(t, text, "vacuum"), "verdict: healthy")
	mustContain(t, checkSection(t, text, "locks"), `"waiting_application": "diag-waiter"`, `"blocking_application": "diag-holder"`)
	activity := checkSection(t, text, "activity")
	mustContain(t, activity, `"application_name": "diag-idle"`, `"state": "idle in transaction"`, "verdict: healthy")
	mustContain(t, checkSection(t, text, "waits"), `"wait_event_type": "Lock"`)
	mustContain(t, checkSection(t, text, "slow_queries"), schema+".slowmark")
	mustContain(t, checkSection(t, text, "missing_indexes"), `"relname": "scanned"`)
	cache := checkSection(t, text, "cache")
	low := false
	for _, match := range cacheRate.FindAllStringSubmatch(cache, -1) {
		rate, _ := strconv.ParseFloat(match[2], 64)
		low = low || rate < 95
	}
	if want := map[bool]string{true: "verdict: warning", false: "verdict: healthy"}[low]; !strings.Contains(cache, want) {
		t.Fatalf("cache verdict does not follow the rates:\n%s", cache)
	}
	mustContain(t, checkSection(t, text, "log"), "division by zero")
	mustContain(t, checkSection(t, text, "index_health"), "invalid indexes:\n[]\n", "dup_first", "dup_second", "verdict: warning")
	mustContain(t, checkSection(t, text, "sequences"), "seqfull_id_seq", "verdict: warning")
	mustContain(t, checkSection(t, text, "replication"), "this node is a replica: false")
	mustContain(t, checkSection(t, text, "constraints"), "checked_positive", "verdict: warning")

	for _, pooler := range poolerList(t) {
		if !admin.IsPrimary(pooler) {
			replica := h.mustCall("diagnose", diagnoseInput{Node: admin.NodeName(pooler), Checks: []string{"replication"}})
			mustContain(t, checkSection(t, replica, "replication"), "this node is a replica: true")
		}
	}
}
