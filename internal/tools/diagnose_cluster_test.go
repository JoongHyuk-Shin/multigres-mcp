//go:build cluster

package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
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

	pad := strings.Repeat("x", 600)
	holder := namedClient(t, "diag-holder")
	testenv.Exec(t, holder, "BEGIN")
	testenv.Exec(t, holder, fmt.Sprintf("UPDATE %s.lockme SET a = a WHERE length('%s') > 0", schema, pad))
	waiter := namedClient(t, "diag-waiter")
	waited := make(chan error, 1)
	go func() {
		_, err := waiter.Exec(context.Background(), fmt.Sprintf("SELECT a, '%s' AS pad FROM %s.lockme FOR UPDATE", pad, schema))
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
	locks := checkSection(t, text, "locks")
	mustContain(t, locks, `"waiting_application": "diag-waiter"`, `"blocking_application": "diag-holder"`)
	for _, column := range []string{"waiting_query", "blocking_query"} {
		shown := regexp.MustCompile(`"` + column + `": "([^"]*)"`).FindStringSubmatch(locks)
		if shown == nil || len(shown[1]) != 500 {
			t.Fatalf("%s is not the first 500 characters of a longer statement:\n%s", column, locks)
		}
	}
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

func TestToolsWithoutAdminServerReportItInsteadOfCrashing(t *testing.T) {
	cfg := clusterConfig(config.Restricted)
	cfg.AdminURL = ""
	h := newHarness(t, cfg)
	notConfigured := admin.ErrNotConfigured.Error()

	mustContain(t, sectionOf(t, h.mustCall("diagnose", diagnoseInput{}), "log"), "could not check: "+notConfigured)

	listed, err := h.client.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	withNode := 0
	for _, tool := range listed.Tools {
		raw, err := json.Marshal(tool.InputSchema)
		if err != nil {
			t.Fatal(err)
		}
		var schema struct {
			Properties map[string]any `json:"properties"`
		}
		if err := json.Unmarshal(raw, &schema); err != nil {
			t.Fatal(err)
		}
		if _, ok := schema.Properties["node"]; !ok {
			continue
		}
		withNode++
		text, isError := h.call(tool.Name, map[string]any{"node": "primary"})
		if !isError {
			t.Fatalf("%s with a node and no admin server did not fail:\n%s", tool.Name, text)
		}
		mustContain(t, text, notConfigured)
	}
	if withNode != 2 {
		t.Fatalf("expected get_top_queries and diagnose to take a node without an admin server, found %d tools", withNode)
	}
}

func TestLongStatementLeavesRoomForGatewayAndLaterChecks(t *testing.T) {
	schema := testenv.Schema(t, "longtext")
	setup := gateway(t)
	testenv.Exec(t, setup, "SELECT pg_stat_statements_reset()")
	columns := make([]string, 1000)
	for i := range columns {
		columns[i] = fmt.Sprintf("column_with_a_long_name_to_make_the_statement_long_%04d int", i)
	}
	statement := fmt.Sprintf("CREATE TABLE %s.long_statement (%s)", schema, strings.Join(columns, ", "))
	testenv.Exec(t, setup, statement)
	h := newHarness(t, clusterConfig(config.Restricted))
	shownIn := func(name, text string, preview int) map[string]any {
		t.Helper()
		if strings.Contains(text, "[result truncated") {
			t.Fatalf("one long statement filled the %s result:\n%s", name, text)
		}
		mustContain(t, text, "gateway query statistics, ")
		row := statementRow(t, text, statement[:100])
		if query := row["query"].(string); query != statement[:preview] {
			t.Fatalf("%s shows %d characters of the statement, want the first %d", name, len(query), preview)
		}
		if length := row["query_length"].(json.Number).String(); length != strconv.Itoa(len(statement)) {
			t.Fatalf("%s gives query_length %s for a statement of %d characters", name, length, len(statement))
		}
		return row
	}

	var top string
	var topRow map[string]any
	for _, sortBy := range []string{"resources", "total_time", "mean_time"} {
		top = h.mustCall("get_top_queries", topQueriesInput{SortBy: sortBy})
		topRow = shownIn("get_top_queries sort_by "+sortBy, top, topPreview)
	}
	diagnosed := h.mustCall("diagnose", diagnoseInput{Checks: []string{"slow_queries", "constraints"}})
	shownIn("diagnose", checkSection(t, diagnosed, "slow_queries"), diagnosePreview)
	mustContain(t, checkSection(t, diagnosed, "slow_queries"), "verdict: none")
	mustContain(t, checkSection(t, diagnosed, "constraints"), "verdict: ")

	guide := regexp.MustCompile(`SELECT substr\(query, 1, (\d+)\) FROM \S+ WHERE queryid = <queryid> LIMIT 1`).FindStringSubmatch(top)
	if guide == nil {
		t.Fatalf("no instruction for reading the full text:\n%s", top)
	}
	piece, _ := strconv.Atoi(guide[1])
	first := h.mustCall("execute_sql", sqlInput{SQL: strings.Replace(guide[0], "<queryid>", fmt.Sprint(topRow["queryid"]), 1)})
	if strings.Contains(first, "[result truncated") || !strings.Contains(first, statement[:min(piece, len(statement))]) {
		t.Fatalf("the instruction's first piece is not the first %d characters of the statement:\n%s", piece, first)
	}
}

func TestGatewayRowsShowTheStartOfLongStatements(t *testing.T) {
	const shown = 500
	const marker = "SELECT pg_sleep($1) AS long_gateway_row_"
	testenv.Exec(t, gateway(t), "SELECT pg_stat_statements_reset()")
	aliases := make([]string, 120)
	for i := range aliases {
		aliases[i] = fmt.Sprintf("%d AS column_with_a_long_name_to_fill_the_gateway_text_%03d", i, i)
	}
	failures := make(chan error, 11)
	var wg sync.WaitGroup
	for n := range 11 {
		statement := fmt.Sprintf(`SELECT pg_sleep(2) AS long_gateway_row_%02d, 1 AS "가나다라마바사아자차카타파하", %s`, n, strings.Join(aliases, ", "))
		conn := gateway(t)
		wg.Go(func() {
			for range 6 {
				if _, err := conn.Exec(context.Background(), statement); err != nil {
					failures <- err
					return
				}
			}
		})
	}
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}

	h := newHarness(t, clusterConfig(config.Restricted))
	gateways, err := h.deps.Admin.Gateways(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	type keptQuery struct {
		text        []rune
		fingerprint string
	}
	kept := map[string]keptQuery{}
	for _, gw := range gateways {
		answer, err := h.deps.Admin.GatewayQueries(context.Background(), gw.GetId())
		if err != nil {
			t.Fatal(err)
		}
		for _, query := range answer.GetSnapshot().GetQueries() {
			if text := query.GetNormalizedSql(); strings.HasPrefix(text, marker) && len([]rune(text)) > shown {
				kept[text[:len(marker)+2]] = keptQuery{text: []rune(text), fingerprint: query.GetFingerprint()}
			}
		}
	}
	if len(kept) != 11 {
		t.Fatalf("the gateways kept %d of the 11 long statements", len(kept))
	}

	results := map[string]string{
		"get_top_queries": h.mustCall("get_top_queries", topQueriesInput{SortBy: "total_time"}),
		"diagnose":        h.mustCall("diagnose", diagnoseInput{}),
	}
	for name, text := range results {
		if strings.Contains(text, "[result truncated") {
			t.Fatalf("long gateway rows filled the %s result:\n%s", name, text)
		}
		label := strings.Index(text, "gateway query statistics, ")
		note := strings.Index(text, fmt.Sprintf("normalized_sql is the first %d characters", shown))
		if label < 0 || note < 0 || note > label {
			t.Fatalf("%s does not say before the gateway rows that they show the first %d characters:\n%s", name, shown, text)
		}
		mustContain(t, text[note:label], "fingerprint tells apart rows that start alike", "do not pass normalized_sql to explain_query or the index advisors")
		var rows []gatewayQuery
		if err := json.NewDecoder(strings.NewReader(text[label+strings.Index(text[label:], "["):])).Decode(&rows); err != nil {
			t.Fatalf("%s gateway rows: %v\n%s", name, err, text)
		}
		long := 0
		for _, row := range rows {
			if !strings.HasPrefix(row.NormalizedSQL, marker) {
				continue
			}
			long++
			full := kept[row.NormalizedSQL[:len(marker)+2]]
			if row.NormalizedSQL != string(full.text[:shown]) {
				t.Fatalf("%s shows %d characters of a gateway row, want the first %d of the %d the gateway kept", name, len([]rune(row.NormalizedSQL)), shown, len(full.text))
			}
			if row.Fingerprint != full.fingerprint {
				t.Fatalf("%s gives fingerprint %q for a gateway row the gateway keeps as %q", name, row.Fingerprint, full.fingerprint)
			}
		}
		if long == 0 {
			t.Fatalf("no long gateway row in the %s result:\n%s", name, text)
		}
	}
	mustContain(t, checkSection(t, results["diagnose"], "constraints"), "verdict: ")
}

func statementRow(t *testing.T, text, start string) map[string]any {
	t.Helper()
	at := strings.Index(text, "pg_stat_statements, ")
	if at < 0 {
		t.Fatalf("no pg_stat_statements rows:\n%s", text)
	}
	decoder := json.NewDecoder(strings.NewReader(text[at+strings.Index(text[at:], "["):]))
	decoder.UseNumber()
	var rows []map[string]any
	if err := decoder.Decode(&rows); err != nil {
		t.Fatalf("pg_stat_statements rows: %v\n%s", err, text)
	}
	for _, row := range rows {
		if query, _ := row["query"].(string); strings.HasPrefix(query, start) {
			return row
		}
	}
	t.Fatalf("no pg_stat_statements row starts with %q:\n%s", start, text)
	return nil
}
