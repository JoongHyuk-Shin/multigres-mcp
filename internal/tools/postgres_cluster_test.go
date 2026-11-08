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

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/config"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/db"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/testenv"
)

const cleanEnd = "note: hypopg cleanup: 0 hypothetical and 0 hidden indexes left on this session after reset"

func TestSQLResultsNameTheSettingsSetForTheCall(t *testing.T) {
	schema := testenv.Schema(t, "callsettings")
	testenv.Exec(t, gateway(t), "CREATE TABLE "+schema+".t (a int)")
	h := newHarness(t, clusterConfig(config.Unrestricted))
	settings := "set by this server for this call only: statement_timeout 10000ms"
	var gatewayDefault string
	if err := gateway(t).QueryRow(context.Background(), "SHOW statement_timeout").Scan(&gatewayDefault); err != nil {
		t.Fatal(err)
	}
	before := "Before this call set its own, this server's session had statement_timeout " + gatewayDefault + ":"

	read := h.mustCall("execute_sql", sqlInput{SQL: "SELECT setting, current_setting('statement_timeout') FROM pg_settings WHERE name = 'lock_timeout'"})
	mustContain(t, read, `"2000"`, `"10s"`, settings, "lock_timeout 2s", `search_path pg_catalog, "$user", public`, before)

	write := h.mustCall("execute_write_sql", sqlInput{SQL: "INSERT INTO " + schema + ".t VALUES (1)"})
	mustContain(t, write, settings, "lock_timeout 2s", before)
	if strings.Contains(write, "search_path") {
		t.Fatalf("a write names a search_path it does not set:\n%s", write)
	}
}

func TestBuiltinNamesMustNotBeShadowed(t *testing.T) {
	setup := gateway(t)
	testenv.Exec(t, setup, "CREATE FUNCTION public.abs(text) RETURNS text LANGUAGE sql AS $$ SELECT 'shadow' $$")
	t.Cleanup(func() { testenv.Exec(t, setup, "DROP FUNCTION public.abs(text)") })
	h := newHarness(t, clusterConfig(config.Restricted))

	text, isError := h.call("execute_sql", sqlInput{SQL: "SELECT abs('x')"})
	if !isError || strings.Contains(text, "shadow\"") {
		t.Fatalf("an unqualified name that a public function shadows was sent:\n%s", text)
	}
	mustContain(t, text, "was not sent", "public.abs", "pg_catalog. prefix")
	mustContain(t, h.mustCall("execute_sql", sqlInput{SQL: "SELECT pg_catalog.abs(-3)"}), `["3"]`)

	trimmed := explainInput{SQL: "SELECT TRIM(BOTH 'x' FROM CAST($1 AS text))"}
	mustContain(t, h.mustCall("explain_query", trimmed), "generic plan")
	testenv.Exec(t, setup, "CREATE FUNCTION public.btrim(text, text) RETURNS text LANGUAGE sql AS $$ SELECT 'shadow' $$")
	t.Cleanup(func() { testenv.Exec(t, setup, "DROP FUNCTION public.btrim(text, text)") })
	text, isError = h.call("explain_query", trimmed)
	if !isError {
		t.Fatalf("a name that only the rewritten generic plan text has was not checked:\n%s", text)
	}
	mustContain(t, text, "was not sent", "public.btrim", "pg_catalog. prefix")
}

func TestToolsReleaseAdvisoryLocksTakenInsideViews(t *testing.T) {
	schema := testenv.Schema(t, "toollocks")
	base := 700300
	testenv.Exec(t, gateway(t), fmt.Sprintf(`CREATE FUNCTION %[1]s.take() RETURNS int LANGUAGE sql AS $$
SELECT pg_advisory_lock_shared(%[2]d);
SELECT pg_advisory_lock(%[2]d, %[3]d);
SELECT pg_advisory_lock(%[4]d);
SELECT pg_advisory_lock(%[4]d);
SELECT 1 $$;
CREATE FUNCTION %[1]s.take_and_wait() RETURNS int LANGUAGE sql AS $$ SELECT %[1]s.take(); SELECT pg_sleep(5); SELECT 1 $$;
CREATE VIEW %[1]s.locks AS SELECT %[1]s.take();
CREATE VIEW %[1]s.slow_locks AS SELECT %[1]s.take_and_wait()`, schema, base, base+1, base+2))
	h := newHarness(t, clusterConfig(config.Unrestricted))
	released := "note: released 3 session advisory locks taken inside database objects"
	view := fmt.Sprintf("SELECT * FROM %s.locks", schema)

	ends := map[string]func() (string, bool){
		"normal": func() (string, bool) { return h.call("execute_sql", sqlInput{SQL: view}) },
		"error after the view": func() (string, bool) {
			return h.call("execute_sql", sqlInput{SQL: fmt.Sprintf("SELECT 1 / (l.take - 1) FROM %s.locks l", schema)})
		},
		"cancel while the view runs": func() (string, bool) {
			text, err := h.cancelWhileRunning("slow_locks", func(ctx context.Context, out *output) error {
				return h.deps.executeSQL(ctx, sqlInput{SQL: fmt.Sprintf("SELECT * FROM %s.slow_locks", schema)}, out)
			})
			return text, err != nil
		},
		"write tool": func() (string, bool) { return h.call("execute_write_sql", sqlInput{SQL: view}) },
	}
	for name, end := range ends {
		text, isError := end()
		if isError != (name == "error after the view" || name == "cancel while the view runs") {
			t.Fatalf("%s: unexpected outcome:\n%s", name, text)
		}
		mustContain(t, text, released)
		if held := heldAdvisory(t, base); held != 0 {
			t.Fatalf("%s: %d advisory locks still held after the call", name, held)
		}
	}

	control := testenv.Connect(t, testenv.GatewayURL())
	testenv.Count(t, control, view)
	control.Close(context.Background())
	held := heldAdvisory(t, base)
	testenv.Exec(t, primary(t), "SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'advisory' AND objid IN ($1, $2, $3)", base, base+1, base+2)
	if held != 3 {
		t.Fatalf("control: expected a plain client to leave 3 advisory locks on the gateway, found %d", held)
	}
}

func TestExplainQueryClearsItsHypotheticalIndexes(t *testing.T) {
	schema := ordersSchema(t, "toolhypo")
	h := newHarness(t, clusterConfig(config.Restricted))
	query := fmt.Sprintf("SELECT * FROM %s.orders WHERE customer_id = 5", schema)
	index := hypotheticalIndex{Table: schema + ".orders", Columns: []string{"customer_id"}}

	text := h.mustCall("explain_query", explainInput{SQL: query, HypotheticalIndexes: []hypotheticalIndex{index}})
	if !hypotheticalName.MatchString(text) {
		t.Fatalf("the plan does not use the hypothetical index:\n%s", text)
	}
	mustContain(t, text, cleanEnd)

	release := lockTable(t, schema+".lines")
	text, err := h.cancelWhileRunning("l.order_id = o.id", func(ctx context.Context, out *output) error {
		return h.deps.explainQuery(ctx, explainInput{
			SQL:                 fmt.Sprintf("SELECT * FROM %[1]s.orders o JOIN %[1]s.lines l ON l.order_id = o.id WHERE o.customer_id = 5", schema),
			HypotheticalIndexes: []hypotheticalIndex{index},
		}, out)
	})
	release()
	if err == nil {
		t.Fatalf("the call was not cancelled:\n%s", text)
	}
	mustContain(t, text, "hypothetical index", cleanEnd)

	restore := db.BreakEndReset()
	text, _ = h.call("explain_query", explainInput{SQL: query, HypotheticalIndexes: []hypotheticalIndex{index}})
	restore()
	mustContain(t, text, "cleanup could not be confirmed, so the server ended its Postgres session")
}

func TestTruncatedReadStopsTheStatement(t *testing.T) {
	schema := ordersSchema(t, "bigresult")
	h := newHarness(t, clusterConfig(config.Restricted))
	start := time.Now()
	text := h.mustCall("execute_sql", sqlInput{SQL: fmt.Sprintf("SELECT a.id, b.id FROM %[1]s.orders a CROSS JOIN %[1]s.orders b", schema)})
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("a truncated result took %v", took)
	}
	marker := strings.Index(text, "[result truncated")
	if marker < 0 {
		t.Fatalf("the result was not truncated:\n%s", text[max(0, len(text)-500):])
	}
	mustContain(t, text[marker:], "rows shown: ", "answered_by: ")
	shown := regexp.MustCompile(`rows shown: (\d+)`).FindStringSubmatch(text[marker:])
	if full := len(regexp.MustCompile(`(?m)^\["\d+","\d+"\]$`).FindAllString(text[:marker], -1)); shown == nil || shown[1] != strconv.Itoa(full) {
		t.Fatalf("the result says %v rows were shown and has %d whole rows", shown, full)
	}
	if strings.Contains(text, "ended its Postgres session") {
		t.Fatalf("stopping the read ended the session:\n%s", text[marker:])
	}
	if running := testenv.Count(t, primary(t), "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND state = 'active' AND query LIKE $2", db.ApplicationName, "%"+schema+".orders%"); running != 0 {
		t.Fatalf("%d statements still run after the truncated call", running)
	}
}

func TestOrdinaryRoleEndsItsOwnSession(t *testing.T) {
	schema := ordersSchema(t, "plainrole")
	role, password := unique("mcp_plain_"), unique("Pw-")
	setup := gateway(t)
	testenv.Exec(t, setup, fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", role, password))
	t.Cleanup(func() {
		cleanup := gateway(t)
		testenv.Exec(t, cleanup, "DROP OWNED BY "+role)
		testenv.Exec(t, cleanup, "DROP ROLE "+role)
	})
	testenv.Exec(t, setup, fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s", schema, role))
	testenv.Exec(t, setup, fmt.Sprintf("GRANT SELECT ON ALL TABLES IN SCHEMA %s TO %s", schema, role))
	cfg := clusterConfig(config.Restricted)
	cfg.DatabaseURL = gatewayURLAs(role, password, 15432)
	h := newHarness(t, cfg)
	node := primary(t)
	if superuser := testenv.Count(t, node, "SELECT count(*) FROM pg_roles WHERE rolname = $1 AND rolsuper", role); superuser != 0 {
		t.Fatalf("%s is a superuser", role)
	}

	restore := db.BreakEndReset()
	text, _ := h.call("explain_query", explainInput{
		SQL:                 fmt.Sprintf("SELECT * FROM %s.orders WHERE customer_id = 5", schema),
		HypotheticalIndexes: []hypotheticalIndex{{Table: schema + ".orders", Columns: []string{"customer_id"}}},
	})
	restore()
	mustContain(t, text, "cleanup could not be confirmed, so the server ended its Postgres session (pid ")
	pid := regexp.MustCompile(`ended its Postgres session \(pid (\d+)\)`).FindStringSubmatch(text)[1]
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		if testenv.Count(t, node, "SELECT count(*) FROM pg_stat_activity WHERE pid = $1::int AND usename = $2", pid, role) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("session %s of %s is still there", pid, role)
		}
	}
}

func TestCatalogToolsShowTheWholeSchemaToARoleWithoutPrivileges(t *testing.T) {
	schema := testenv.Schema(t, "catalogrole")
	setup := gateway(t)
	testenv.Exec(t, setup, fmt.Sprintf(`CREATE TABLE %[1]s.parent (id int PRIMARY KEY);
CREATE TABLE %[1]s.orders (id bigint PRIMARY KEY, parent_id int REFERENCES %[1]s.parent (id), code varchar(20) UNIQUE, CONSTRAINT positive CHECK (id > 0));
CREATE VIEW %[1]s.recent AS SELECT id FROM %[1]s.orders;
CREATE SEQUENCE %[1]s.ticket START 5`, schema))
	role, password := unique("mcp_none_"), unique("Pw-")
	testenv.Exec(t, setup, fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", role, password))
	t.Cleanup(func() { testenv.Exec(t, gateway(t), "DROP ROLE "+role) })
	owner := newHarness(t, clusterConfig(config.Restricted))
	cfg := clusterConfig(config.Restricted)
	cfg.DatabaseURL = gatewayURLAs(role, password, 15432)
	unprivileged := newHarness(t, cfg)

	calls := []struct {
		name string
		args any
		want []string
	}{
		{"list_schemas", noInput{}, []string{`["` + schema + `","postgres","User Schema"]`}},
		{"list_objects", objectsInput{SchemaName: schema}, []string{`["` + schema + `","orders","BASE TABLE"]`}},
		{"list_objects", objectsInput{SchemaName: schema, ObjectType: "view"}, []string{`["` + schema + `","recent","VIEW"]`}},
		{"list_objects", objectsInput{SchemaName: schema, ObjectType: "sequence"}, []string{`["` + schema + `","ticket","bigint"]`}},
		{"get_object_details", objectInput{SchemaName: schema, ObjectName: "orders"}, []string{
			`["code","character varying(20)","YES",null]`,
			`["orders_pkey","PRIMARY KEY","id"]`,
			`["orders_parent_id_fkey","FOREIGN KEY","parent_id"]`,
			`["orders_code_key","UNIQUE","code"]`,
			`["positive","CHECK","id"]`,
		}},
		{"get_object_details", objectInput{SchemaName: schema, ObjectName: "ticket", ObjectType: "sequence"}, []string{`["` + schema + `","ticket","bigint","5","1"]`}},
	}
	for _, c := range calls {
		seen, shown := resultBody(owner.mustCall(c.name, c.args)), resultBody(unprivileged.mustCall(c.name, c.args))
		if shown != seen {
			t.Fatalf("%s %+v shows a role without privileges something else.\nowner:\n%s\nrole without privileges:\n%s", c.name, c.args, seen, shown)
		}
		mustContain(t, seen, c.want...)
	}
}

func resultBody(text string) string {
	if end := strings.Index(text, "answered_by: "); end >= 0 {
		return text[:end]
	}
	return text
}

func TestExecuteSQLClearsHypopgStateFromViews(t *testing.T) {
	schema := ordersSchema(t, "toolhypoview")
	testenv.Exec(t, gateway(t), fmt.Sprintf(`CREATE FUNCTION %[1]s.mk() RETURNS oid LANGUAGE sql AS $$ SELECT indexrelid FROM public.hypopg_create_index('CREATE INDEX ON %[1]s.orders (customer_id)') $$;
CREATE FUNCTION %[1]s.hide() RETURNS bool LANGUAGE sql AS $$ SELECT public.hypopg_hide_index('%[1]s.orders_pkey'::regclass) $$;
CREATE VIEW %[1]s.mk_view AS SELECT %[1]s.mk();
CREATE VIEW %[1]s.hide_view AS SELECT %[1]s.hide()`, schema))
	h := newHarness(t, clusterConfig(config.Restricted))
	query := fmt.Sprintf("SELECT * FROM %s.orders WHERE customer_id = 5", schema)

	text := h.mustCall("execute_sql", sqlInput{SQL: fmt.Sprintf("SELECT * FROM %s.mk_view", schema)})
	mustContain(t, text, "note: hypopg cleanup: removed 1 hypothetical indexes and restored 0 hidden indexes")
	if seen := countClients(t, planUsesHypothetical(query)); seen != (visibility{}) {
		resetClients(t)
		t.Fatalf("hypothetical index from a view reached other clients: %+v of %d", seen, clientsPerMode)
	}

	text = h.mustCall("execute_sql", sqlInput{SQL: fmt.Sprintf("SELECT * FROM %s.hide_view", schema)})
	mustContain(t, text, "note: hypopg cleanup: removed 0 hypothetical indexes and restored 1 hidden indexes")
	if seen := countClients(t, hidesIndexes); seen != (visibility{}) {
		resetClients(t)
		t.Fatalf("index hidden by a view stayed hidden for other clients: %+v of %d", seen, clientsPerMode)
	}
}

func TestExplainQueryDoesNotWaitOnLocks(t *testing.T) {
	schema := ordersSchema(t, "lockwait")
	h := newHarness(t, clusterConfig(config.Restricted))
	release := lockTable(t, schema+".orders")
	defer release()
	start := time.Now()
	text, isError := h.call("explain_query", explainInput{SQL: fmt.Sprintf("SELECT * FROM %s.orders WHERE customer_id = 5", schema)})
	elapsed := time.Since(start)
	if !isError || elapsed > 5*time.Second {
		t.Fatalf("explain_query waited %s on a locked table:\n%s", elapsed, text)
	}
	mustContain(t, text, "SQLSTATE 55P03")
}

func TestIndexAdvisorSkipsColumnsWithoutBtree(t *testing.T) {
	schema := ordersSchema(t, "nobtree")
	setup := gateway(t)
	testenv.Exec(t, setup, fmt.Sprintf(`ALTER TABLE %[1]s.orders ADD COLUMN tags json;
UPDATE %[1]s.orders SET tags = '{"k": 1}' WHERE id %% 7 = 0;
ANALYZE %[1]s.orders`, schema))
	h := newHarness(t, clusterConfig(config.Restricted))
	text := h.mustCall("analyze_query_indexes", queryIndexesInput{Queries: []string{
		fmt.Sprintf("SELECT * FROM %s.orders WHERE customer_id = 5 AND tags::text = 'x'", schema),
	}})
	mustContain(t, text, `USING btree (\"customer_id\")`, "tags", "no default operator class", cleanEnd)
}
