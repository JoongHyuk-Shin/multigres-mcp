//go:build cluster

package tools

import (
	"context"
	"encoding/json"
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
	mustContain(t, text, "was not sent", "public.abs", "write abs with the pg_catalog. prefix")
	mustContain(t, h.mustCall("execute_sql", sqlInput{SQL: "SELECT pg_catalog.abs(-3)"}), `["3"]`)

	trimmed := explainInput{SQL: "SELECT TRIM(BOTH 'x' FROM CAST($1 AS text))"}
	mustContain(t, h.mustCall("explain_query", trimmed), "generic plan")
	testenv.Exec(t, setup, "CREATE FUNCTION public.btrim(text, text) RETURNS text LANGUAGE sql AS $$ SELECT 'shadow' $$")
	t.Cleanup(func() { testenv.Exec(t, setup, "DROP FUNCTION public.btrim(text, text)") })
	text, isError = h.call("explain_query", trimmed)
	if !isError {
		t.Fatalf("a name that only the rewritten generic plan text has was not checked:\n%s", text)
	}
	mustContain(t, text, "was not sent", "public.btrim", "without calling btrim")
	text, isError = h.call("execute_sql", sqlInput{SQL: "SELECT abs('x'), TRIM(BOTH 'x' FROM 'y')"})
	if !isError {
		t.Fatalf("a statement calling two shadowed names was sent:\n%s", text)
	}
	mustContain(t, text, "public.abs, public.btrim", "write abs with the pg_catalog. prefix", "without calling btrim")
}

func TestGatewayRewriteMustNotReachShadowingFunctions(t *testing.T) {
	schema := ordersSchema(t, "rewrite")
	setup := gateway(t)
	h := newHarness(t, clusterConfig(config.Restricted))
	rest := fmt.Sprintf(" AS t, current_setting('statement_timeout') AS s FROM %s.orders WHERE customer_id = 5", schema)
	queries := []string{"SELECT trim(CAST(status AS varchar))" + rest, "SELECT pg_catalog.btrim(CAST(status AS varchar))" + rest}
	tools := []struct {
		name, ran string
		call      func(sql string) (string, bool)
	}{
		{"execute_sql", `["done","`, func(sql string) (string, bool) { return h.call("execute_sql", sqlInput{SQL: sql}) }},
		{"explain_query", "actual time", func(sql string) (string, bool) {
			return h.call("explain_query", explainInput{SQL: sql, Analyze: true})
		}},
		{"analyze_query_indexes", `(\"customer_id\")`, func(sql string) (string, bool) {
			return h.call("analyze_query_indexes", queryIndexesInput{Queries: []string{sql}})
		}},
	}
	for _, tool := range tools {
		for _, sql := range queries {
			text, isError := tool.call(sql)
			if isError {
				t.Fatalf("%s failed on %q before any shadowing function exists:\n%s", tool.name, sql, text)
			}
			mustContain(t, text, tool.ran)
		}
	}
	testenv.Exec(t, setup, "CREATE FUNCTION public.btrim(varchar) RETURNS text LANGUAGE sql AS $$ SELECT 'shadow' $$")
	t.Cleanup(func() { testenv.Exec(t, setup, "DROP FUNCTION public.btrim(varchar)") })
	for _, tool := range tools {
		for _, sql := range queries {
			text, _ := tool.call(sql)
			if strings.Contains(text, `shadow"`) || strings.Contains(text, tool.ran) {
				t.Errorf("%s sent %q although the gateway's rewrite of it calls public.btrim:\n%.600s", tool.name, sql, text)
				continue
			}
			mustContain(t, text, "was not sent", "public.btrim", "without calling btrim")
		}
	}
}

func TestPlainOverlayAndSubstringCallsMustNotReachShadowingFunctions(t *testing.T) {
	schema := ordersSchema(t, "plaincall")
	setup := gateway(t)
	h := newHarness(t, clusterConfig(config.Restricted))
	from := fmt.Sprintf(" AS t FROM %s.orders WHERE customer_id = 5", schema)
	queries := map[string]string{
		"overlay":   "SELECT overlay(CAST(status AS varchar), CAST('X' AS varchar), 1)" + from,
		"substring": "SELECT substring(CAST(status AS varchar), 1, 2)" + from,
	}
	for _, sql := range queries {
		if text, isError := h.call("execute_sql", sqlInput{SQL: sql}); isError {
			t.Fatalf("execute_sql failed on %q before any shadowing function exists:\n%s", sql, text)
		}
	}
	testenv.Exec(t, setup, `CREATE FUNCTION public.overlay(varchar, varchar, integer) RETURNS text LANGUAGE sql AS $$ SELECT 'shadow' $$;
CREATE FUNCTION public.substring(varchar, integer, integer) RETURNS text LANGUAGE sql AS $$ SELECT 'shadow' $$`)
	t.Cleanup(func() {
		testenv.Exec(t, setup, "DROP FUNCTION public.overlay(varchar, varchar, integer); DROP FUNCTION public.substring(varchar, integer, integer)")
	})
	for name, sql := range queries {
		text, _ := h.call("execute_sql", sqlInput{SQL: sql})
		if strings.Contains(text, `shadow"`) || !strings.Contains(text, "was not sent") {
			t.Errorf("execute_sql did not refuse %q although PostgreSQL resolves it to public.%s:\n%.600s", sql, name, text)
			continue
		}
		mustContain(t, text, "public."+name, "without calling "+name)
	}
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
CREATE TABLE %[1]s.orders (id bigint PRIMARY KEY, parent_id int REFERENCES %[1]s.parent (id), code varchar(20) UNIQUE, area box, CONSTRAINT positive CHECK (id > 0), EXCLUDE USING gist (area WITH &&));
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
			`["orders_area_excl","EXCLUDE","area"]`,
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

func TestExtensionsAreListedForTheDatabaseWithTheirSchema(t *testing.T) {
	rows, err := gateway(t).Query(context.Background(), `SELECT e.extname, e.extversion, CASE WHEN e.extrelocatable THEN 't' ELSE 'f' END, n.nspname
FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
ORDER BY e.extname`)
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	plpgsql := ""
	for rows.Next() {
		var name, version, relocatable, schema string
		if err := rows.Scan(&name, &version, &relocatable, &schema); err != nil {
			t.Fatal(err)
		}
		row, err := json.Marshal([]string{name, version, relocatable, schema})
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, string(row))
		if name == "plpgsql" {
			plpgsql = string(row)
		}
	}
	if err := rows.Err(); err != nil || plpgsql == "" {
		t.Fatalf("reading pg_extension: %v, plpgsql row %q", err, plpgsql)
	}
	h := newHarness(t, clusterConfig(config.Restricted))

	listed := h.mustCall("list_objects", objectsInput{SchemaName: "no_such_schema", ObjectType: "extension"})
	mustContain(t, listed, append(want, `"extschema"`, fmt.Sprintf("rows: %d", len(want)))...)

	details := h.mustCall("get_object_details", objectInput{SchemaName: "no_such_schema", ObjectName: "plpgsql", ObjectType: "extension"})
	if first, _, _ := strings.Cut(details, "\n"); first != "object: plpgsql (extension)" {
		t.Fatalf("the first line names a schema the extension is not looked up in: %q\n%s", first, details)
	}
	mustContain(t, details, `"extschema"`, plpgsql)
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

func TestIndexAdvisorReadsDatabaseQualifiedNames(t *testing.T) {
	schema := ordersSchema(t, "dbname")
	setup := gateway(t)
	var database string
	if err := setup.QueryRow(context.Background(), "SELECT current_database()").Scan(&database); err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, clusterConfig(config.Restricted))
	text := h.mustCall("analyze_query_indexes", queryIndexesInput{Queries: []string{
		fmt.Sprintf("SELECT * FROM %s.%s.orders WHERE customer_id = 5", database, schema),
	}})
	mustContain(t, text, "skipped: 0", fmt.Sprintf(`CREATE INDEX ON \"%s\".\"orders\" USING btree (\"customer_id\")`, schema))
}

func TestIndexAdvisorsSayWhyNothingIsRecommended(t *testing.T) {
	schema := testenv.Schema(t, "norec")
	testenv.Exec(t, gateway(t), fmt.Sprintf(`CREATE TABLE %[1]s.wide (id bigint PRIMARY KEY, a int, pad text, tags json);
INSERT INTO %[1]s.wide SELECT g, g %% 20000, repeat('x', 60), '{}' FROM generate_series(1, 100000) g;
CREATE TABLE %[1]s.narrow (a int, b int);
INSERT INTO %[1]s.narrow SELECT g, g %% 50 FROM generate_series(1, 20000) g;
ANALYZE %[1]s.wide; ANALYZE %[1]s.narrow`, schema))
	searched := func(budget, overBudget, smallGain, tooLittle int) string {
		return fmt.Sprintf("no recommendation: none of the 1 candidate indexes was added (postgres-mcp rules); would take the indexes over max_index_size_mb (%d MB), so not costed: %d; lowers the workload cost by less than 10%%: %d; lowers it by 10%% or more but too little for its size (log(cost) + 2 x log(size) did not fall, size being the total size of the tables with candidates, their indexes included): %d", budget, overBudget, smallGain, tooLittle)
	}
	reasonLine := func(text string) string {
		reason := ""
		for _, line := range strings.Split(text, "\n") {
			if strings.HasPrefix(line, "no recommendation:") {
				reason = line
			}
		}
		return reason
	}
	onlyNotesAfter := func(name, text, analysis string) {
		t.Helper()
		at := strings.Index(text, "queries analyzed: ")
		if at < 0 || !strings.HasPrefix(text[at:], analysis) {
			t.Fatalf("%s: want the analysis part\n%s\n%.4000s", name, analysis, text)
		}
		for _, line := range strings.Split(strings.TrimSuffix(text[at+len(analysis):], "\n"), "\n") {
			if line != "" && !strings.HasPrefix(line, "note: ") {
				t.Fatalf("%s: no query was analyzed, but the result goes on after the reason with %q:\n%.4000s", name, line, text)
			}
		}
	}
	h := newHarness(t, clusterConfig(config.Restricted))
	testenv.Exec(t, primary(t), "SELECT pg_stat_statements_reset()")
	workload := h.mustCall("analyze_workload_indexes", workloadIndexesInput{})
	if reason := reasonLine(workload); !strings.Contains(workload, "workload: 0 queries") || reason != "no recommendation: no query was analyzed" {
		t.Fatalf("an empty workload gives the line %q:\n%.3000s", reason, workload)
	}
	onlyNotesAfter("empty workload", workload, "queries analyzed: 0, skipped: 0, candidate indexes: 0\nno recommendation: no query was analyzed\n")
	results := map[string]string{}
	for _, c := range []struct {
		name     string
		query    string
		budgetMB int
		want     string
	}{
		{"nothing analyzed", "SELECT 1", 0, "no recommendation: no query was analyzed"},
		{"no column to index", "SELECT count(*) FROM %s.wide", 0, "no recommendation: no candidate index, because the analyzed queries filter, join, sort or group on no column of an ordinary user table"},
		{"every possible index left out", "SELECT * FROM %s.wide WHERE id = 5 AND pad = 'x' AND tags::text = 'x'", 0, "no recommendation: no candidate index; the columns the analyzed queries filter, join, sort or group on give 7 possible indexes of 1 to 3 columns; already a btree index: 1; include a text column (text, or varchar or char longer than 100 or without a length) with no statistics or wider than 40 bytes on average: 4; cannot be built with btree (listed above): 2"},
		{"over the size budget", "SELECT * FROM %s.wide WHERE a = 42", 1, searched(1, 1, 0, 0)},
		{"small gain", "SELECT * FROM %s.narrow WHERE b < 45", 0, searched(10000, 0, 1, 0)},
		{"too little for its size", "SELECT * FROM %s.narrow WHERE b < 8", 0, searched(10000, 0, 0, 1)},
		{"zero cost", "SELECT * FROM %s.wide WHERE a = 42 AND false", 0, "no recommendation: the analyzed queries' plan cost is 0, so no index can lower it"},
		{"recommended within the default budget", "SELECT * FROM %s.wide WHERE a = 42", 0, ""},
	} {
		query := c.query
		if strings.Contains(query, "%s") {
			query = fmt.Sprintf(query, schema)
		}
		text := h.mustCall("analyze_query_indexes", queryIndexesInput{Queries: []string{query}, MaxIndexSizeMB: c.budgetMB})
		if reason := reasonLine(text); reason != c.want {
			t.Fatalf("%s: the line on why nothing is recommended is\n%q\nwant\n%q\n%.4000s", c.name, reason, c.want, text)
		}
		if c.want == "" {
			mustContain(t, text, "recommended indexes, in the order the search chose them")
		}
		results[c.name] = text
	}
	onlyNotesAfter("SELECT 1", results["nothing analyzed"], "queries analyzed: 0, skipped: 1, candidate indexes: 0\nskipped queries:\n[\n  {\n    \"query\": \"SELECT 1\",\n    \"query_length\": 8,\n    \"reason\": \"reads no ordinary user table\"\n  }\n]\nno recommendation: no query was analyzed\n")
	zero := results["zero cost"]
	mustContain(t, zero, "workload cost (average over queries of plan total cost x weight): 0.00 before, 0.00 after\n")
	if row := listedRow(t, zero, "plan total cost per query, before and after all recommended indexes", "SELECT * FROM "+schema+".wide WHERE a = 42 AND false"); fmt.Sprintf("%v %v", row["cost_before"], row["cost_after"]) != "0 0" {
		t.Fatalf("a query whose plan cost is 0 is listed with costs %v and %v, want 0 and 0", row["cost_before"], row["cost_after"])
	}
}

func TestIndexAdvisorsShowTheStartOfLongQueries(t *testing.T) {
	schema := ordersSchema(t, "longadvice")
	setup := gateway(t)
	testenv.Exec(t, setup, fmt.Sprintf(`CREATE TABLE %[1]s.sink (v int);
CREATE FUNCTION %[1]s.slow() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.006); RETURN NULL; END $$;
CREATE TRIGGER slow AFTER INSERT ON %[1]s.sink FOR EACH STATEMENT EXECUTE FUNCTION %[1]s.slow()`, schema))
	values := make([]string, 8000)
	for i := range values {
		values[i] = fmt.Sprintf("(%d)", i)
	}
	insert := fmt.Sprintf("INSERT INTO %s.sink VALUES %s", schema, strings.Join(values, ", "))
	join := fmt.Sprintf("SELECT count(*) /* %s */ FROM %[2]s.orders a JOIN %[2]s.orders b ON a.customer_id = b.customer_id WHERE a.status = 'open'",
		strings.Repeat("긴 주석으로 문장을 길게 만든다 ", 3000), schema)
	node := primary(t)
	testenv.Exec(t, node, "SELECT pg_stat_statements_reset()")
	for i := 0; i < 55; i++ {
		testenv.Exec(t, node, insert)
		testenv.Count(t, node, join)
	}
	stored := func(start string) (string, int64) {
		t.Helper()
		var text string
		var queryid int64
		if err := setup.QueryRow(context.Background(), "SELECT query, queryid FROM pg_stat_statements WHERE starts_with(query, $1) LIMIT 1", start).Scan(&text, &queryid); err != nil {
			t.Fatalf("pg_stat_statements has no statement starting with %q: %v", start, err)
		}
		return text, queryid
	}
	insertText, insertID := stored("INSERT INTO " + schema + ".sink")
	joinText, joinID := stored("SELECT count(*) /* 긴 주석")

	badLiteral := fmt.Sprintf("SELECT * FROM %s.orders WHERE id = '%s%s'", schema, strings.Repeat("값", 100), strings.Repeat("a", 30000))
	elements := make([]string, 12000)
	for i := range elements {
		elements[i] = strconv.Itoa(i)
	}
	threePartName := fmt.Sprintf("SELECT postgres.pg_catalog.array_length(ARRAY[%s], 1) FROM %s.orders", strings.Join(elements, ", "), schema)

	h := newHarness(t, clusterConfig(config.Restricted))
	workload := h.mustCall("analyze_workload_indexes", workloadIndexesInput{})
	given := h.mustCall("analyze_query_indexes", queryIndexesInput{Queries: []string{join, badLiteral, threePartName}})
	for _, result := range []struct{ name, text string }{{"analyze_workload_indexes", workload}, {"analyze_query_indexes", given}} {
		if strings.Contains(result.text, "[result truncated") {
			t.Fatalf("one long query filled the %s result:\n%.3000s", result.name, result.text)
		}
		mustContain(t, result.text, fmt.Sprintf("query is the first %d characters of each ", advisorPreview), "workload cost (average over queries", "costs are planner estimates")
	}
	shows := func(name string, row map[string]any, text string, queryid int64) {
		t.Helper()
		runes := []rune(text)
		if query := row["query"]; query != string(runes[:advisorPreview]) {
			t.Fatalf("%s shows %q, want the first %d characters of the query", name, query, advisorPreview)
		}
		if length := fmt.Sprint(row["query_length"]); length != strconv.Itoa(len(runes)) {
			t.Fatalf("%s gives query_length %s for a query of %d characters", name, length, len(runes))
		}
		if want := fmt.Sprint(queryid); queryid == 0 && row["queryid"] != nil || queryid != 0 && fmt.Sprint(row["queryid"]) != want {
			t.Fatalf("%s gives queryid %v, want %d (0: none)", name, row["queryid"], queryid)
		}
	}
	shows("skipped workload query", listedRow(t, workload, "skipped queries", insertText[:50]), insertText, insertID)
	costs := "plan total cost per query, before and after all recommended indexes"
	shows("workload query cost", listedRow(t, workload, costs, joinText[:20]), joinText, joinID)
	shows("given query cost", listedRow(t, given, costs, join[:20]), join, 0)
	failed := listedRow(t, given, "skipped queries", badLiteral[:60])
	shows("skipped given query", failed, badLiteral, 0)
	reason := failed["reason"].(string)
	if runes := []rune(reason); !strings.HasPrefix(reason, "EXPLAIN failed: invalid input syntax") || len(runes) != reasonPreview+len([]rune(reasonCutMark)) || string(runes[reasonPreview:]) != reasonCutMark || strings.ContainsRune(reason, '\uFFFD') {
		t.Fatalf("a Postgres error that repeats a long value is shown as %d characters, want its first %d characters and %q:\n%.700s", len(runes), reasonPreview, reasonCutMark, reason)
	}
	rejected := listedRow(t, given, "skipped queries", threePartName[:40])
	if reason := rejected["reason"]; reason != "did not pass the read check: function name postgres.pg_catalog.array_length is not allowed" {
		t.Fatalf("the read check names a rejected function as %.300q", reason)
	}

	guide := regexp.MustCompile(`SELECT substr\(query, 1, (\d+)\) FROM \S+ WHERE queryid = <queryid> LIMIT 1`).FindStringSubmatch(workload)
	if guide == nil {
		t.Fatalf("no instruction for reading the full text:\n%.3000s", workload)
	}
	if strings.Index(workload, guide[0]) > strings.Index(workload, "skipped queries:") {
		t.Fatalf("the instruction comes after the query lists, so it is lost when they fill the result:\n%.3000s", workload)
	}
	piece, _ := strconv.Atoi(guide[1])
	first := h.mustCall("execute_sql", sqlInput{SQL: strings.Replace(guide[0], "<queryid>", fmt.Sprint(insertID), 1)})
	if strings.Contains(first, "[result truncated") || !strings.Contains(first, insertText[:min(piece, len(insertText))]) {
		t.Fatalf("the instruction's first piece is not the first %d characters of the skipped query:\n%.2000s", piece, first)
	}
	if strings.Contains(given, "queryid = <queryid>") {
		t.Fatalf("analyze_query_indexes points at pg_stat_statements for queries it was given:\n%.3000s", given)
	}
}

func listedRow(t *testing.T, text, label, start string) map[string]any {
	t.Helper()
	at := strings.Index(text, label+":\n[")
	if at < 0 {
		t.Fatalf("no %q list:\n%.3000s", label, text)
	}
	decoder := json.NewDecoder(strings.NewReader(text[at+len(label)+2:]))
	decoder.UseNumber()
	var rows []map[string]any
	if err := decoder.Decode(&rows); err != nil {
		t.Fatalf("%s: %v\n%.3000s", label, err, text)
	}
	for _, row := range rows {
		if query, _ := row["query"].(string); strings.HasPrefix(query, start) {
			return row
		}
	}
	t.Fatalf("no row of %q starts with %q:\n%.3000s", label, start, text)
	return nil
}
