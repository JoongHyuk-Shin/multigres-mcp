//go:build cluster

package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/config"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/testenv"
)

const (
	aggressiveListLabel = "up to 10 relations: aggressive ones first, then those with the fewest transactions left before their next autovacuum is aggressive:\n"
	wraparoundListLabel = "tables and materialized views under 10,000,000 transactions or multixacts left, both counted as postgres-mcp counts transactions, 2,146,483,648 minus the age, which is about 2,000,000 when Postgres stops assigning new IDs (system catalogs and temporary tables included, a TOAST table counted with its table):\n"
	wraparoundHealthy   = "verdict: healthy (no table or materialized view in this database under the postgres-mcp threshold of 10,000,000 transactions left, which this check applies to multixacts too)"
	wraparoundDanger    = "verdict: danger (postgres-mcp threshold: fewer than 10,000,000 transactions left, which this check applies to multixacts too)"
)

var (
	aggressiveCount = regexp.MustCompile(`relations whose next autovacuum is aggressive: (\d+) \(tables, materialized views and their TOAST tables; system catalogs, information_schema tables and temporary tables left out\)`)
	databaseAges    = regexp.MustCompile(`this database: age\(datfrozenxid\) (\d+), mxid_age\(datminmxid\) (\d+) \(`)
)

type aggressiveRow struct {
	Schema           string  `json:"schema"`
	Table            string  `json:"table"`
	Toast            *string `json:"toast"`
	XIDAge           int64   `json:"xid_age"`
	XIDAggressiveAt  int64   `json:"xid_aggressive_at"`
	UntilAggressive  int64   `json:"transactions_until_aggressive"`
	MXIDAge          int64   `json:"mxid_age"`
	MXIDAggressiveAt int64   `json:"mxid_aggressive_at"`
	SizeBytes        int64   `json:"size_bytes"`
	Aggressive       bool    `json:"next_autovacuum_aggressive"`
}

type expectedAggressive struct {
	xidAt  int64
	mxidAt int64
}

type wraparoundRow struct {
	Schema           string `json:"schema"`
	Table            string `json:"table"`
	TransactionsLeft int64  `json:"transactions_left"`
	MultixactsLeft   int64  `json:"multixacts_left"`
}

type agedRelation struct {
	schema    string
	table     string
	relation  string
	toast     bool
	multixact bool
	left      int64
}

func TestVacuumShowsRelationsWhoseNextAutovacuumIsAggressive(t *testing.T) {
	setup := gateway(t)
	requireDefaultFreezeSettings(t, setup)
	schema := testenv.Schema(t, "aggr")
	testenv.Exec(t, setup, fmt.Sprintf(`CREATE TABLE %[1]s.a_main0 (id int, body text) WITH (autovacuum_freeze_table_age = 0, autovacuum_multixact_freeze_table_age = 2000000000);
CREATE TABLE %[1]s.b_toast0 (id int, body text) WITH (toast.autovacuum_freeze_table_age = 0);
CREATE TABLE %[1]s.c_mixed (id int, body text) WITH (autovacuum_freeze_table_age = 0, toast.autovacuum_vacuum_threshold = 0);
CREATE TABLE %[1]s.d_mxid0 (id int) WITH (autovacuum_multixact_freeze_table_age = 0, autovacuum_freeze_table_age = 2000000000);
CREATE MATERIALIZED VIEW %[1]s.m_view WITH (autovacuum_freeze_table_age = 0) AS SELECT 1 AS id;
CREATE TABLE %[1]s.n_near (id int) WITH (autovacuum_freeze_table_age = 100000000);
CREATE TABLE %[1]s.p_parent (id int) PARTITION BY RANGE (id);
CREATE UNLOGGED TABLE %[1]s.u_unlogged (id int) WITH (autovacuum_freeze_table_age = 0);
INSERT INTO %[1]s.a_main0 SELECT 1, string_agg(md5(i::text), '') FROM generate_series(1, 300) i`, schema))
	temporary := primary(t)
	testenv.Exec(t, temporary, "CREATE TEMP TABLE aggr_temporary (id int, body text) WITH (autovacuum_freeze_table_age = 0)")

	h := newHarness(t, clusterConfig(config.Restricted))
	var oldestXID, oldestMXID int64
	if err := setup.QueryRow(context.Background(), "SELECT max(age(relfrozenxid)), max(mxid_age(relminmxid)) FROM pg_class WHERE relkind IN ('r', 'm', 't')").Scan(&oldestXID, &oldestMXID); err != nil {
		t.Fatal(err)
	}
	section := checkSection(t, h.mustCall("diagnose", diagnoseInput{Checks: []string{"vacuum"}}), "vacuum")
	mustContain(t, section, "vacuum_freeze_table_age 150000000", "autovacuum_freeze_max_age 200000000",
		"vacuum_multixact_freeze_table_age 150000000", "autovacuum_multixact_freeze_max_age 400000000",
		"a TOAST table with no storage parameters of its own takes its table's")
	checkDatabaseAges(t, section, oldestXID, oldestMXID)
	if count := aggressiveTotal(t, section); count != 7 {
		t.Fatalf("aggressive relations: %d, want only this test's 7 (another relation in this database may be aggressive):\n%s", count, section)
	}
	rows := aggressiveRows(t, section)
	checkAggressiveOrder(t, rows, section)

	toastOf := func(row aggressiveRow) string {
		var name string
		if err := setup.QueryRow(context.Background(), "SELECT c.reltoastrelid::regclass::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2", row.Schema, row.Table).Scan(&name); err != nil {
			t.Fatal(err)
		}
		return name
	}
	want := map[string]expectedAggressive{
		"a_main0":        {xidAt: 0, mxidAt: 380000000},
		"a_main0 TOAST":  {xidAt: 0, mxidAt: 380000000},
		"b_toast0 TOAST": {xidAt: 0, mxidAt: 150000000},
		"c_mixed":        {xidAt: 0, mxidAt: 150000000},
		"d_mxid0":        {xidAt: 190000000, mxidAt: 0},
		"m_view":         {xidAt: 0, mxidAt: 150000000},
		"u_unlogged":     {xidAt: 0, mxidAt: 150000000},
	}
	got := map[string]aggressiveRow{}
	var closest []string
	for _, row := range rows {
		key := row.Table
		if row.Toast != nil {
			if toast := toastOf(row); *row.Toast != toast {
				t.Fatalf("%s.%s: toast %q is not that table's TOAST table %q", row.Schema, row.Table, *row.Toast, toast)
			}
			key += " TOAST"
		}
		if !row.Aggressive {
			closest = append(closest, row.Schema+"."+key)
			continue
		}
		if row.Schema != schema {
			t.Fatalf("aggressive relation outside this test's schema: %+v\n%s", row, section)
		}
		got[key] = row
	}
	if len(closest) != 3 || closest[0] != schema+".n_near" {
		t.Fatalf("listed after the aggressive relations: %v, want %s.n_near first (fewest transactions left), then two more:\n%s", closest, schema, section)
	}
	if len(got) != len(want) {
		t.Fatalf("aggressive relations %v, want %v:\n%s", keysOf(got), keysOf(want), section)
	}
	for key, expected := range want {
		row, ok := got[key]
		if !ok {
			t.Fatalf("%s is not listed as aggressive:\n%s", key, section)
		}
		if row.XIDAggressiveAt != expected.xidAt || row.MXIDAggressiveAt != expected.mxidAt {
			t.Fatalf("%s: aggressive at xid age %d and mxid age %d, want %d and %d", key, row.XIDAggressiveAt, row.MXIDAggressiveAt, expected.xidAt, expected.mxidAt)
		}
	}
	mainBytes := testenv.Count(t, setup, "SELECT pg_relation_size($1::regclass)", schema+".a_main0")
	toastBytes := testenv.Count(t, setup, "SELECT pg_relation_size(reltoastrelid) FROM pg_class WHERE oid = $1::regclass", schema+".a_main0")
	if toastBytes == 0 || got["a_main0"].SizeBytes != mainBytes || got["a_main0 TOAST"].SizeBytes != toastBytes {
		t.Fatalf("sizes: table %d and TOAST %d, want pg_relation_size %d and %d", got["a_main0"].SizeBytes, got["a_main0 TOAST"].SizeBytes, mainBytes, toastBytes)
	}

	testenv.Exec(t, setup, fmt.Sprintf(`CREATE TABLE %[1]s.f_more (id int, body text) WITH (autovacuum_freeze_table_age = 0);
CREATE TABLE %[1]s.g_more (id int, body text) WITH (autovacuum_freeze_table_age = 0);
CREATE TABLE %[1]s.h_more (id int, body text) WITH (autovacuum_freeze_table_age = 0)`, schema))
	section = checkSection(t, h.mustCall("diagnose", diagnoseInput{Checks: []string{"vacuum"}}), "vacuum")
	if count := aggressiveTotal(t, section); count != 13 {
		t.Fatalf("aggressive relations after adding three tables and their TOAST tables: %d, want 13:\n%s", count, section)
	}
	rows = aggressiveRows(t, section)
	if len(rows) != 10 {
		t.Fatalf("listed %d relations, want 10:\n%s", len(rows), section)
	}
	for _, row := range rows {
		if !row.Aggressive {
			t.Fatalf("a relation that is not aggressive is listed while 13 are:\n%s", section)
		}
	}
}

func TestWraparoundVerdictCountsMaterializedViewsMultixactsAndTOAST(t *testing.T) {
	setup := gateway(t)
	schema := testenv.Schema(t, "wrap")
	h := newHarness(t, clusterConfig(config.Restricted))
	section := checkSection(t, h.mustCall("diagnose", diagnoseInput{Checks: []string{"vacuum"}}), "vacuum")
	mustContain(t, section, wraparoundHealthy)
	if rows := wraparoundRows(t, section); len(rows) != 0 {
		t.Fatalf("relations under the threshold before any was aged: %+v\n%s", rows, section)
	}

	testenv.Exec(t, setup, fmt.Sprintf(`CREATE MATERIALIZED VIEW %[1]s.m_view AS SELECT 1 AS id;
CREATE TABLE %[1]s.p_parent (id int) PARTITION BY RANGE (id);
CREATE VIEW %[1]s.v_view AS SELECT 1 AS id`, schema))
	temporary := primary(t)
	testenv.Exec(t, temporary, "CREATE TEMP TABLE wrap_mxid (id int); CREATE TEMP TABLE wrap_toast_xid (id int, body text); CREATE TEMP TABLE wrap_toast_mxid (id int, body text)")
	var temporarySchema, catalogFrozenXID string
	if err := temporary.QueryRow(context.Background(), "SELECT nspname FROM pg_namespace WHERE oid = pg_my_temp_schema()").Scan(&temporarySchema); err != nil {
		t.Fatal(err)
	}
	if err := temporary.QueryRow(context.Background(), "SELECT relfrozenxid::text FROM pg_class WHERE oid = 'information_schema.sql_features'::regclass").Scan(&catalogFrozenXID); err != nil {
		t.Fatal(err)
	}
	autovacuumBlocker := primary(t)
	testenv.Exec(t, autovacuumBlocker, "BEGIN")
	testenv.Exec(t, autovacuumBlocker, fmt.Sprintf("ANALYZE %s.m_view, information_schema.sql_features", schema))
	t.Cleanup(func() {
		testenv.Exec(t, temporary, "UPDATE pg_class SET relfrozenxid = $1::text::xid WHERE oid = 'information_schema.sql_features'::regclass", catalogFrozenXID)
	})

	matview := agedRelation{schema: schema, table: "m_view", relation: schema + ".m_view", left: 1_000_000}
	tableMultixact := agedRelation{schema: temporarySchema, table: "wrap_mxid", relation: "wrap_mxid", multixact: true, left: 2_000_000}
	toastTransaction := agedRelation{schema: temporarySchema, table: "wrap_toast_xid", relation: "wrap_toast_xid", toast: true, left: 3_000_000}
	toastMultixact := agedRelation{schema: temporarySchema, table: "wrap_toast_mxid", relation: "wrap_toast_mxid", toast: true, multixact: true, left: 4_000_000}
	catalog := agedRelation{schema: "information_schema", table: "sql_features", relation: "information_schema.sql_features", left: 5_000_000}

	for _, aged := range []agedRelation{matview, tableMultixact, toastMultixact} {
		ageRelation(t, temporary, aged)
	}
	section = checkSection(t, h.mustCall("diagnose", diagnoseInput{Checks: []string{"vacuum"}}), "vacuum")
	mustContain(t, section, wraparoundDanger)
	checkWraparoundRows(t, wraparoundRows(t, section), []agedRelation{matview, tableMultixact, toastMultixact}, section)

	for _, aged := range []agedRelation{toastTransaction, catalog} {
		ageRelation(t, temporary, aged)
	}
	section = checkSection(t, h.mustCall("diagnose", diagnoseInput{Checks: []string{"vacuum"}}), "vacuum")
	mustContain(t, section, wraparoundDanger)
	checkWraparoundRows(t, wraparoundRows(t, section), []agedRelation{matview, tableMultixact, toastTransaction, toastMultixact, catalog}, section)
}

func requireDefaultFreezeSettings(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	defaults := map[string]string{
		"vacuum_freeze_table_age":             "150000000",
		"autovacuum_freeze_max_age":           "200000000",
		"vacuum_multixact_freeze_table_age":   "150000000",
		"autovacuum_multixact_freeze_max_age": "400000000",
	}
	for name, value := range defaults {
		var setting string
		if err := conn.QueryRow(context.Background(), "SELECT current_setting($1)", name).Scan(&setting); err != nil {
			t.Fatal(err)
		}
		if setting != value {
			t.Fatalf("%s is %s; this test's expected limits assume the default %s", name, setting, value)
		}
	}
}

func aggressiveTotal(t *testing.T, section string) int {
	t.Helper()
	match := aggressiveCount.FindStringSubmatch(section)
	if match == nil {
		t.Fatalf("no count of aggressive relations:\n%s", section)
	}
	count, _ := strconv.Atoi(match[1])
	return count
}

func aggressiveRows(t *testing.T, section string) []aggressiveRow {
	t.Helper()
	start := strings.Index(section, aggressiveListLabel)
	if start < 0 {
		t.Fatalf("no list of relations closest to an aggressive autovacuum:\n%s", section)
	}
	decoder := json.NewDecoder(strings.NewReader(section[start+len(aggressiveListLabel):]))
	decoder.DisallowUnknownFields()
	var rows []aggressiveRow
	if err := decoder.Decode(&rows); err != nil {
		t.Fatalf("list of relations: %v\n%s", err, section)
	}
	for _, row := range rows {
		if row.Schema == "pg_catalog" || row.Schema == "information_schema" || strings.HasPrefix(row.Schema, "pg_temp") {
			t.Fatalf("listed a system catalog or a temporary table: %+v", row)
		}
		if row.UntilAggressive != row.XIDAggressiveAt-row.XIDAge {
			t.Fatalf("transactions_until_aggressive is not xid_aggressive_at - xid_age: %+v", row)
		}
		if row.Aggressive != (row.XIDAge >= row.XIDAggressiveAt || row.MXIDAge >= row.MXIDAggressiveAt) {
			t.Fatalf("next_autovacuum_aggressive does not follow the two ages: %+v", row)
		}
	}
	return rows
}

func checkAggressiveOrder(t *testing.T, rows []aggressiveRow, section string) {
	t.Helper()
	for i := 1; i < len(rows); i++ {
		previous, row := rows[i-1], rows[i]
		if row.Aggressive && !previous.Aggressive {
			t.Fatalf("an aggressive relation comes after one that is not:\n%s", section)
		}
		if row.Aggressive == previous.Aggressive && row.UntilAggressive < previous.UntilAggressive {
			t.Fatalf("not ordered by transactions left:\n%s", section)
		}
	}
}

func checkDatabaseAges(t *testing.T, section string, oldestXID, oldestMXID int64) {
	t.Helper()
	match := databaseAges.FindStringSubmatch(section)
	if match == nil {
		t.Fatalf("no database ages:\n%s", section)
	}
	xidAge, _ := strconv.ParseInt(match[1], 10, 64)
	mxidAge, _ := strconv.ParseInt(match[2], 10, 64)
	if xidAge < oldestXID || mxidAge < oldestMXID {
		t.Fatalf("database ages %d and %d are younger than the oldest relation before the call, system catalogs included (%d and %d):\n%s", xidAge, mxidAge, oldestXID, oldestMXID, section)
	}
}

func wraparoundRows(t *testing.T, section string) []wraparoundRow {
	t.Helper()
	start := strings.Index(section, wraparoundListLabel)
	if start < 0 {
		t.Fatalf("no list of relations close to wraparound:\n%s", section)
	}
	decoder := json.NewDecoder(strings.NewReader(section[start+len(wraparoundListLabel):]))
	decoder.DisallowUnknownFields()
	var rows []wraparoundRow
	if err := decoder.Decode(&rows); err != nil {
		t.Fatalf("list of relations close to wraparound: %v\n%s", err, section)
	}
	return rows
}

func ageRelation(t *testing.T, conn *pgx.Conn, aged agedRelation) {
	t.Helper()
	column, ageFunction := "relfrozenxid", "age"
	if aged.multixact {
		column, ageFunction = "relminmxid", "mxid_age"
	}
	target := "$1::regclass"
	if aged.toast {
		target = "(SELECT reltoastrelid FROM pg_class WHERE oid = $1::regclass)"
	}
	update := fmt.Sprintf("UPDATE pg_class SET %[1]s = (((%[1]s::text::bigint + %[2]s(%[1]s) - $2) %% 4294967296 + 4294967296) %% 4294967296)::text::xid WHERE oid = %[3]s", column, ageFunction, target)
	testenv.Exec(t, conn, update, aged.relation, 2146483648-aged.left)
}

func checkWraparoundRows(t *testing.T, rows []wraparoundRow, want []agedRelation, section string) {
	t.Helper()
	if len(rows) != len(want) {
		t.Fatalf("listed %d relations, want %d (%+v):\n%s", len(rows), len(want), want, section)
	}
	for i, row := range rows {
		expected := want[i]
		if row.Schema != expected.schema || row.Table != expected.table {
			t.Fatalf("row %d is %s.%s, want %s.%s (fewest transactions or multixacts left first):\n%s", i, row.Schema, row.Table, expected.schema, expected.table, section)
		}
		aged, other := row.TransactionsLeft, row.MultixactsLeft
		if expected.multixact {
			aged, other = other, aged
		}
		if aged > expected.left || aged < expected.left-1000 || other < 10_000_000 {
			t.Fatalf("%s.%s: transactions_left %d and multixacts_left %d, want %d on the aged counter and the other far from the threshold:\n%s", row.Schema, row.Table, row.TransactionsLeft, row.MultixactsLeft, expected.left, section)
		}
	}
}

func keysOf[V any](m map[string]V) []string {
	var out []string
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}
