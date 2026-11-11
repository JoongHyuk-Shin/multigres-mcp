package sqlguard

import (
	"strings"
	"testing"

	"github.com/multigres/multigres/go/common/parser"
)

func TestCheckReadAccepts(t *testing.T) {
	accepted := []string{
		"SELECT relname, n_live_tup FROM pg_stat_user_tables ORDER BY n_live_tup DESC LIMIT 5",
		"EXPLAIN SELECT * FROM mcp_orders WHERE customer_id = 1",
		"EXPLAIN (FORMAT JSON, COSTS true) SELECT 1",
		"EXPLAIN (ANALYZE, BUFFERS) SELECT count(*) FROM pg_class",
		"SHOW max_connections",
		"SHOW ALL",
		"SELECT lower('A'), now(), count(*) FROM pg_class",
		"SELECT pg_catalog.lower('A')",
		"SELECT CURRENT_DATE, CURRENT_USER",
		"SELECT TRIM(both ' ' from ' a ')",
		"SELECT CAST('1' AS integer), '2'::int",
		"SELECT ARRAY[1, 2, 3]",
		"SELECT EXTRACT(year FROM now())",
		"SELECT SUBSTRING('abc' FROM 1 FOR 2)",
		"SELECT COALESCE(NULL, 1), NULLIF(1, 2), GREATEST(1, 2)",
		"SELECT * FROM unnest(ARRAY[1, 2])",
		"WITH recent AS (SELECT relname FROM pg_class) SELECT * FROM recent",
		"SELECT row_number() OVER (ORDER BY oid) FROM pg_class",
		"SELECT 1 UNION ALL SELECT 2",
		"SELECT * FROM mcp_orders WHERE customer_id = $1 AND amount > $2",
		"SELECT 1;",
		"SELECT now() AT TIME ZONE 'UTC'",
		"SELECT 'a_b' LIKE 'a#_b' ESCAPE '#'",
		"SELECT 'a_b' NOT LIKE 'a#_b' ESCAPE '#'",
		"SELECT 'abc' SIMILAR TO 'a%'",
		"SELECT (DATE '2026-01-01', DATE '2026-02-01') OVERLAPS (DATE '2026-01-15', DATE '2026-03-01')",
		"SELECT COLLATION FOR ('a')",
		"SELECT 'a' IS NORMALIZED",
	}
	for _, sql := range accepted {
		if _, err := CheckRead(sql); err != nil {
			t.Errorf("rejected %q: %v", sql, err)
		}
	}
}

func TestCheckReadRejects(t *testing.T) {
	rejected := []string{
		"ROLLBACK; DROP TABLE customers;",
		"SELECT 1; SELECT 2",
		"",
		"   ",
		"-- only a comment",
		"UPDATE t SET a = 1",
		"INSERT INTO t VALUES (1)",
		"DELETE FROM t",
		"MERGE INTO t USING s ON t.a = s.a WHEN MATCHED THEN DELETE",
		"WITH gone AS (DELETE FROM t RETURNING *) SELECT * FROM gone",
		"WITH added AS (INSERT INTO t VALUES (1) RETURNING *) SELECT * FROM added",
		"SELECT * INTO copy_t FROM t",
		"SELECT * FROM t FOR UPDATE",
		"SELECT * FROM t FOR SHARE",
		"SELECT pg_terminate_backend(1)",
		"SELECT set_config('a.b', '1', false)",
		"SELECT pg_sleep(1)",
		"SELECT pg_read_file('postgresql.conf')",
		"SELECT hypopg_create_index('CREATE INDEX ON t (a)')",
		"SELECT pg_current_xact_id()",
		"SELECT setseed(0.5)",
		"SELECT ts_stat('SELECT 1')",
		"SELECT ts_rewrite('a'::tsquery, 'SELECT 1')",
		"SELECT random()",
		"SELECT random(1, 10)",
		"SELECT random_normal()",
		"SELECT crypt('a', 'b')",
		"SELECT public.lower('A')",
		"SELECT other.pg_catalog.lower('A')",
		"SELECT pg_catalog.pg_sleep(1)",
		"EXPLAIN ANALYZE DELETE FROM t",
		"EXPLAIN (ANALYZE) UPDATE t SET a = 1",
		"EXPLAIN CREATE TABLE x AS SELECT 1",
		"EXPLAIN EXECUTE p",
		"EXPLAIN DECLARE c CURSOR FOR SELECT 1",
		"EXPLAIN (FOO) SELECT 1",
		"BEGIN",
		"COMMIT",
		"ROLLBACK",
		"SAVEPOINT s",
		"SET multigres.unsafe_connection = on",
		"SET statement_timeout = 0",
		"COPY t TO PROGRAM 'id'",
		"COPY (SELECT 1) TO STDOUT",
		"CREATE EXTENSION hypopg",
		"DROP TABLE t",
		"DO $$ BEGIN END $$",
		"CALL p()",
		"VACUUM t",
		"PREPARE p AS SELECT 1",
		"SELECT * FROM t WHERE a = (SELECT pg_terminate_backend(1))",
		"SELECT * FROM t ORDER BY pg_terminate_backend(1)",
		"SELECT * FROM t, LATERAL (SELECT pg_terminate_backend(1)) x",
		"WITH x AS (SELECT pg_terminate_backend(1)) SELECT * FROM x",
		"SELECT * FROM pg_terminate_backend(1)",
		"SELECT count(*) FILTER (WHERE pg_terminate_backend(1)) FROM t",
		"SELECT 1 UNION SELECT pg_terminate_backend(1)",
		"SELECT * FROM t GROUP BY a HAVING pg_terminate_backend(1)",
		"SELECT ARRAY[pg_terminate_backend(1)]",
		"SELECT CAST(pg_terminate_backend(1) AS text)",
		"SELECT COALESCE(pg_terminate_backend(1), true)",
		"SELECT CASE WHEN true THEN pg_terminate_backend(1) END",
		"SELECT * FROM t WHERE a IN (VALUES (pg_terminate_backend(1)))",
		"SELECT lower(pg_terminate_backend(1)::text)",
	}
	for _, sql := range rejected {
		if _, err := CheckRead(sql); err == nil {
			t.Errorf("accepted %q", sql)
		}
	}
}

func TestRejectedFunctionNameIsNamedWithoutItsArguments(t *testing.T) {
	elements := make([]string, 12000)
	for i := range elements {
		elements[i] = "0"
	}
	_, err := CheckRead("SELECT postgres.pg_catalog.array_length(ARRAY[" + strings.Join(elements, ", ") + "], 1)")
	if err == nil || err.Error() != "function name postgres.pg_catalog.array_length is not allowed" {
		t.Fatalf("got %v", err)
	}
}

func TestCheckedReportsUnqualifiedFunctionsAndPlaceholders(t *testing.T) {
	checked, err := CheckRead("SELECT lower(a), pg_catalog.upper(b), abs($1) FROM t WHERE c = $2")
	if err != nil {
		t.Fatal(err)
	}
	names := strings.Join(checked.UnqualifiedFunctions(), ",")
	if names != "abs,lower" {
		t.Fatalf("unqualified functions %q", names)
	}
	if checked.Placeholders() != 2 {
		t.Fatalf("placeholders %d", checked.Placeholders())
	}
	if !checked.IsSelect() {
		t.Fatal("expected select")
	}
	show, err := CheckRead("SHOW work_mem")
	if err != nil {
		t.Fatal(err)
	}
	if show.IsSelect() || show.Placeholders() != 0 {
		t.Fatalf("show reported select=%v placeholders=%d", show.IsSelect(), show.Placeholders())
	}
}

func TestGenericPlanTextComesFromTree(t *testing.T) {
	checked, err := CheckRead("SELECT /* marker */ 1")
	if err != nil {
		t.Fatal(err)
	}
	text, _, err := checked.GenericPlanText(false)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(text, "marker") {
		t.Fatalf("model text leaked into generic plan text: %q", text)
	}
	if !strings.HasPrefix(text, "EXPLAIN (GENERIC_PLAN) ") {
		t.Fatalf("prefix missing: %q", text)
	}
	statements, err := parser.ParseSQL(text)
	if err != nil || len(statements) != 1 {
		t.Fatalf("generic plan text is not one statement: %q %v", text, err)
	}
}

func TestGenericPlanRoundTripKeepsMeaning(t *testing.T) {
	inputs := []string{
		"SELECT COUNT(*) FROM mcp_orders WHERE customer_id = $1 AND amount > $2",
		"SELECT o.id FROM mcp_orders AS o INNER JOIN mcp_customers AS c ON o.customer_id = c.id WHERE c.name LIKE $1",
		"SELECT name FROM mcp_customers WHERE name LIKE $1 || '%' ORDER BY name LIMIT $2",
		"SELECT customer_id, sum(amount) FROM mcp_orders GROUP BY customer_id HAVING sum(amount) > $1",
		"SELECT * FROM mcp_orders WHERE id IN (SELECT order_id FROM mcp_items WHERE qty > $1)",
		"WITH recent AS (SELECT * FROM mcp_orders WHERE created_at > now() - $1::interval) SELECT count(*) FROM recent",
		"SELECT CURRENT_DATE, TRIM(both ' ' from name), CAST(id AS text), ARRAY[1, 2] FROM mcp_customers WHERE id = $1",
		"SELECT EXTRACT(year FROM created_at) FROM mcp_orders WHERE id = $1",
	}
	for _, sql := range inputs {
		checked, err := CheckRead(sql)
		if err != nil {
			t.Fatalf("%q: %v", sql, err)
		}
		text, _, err := checked.GenericPlanText(true)
		if err != nil {
			t.Fatalf("%q: %v", sql, err)
		}
		inner := strings.TrimPrefix(text, "EXPLAIN (GENERIC_PLAN, FORMAT JSON) ")
		again, err := CheckRead(inner)
		if err != nil {
			t.Fatalf("round trip of %q failed the guard: %q %v", sql, inner, err)
		}
		if again.Placeholders() != checked.Placeholders() {
			t.Fatalf("placeholder count changed for %q: %q", sql, inner)
		}
		if again.tree.SqlString() != checked.tree.SqlString() {
			t.Fatalf("round trip changed meaning of %q:\n%s\n%s", sql, checked.tree.SqlString(), again.tree.SqlString())
		}
	}
}

func TestVerifyOneReadStatementRejectsSmuggledStatements(t *testing.T) {
	if _, err := verifyOneReadStatement("SELECT 1; SELECT 2"); err == nil {
		t.Error("accepted two statements")
	}
	if _, err := verifyOneReadStatement("SELECT 1"); err != nil {
		t.Fatal(err)
	}
}

func TestCheckWrite(t *testing.T) {
	accepted := []string{
		"INSERT INTO t VALUES (1)",
		"UPDATE t SET a = 1",
		"DELETE FROM t WHERE a = 1",
		"CREATE TABLE t2 (a int)",
		"CREATE INDEX ON t (a)",
		"SET LOCAL work_mem = '64MB'",
		"SELECT set_config('work_mem', '64MB', true)",
		"SELECT pg_catalog.set_config('work_mem', '64MB', true)",
		"SELECT pg_terminate_backend(1)",
		"DECLARE c CURSOR FOR SELECT 1",
		"ANALYZE t",
		"SELECT pg_create_logical_replication_slot('s', 'pgoutput')",
		"SELECT pg_create_physical_replication_slot('s', false, false)",
		"SELECT pg_catalog.pg_create_logical_replication_slot('s', 'pgoutput', temporary => false)",
	}
	for _, sql := range accepted {
		checked, err := CheckWrite(sql)
		if err != nil {
			t.Errorf("rejected %q: %v", sql, err)
			continue
		}
		if checked.Text() != sql {
			t.Errorf("text changed: %q", checked.Text())
		}
	}
	rejected := []string{
		"",
		"INSERT INTO t VALUES (1); DROP TABLE t",
		"BEGIN",
		"COMMIT",
		"ROLLBACK",
		"SAVEPOINT s",
		"SET work_mem = '64MB'",
		"SET SESSION work_mem = '64MB'",
		"RESET work_mem",
		"RESET ALL",
		"SET LOCAL multigres.unsafe_connection = on",
		"SET multigres.unsafe_connection = on",
		"SELECT hypopg_create_index('CREATE INDEX ON t (a)')",
		"SELECT public.hypopg_reset()",
		"CREATE TEMP TABLE tt (a int)",
		"CREATE TEMPORARY TABLE tt AS SELECT 1",
		"SELECT 1 INTO TEMP tt",
		"CREATE TEMP VIEW tv AS SELECT 1",
		"CREATE TEMP SEQUENCE ts",
		"PREPARE p AS SELECT 1",
		"LISTEN chan",
		"UNLISTEN *",
		"DECLARE c CURSOR WITH HOLD FOR SELECT 1",
		"LOAD 'auto_explain'",
		"DISCARD ALL",
		"DO $$BEGIN NULL; END$$",
		"SELECT set_config('work_mem', '64MB', false)",
		"SELECT pg_catalog.set_config('work_mem', '64MB', false)",
		"SELECT set_config('work_mem', '64MB', 1 = 1)",
		"SELECT set_config('work_mem', '64MB', $1)",
		"SELECT set_config('work_mem', '64MB')",
		"UPDATE t SET a = length(set_config('work_mem', '64MB', false))",
		"SELECT setseed(0.5)",
		"INSERT INTO t SELECT 1 FROM pg_catalog.setseed(0.5)",
		"SELECT pg_create_physical_replication_slot('s', false, true)",
		"SELECT pg_create_logical_replication_slot('s', 'pgoutput', true)",
		"SELECT pg_catalog.pg_create_logical_replication_slot('s', 'pgoutput', temporary => true)",
		"SELECT pg_create_logical_replication_slot('s', 'pgoutput', temporary => 1 = 1)",
		"SELECT pg_create_physical_replication_slot('s', false, $1)",
		"SELECT postgres.pg_catalog.set_config('work_mem', '64MB', false)",
	}
	for _, sql := range rejected {
		if _, err := CheckWrite(sql); err == nil {
			t.Errorf("accepted %q", sql)
		}
	}
}
