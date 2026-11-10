package advisor

import (
	"reflect"
	"testing"

	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
)

func collect(t *testing.T, sql string) *collector {
	t.Helper()
	statements, err := parser.ParseSQL(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	return collectColumns(statements[0].(*ast.SelectStmt))
}

func usesOf(c *collector) []string {
	var found []string
	for _, use := range c.uses {
		found = append(found, describeUse(use))
	}
	return found
}

func describeUse(use columnUse) string {
	text := use.column + "@"
	for i, level := range use.levels {
		if i > 0 {
			text += "|"
		}
		for j, rel := range level {
			if j > 0 {
				text += ","
			}
			if rel.schema != "" {
				text += rel.schema + "."
			}
			text += rel.name
		}
	}
	return text
}

func TestCollectColumnsFindsEveryConditionClause(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		want []string
	}{
		{"where with alias", "SELECT * FROM orders o WHERE o.customer_id = 1", []string{"customer_id@orders"}},
		{"gateway rewritten join", "SELECT SUM(amount) FROM mcp_orders AS o INNER JOIN mcp_customers AS c ON o.customer_id = c.id WHERE c.region = 'x'", []string{"region@mcp_customers", "customer_id@mcp_orders", "id@mcp_customers"}},
		{"placeholders from query statistics", "SELECT COUNT(*) FROM mcp_orders WHERE customer_id = $1 AND amount > $2", []string{"customer_id@mcp_orders", "amount@mcp_orders"}},
		{"unqualified sees every table of its level", "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id WHERE status = 'x'", []string{"status@orders,customers", "customer_id@orders", "id@customers"}},
		{"nested join keeps the inner ON", "SELECT * FROM a JOIN b ON a.x = b.x JOIN c ON b.y = c.y", []string{"x@a", "x@b", "y@b", "y@c"}},
		{"using", "SELECT * FROM a JOIN b USING (k)", []string{"k@a,b"}},
		{"having", "SELECT region FROM sales GROUP BY region HAVING sum(amount) > 10", []string{"amount@sales", "region@sales"}},
		{"order by output alias", "SELECT lower(name) AS n FROM users ORDER BY n", []string{"name@users"}},
		{"group by", "SELECT region, count(*) FROM sales GROUP BY region", []string{"region@sales"}},
		{"schema qualified", "SELECT * FROM app.users WHERE app.users.email = 'x'", []string{"email@app.users"}},
		{"database qualified", "SELECT * FROM appdb.app.users WHERE email = 'x'", []string{"email@app.users"}},
		{"database qualified using", "SELECT * FROM appdb.app.a JOIN appdb.app.b USING (k)", []string{"k@app.a,app.b"}},
		{"correlated subquery reaches the outer level", "SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM items i WHERE i.order_id = o.id AND sku = 'x')", []string{"order_id@items", "id@orders", "sku@items|orders"}},
		{"cte name is not a table", "WITH recent AS (SELECT * FROM orders WHERE created > now()) SELECT * FROM recent WHERE recent.total > 1", []string{"created@orders"}},
		{"from subquery", "SELECT * FROM (SELECT * FROM orders WHERE status = 'x') s WHERE s.total > 1", []string{"status@orders"}},
		{"union", "SELECT id FROM a WHERE x = 1 UNION SELECT id FROM b WHERE y = 2", []string{"x@a", "y@b"}},
		{"target list is not a condition", "SELECT price FROM items", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := usesOf(collect(t, tc.sql))
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("uses = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestCollectColumnsRecordsReferencedRelations(t *testing.T) {
	c := collect(t, "WITH r AS (SELECT * FROM orders) SELECT * FROM r JOIN app.customers c ON true, LATERAL (SELECT * FROM items) i")
	want := []relation{{name: "orders"}, {schema: "app", name: "customers"}, {name: "items"}}
	if !reflect.DeepEqual(c.relations, want) {
		t.Fatalf("relations = %+v, want %+v", c.relations, want)
	}
}

func TestBindUseTakesTheNearestLevelWithTheColumn(t *testing.T) {
	orders, items := relation{name: "orders"}, relation{name: "items"}
	tables := map[relation]table{orders: {oid: 1, name: "orders", ordinary: true}, items: {oid: 2, name: "items", ordinary: true}}
	columns := map[uint32]map[string]column{1: {"id": {}, "sku": {}}, 2: {"order_id": {}}}
	got := bindUse(columnUse{levels: [][]relation{{items}, {orders}}, column: "sku"}, tables, columns)
	if len(got) != 1 || got[0].oid != 1 {
		t.Fatalf("sku bound to %+v, want orders only", got)
	}
	if got := bindUse(columnUse{levels: [][]relation{{items}, {orders}}, column: "missing"}, tables, columns); len(got) != 0 {
		t.Fatalf("a column no table has bound to %+v", got)
	}
}

func TestCombinationsKeepTheGivenOrder(t *testing.T) {
	got := combinations([]string{"a", "b", "c", "d"}, 3)
	want := [][]string{{"a"}, {"b"}, {"c"}, {"d"}, {"a", "b"}, {"a", "c"}, {"a", "d"}, {"b", "c"}, {"b", "d"}, {"c", "d"}, {"a", "b", "c"}, {"a", "b", "d"}, {"a", "c", "d"}, {"b", "c", "d"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("combinations = %q, want %q", got, want)
	}
}

func TestLongTextFollowsPostgresMCP(t *testing.T) {
	narrow, wide := int32(20), int32(41)
	columns := map[string]column{"id": {}, "code": {longText: true, avgWidth: &narrow}, "body": {longText: true, avgWidth: &wide}, "unseen": {longText: true}}
	cases := []struct {
		combo      []string
		long, drop bool
	}{
		{[]string{"id"}, false, false},
		{[]string{"id", "code"}, true, false},
		{[]string{"code", "body"}, true, true},
		{[]string{"unseen"}, true, true},
	}
	for _, tc := range cases {
		long, drop := longText(columns, tc.combo)
		if long != tc.long || drop != tc.drop {
			t.Errorf("longText(%q) = %t, %t, want %t, %t", tc.combo, long, drop, tc.long, tc.drop)
		}
	}
}

func TestTotalCostReadsTheTopPlan(t *testing.T) {
	cost, err := totalCost(`[{"Plan": {"Node Type": "Seq Scan", "Total Cost": 4235.93, "Plans": [{"Total Cost": 1.5}]}}]`)
	if err != nil || cost != 4235.93 {
		t.Fatalf("totalCost = %v, %v", cost, err)
	}
	if _, err := totalCost(`[{"Plan": {"Node Type": "Result"}}]`); err == nil {
		t.Fatal("a plan without Total Cost was accepted")
	}
}

func TestConfigKeyIgnoresOrder(t *testing.T) {
	if configKey([]int{3, 1, 2}) != configKey([]int{2, 3, 1}) {
		t.Fatal("the same index set got two cache keys")
	}
	if configKey([]int{1, 2}) == configKey([]int{12}) {
		t.Fatal("different index sets share a cache key")
	}
}
