//go:build cluster

package e2e

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/admin"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/testenv"
)

var binary string

var annotations = map[string]string{
	"list_schemas": "r", "list_objects": "r", "get_object_details": "r", "execute_sql": "r", "explain_query": "r",
	"get_top_queries": "r", "analyze_workload_indexes": "r", "analyze_query_indexes": "r", "diagnose": "r",
	"cluster_status": "r", "list_backups": "r", "verify_backups": "r", "get_backup_job_status": "r", "read_node_log": "r",
	"execute_write_sql": "d", "switch_primary": "d", "create_backup": "-", "expire_backups": "d", "set_postgres_restarts": "di",
}

var clusterTools = map[string]bool{
	"cluster_status": true, "list_backups": true, "verify_backups": true, "get_backup_job_status": true, "read_node_log": true,
	"switch_primary": true, "create_backup": true, "expire_backups": true, "set_postgres_restarts": true,
}

var (
	hypothetical = regexp.MustCompile(`<\d+>`)
	jobID        = regexp.MustCompile(`job_id: (\S+)`)
	switched     = regexp.MustCompile(`the primary changed from (\S+) to (\S+)`)
	after        = regexp.MustCompile(`after: primary (\S+), term \d+`)
	workloadCost = regexp.MustCompile(`workload cost \([^)]*\): ([0-9.]+) before, ([0-9.]+) after`)
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "multigres-mcp-e2e")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	binary = filepath.Join(dir, "multigres-mcp")
	build := exec.Command("go", "build", "-o", binary, "../../cmd/multigres-mcp")
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build:", err)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

func TestToolListsFollowTheMode(t *testing.T) {
	cases := []struct {
		name  string
		args  []string
		tools int
	}{
		{"restricted with the admin server", []string{"--admin-url", testenv.AdminURL()}, 14},
		{"unrestricted with the admin server", []string{"--admin-url", testenv.AdminURL(), "--access-mode", "unrestricted"}, 19},
		{"restricted without the admin server", nil, 9},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := start(t, nil, append([]string{"--database-url", testenv.GatewayURL()}, c.args...)...)
			listed, err := s.session.ListTools(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(listed.Tools) != c.tools {
				t.Fatalf("%d tools listed, want %d: %v", len(listed.Tools), c.tools, names(listed.Tools))
			}
			unrestricted := c.tools == 19
			for _, tool := range listed.Tools {
				want, known := annotations[tool.Name]
				if !known {
					t.Fatalf("unexpected tool %s", tool.Name)
				}
				if c.tools == 9 && clusterTools[tool.Name] {
					t.Fatalf("cluster tool %s listed without an admin server", tool.Name)
				}
				if !unrestricted && want != "r" {
					t.Fatalf("write tool %s listed in restricted mode", tool.Name)
				}
			}
			if !unrestricted {
				_, err := s.session.CallTool(context.Background(), &mcp.CallToolParams{Name: "execute_write_sql", Arguments: map[string]any{"sql": "SELECT 1"}})
				if err == nil || !strings.Contains(err.Error(), "unknown tool") {
					t.Fatalf("execute_write_sql in restricted mode: %v", err)
				}
			}
		})
	}
}

func TestEveryToolAnswersThroughTheBinary(t *testing.T) {
	waitForSettledPrimary(t)
	schema := ordersSchema(t)
	s := start(t, nil, "--database-url", testenv.GatewayURL(), "--admin-url", testenv.AdminURL(), "--access-mode", "unrestricted",
		"--node-host-map", "localhost="+testenv.NodeHost())
	query := fmt.Sprintf("SELECT * FROM %s.orders WHERE customer_id = 5", schema)
	primary, replica := roles(t)
	called := map[string]bool{"switch_primary": true}
	check := func(name string, args map[string]any, parts ...string) string {
		t.Helper()
		called[name] = true
		text := s.mustCall(name, args)
		mustContain(t, name, text, parts...)
		return text
	}

	check("list_schemas", nil, schema, "Multigres Internal Schema", `["pg_catalog","postgres","System Schema"]`)
	mustContain(t, "server stderr", s.stderr.String(), "the database password equals the user or database name")
	check("list_objects", map[string]any{"schema_name": schema}, "orders", "customers")
	check("list_objects", map[string]any{"schema_name": "public", "object_type": "extension"}, "hypopg", "pg_stat_statements")
	check("get_object_details", map[string]any{"schema_name": schema, "object_name": "orders"}, "customer_id", "orders_pkey", "orders_customer_id_fkey")
	check("execute_sql", map[string]any{"sql": fmt.Sprintf("SELECT count(*) FROM %s.orders", schema)}, "50000", "rows: 1")
	check("explain_query", map[string]any{"sql": query, "analyze": true}, "actual time=")
	plan := check("explain_query", map[string]any{"sql": query, "hypothetical_indexes": []map[string]any{{"table": schema + ".orders", "columns": []string{"customer_id"}}}})
	if !hypothetical.MatchString(plan) {
		t.Fatalf("explain_query does not use the hypothetical index:\n%s", plan)
	}
	check("get_top_queries", map[string]any{"source": "pg_stat_statements", "sort_by": "total_time"})
	check("get_top_queries", map[string]any{"source": "gateway", "sort_by": "total_time"}, "fingerprints returned", "may have been cut by the gateway without a mark")
	check("analyze_workload_indexes", nil)
	advice := check("analyze_query_indexes", map[string]any{"queries": []string{query}}, `(\"customer_id\")`)
	costs := workloadCost.FindStringSubmatch(advice)
	if costs == nil {
		t.Fatalf("analyze_query_indexes has no workload cost line:\n%s", advice)
	}
	if before, _ := strconv.ParseFloat(costs[1], 64); before <= 0 {
		t.Fatalf("workload cost before is %s", costs[1])
	} else if recommended, _ := strconv.ParseFloat(costs[2], 64); recommended >= before {
		t.Fatalf("the recommended index does not lower the workload cost: %s before, %s after", costs[1], costs[2])
	}
	check("diagnose", map[string]any{"checks": []string{"activity", "cache"}})
	check("cluster_status", nil, primary)
	check("list_backups", nil, "backups: ")
	check("verify_backups", nil, "duration: ")
	check("read_node_log", map[string]any{"node": "primary", "max_bytes": 1024}, "node: "+primary)
	check("execute_write_sql", map[string]any{"sql": fmt.Sprintf("UPDATE %s.orders SET amount = amount WHERE id = 1", schema)}, "command: UPDATE 1", "committed")
	accepted := check("create_backup", map[string]any{"type": "full"}, "job_id: ")
	check("get_backup_job_status", map[string]any{"job_id": jobID.FindStringSubmatch(accepted)[1]})
	check("expire_backups", nil)
	check("set_postgres_restarts", map[string]any{"node": replica, "enabled": true}, "accepted: automatic Postgres restarts on "+replica+" set to true")

	if len(called) != len(annotations) {
		t.Fatalf("called %d of %d tools", len(called), len(annotations))
	}
}

func TestFailuresAreReportedAndTheServerStaysUp(t *testing.T) {
	schema := ordersSchema(t)
	closed := closedPort(t)

	t.Run("closed gateway and admin ports", func(t *testing.T) {
		adminAddress := fmt.Sprintf("127.0.0.1:%d", closedPort(t))
		s := start(t, nil, "--database-url", fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%d/postgres", closed),
			"--admin-url", "http://"+adminAddress)
		for name, args := range map[string]map[string]any{"execute_sql": {"sql": "SELECT 1"}, "list_backups": nil, "cluster_status": nil} {
			text, isError := s.call(name, args)
			if isError != (name != "cluster_status") || !strings.Contains(text, "connection refused") {
				t.Fatalf("%s on a closed port (isError %t):\n%s", name, isError, text)
			}
			if strings.Contains(text, adminAddress) {
				t.Fatalf("%s shows the admin server address:\n%s", name, text)
			}
		}
	})

	t.Run("admin server refuses the token", func(t *testing.T) {
		var mu sync.Mutex
		var seen []string
		refusing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			seen = append(seen, r.Header.Get("Authorization"))
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"code":"unauthenticated","message":"authentication failed"}`)
		}))
		t.Cleanup(refusing.Close)
		token := fmt.Sprintf("e2e-token-%d", time.Now().UnixNano())
		s := start(t, []string{"MULTIGRES_ADMIN_URL=" + refusing.URL, "MULTIGRES_ADMIN_TOKEN=" + token}, "--database-url", testenv.GatewayURL())
		for _, name := range []string{"list_backups", "cluster_status"} {
			text, isError := s.call(name, nil)
			if isError != (name != "cluster_status") || !strings.Contains(text, "admin API error unauthenticated: authentication failed") {
				t.Fatalf("%s against a refusing admin server (isError %t):\n%s", name, isError, text)
			}
			if strings.Contains(text, token) {
				t.Fatalf("the token is in the %s result:\n%s", name, text)
			}
		}
		mu.Lock()
		headers := append([]string(nil), seen...)
		mu.Unlock()
		if len(headers) == 0 || headers[0] != "Bearer "+token {
			t.Fatalf("the admin server saw Authorization %q", headers)
		}
		s.mustCall("list_schemas", nil)
	})

	t.Run("restricted mode refuses writes", func(t *testing.T) {
		s := start(t, nil, "--database-url", testenv.GatewayURL())
		before := tableState(t, schema)
		for _, sql := range []string{
			fmt.Sprintf("UPDATE %s.orders SET amount = 0", schema),
			fmt.Sprintf("ROLLBACK; DROP TABLE %s.customers;", schema),
		} {
			text, isError := s.call("execute_sql", map[string]any{"sql": sql})
			if !isError {
				t.Fatalf("restricted execute_sql accepted %q:\n%s", sql, text)
			}
		}
		if after := tableState(t, schema); after != before {
			t.Fatalf("tables changed from %q to %q", before, after)
		}
	})
}

func TestStartupArgumentsEndCleanly(t *testing.T) {
	help := exec.Command(binary, "--help")
	var helpOut bytes.Buffer
	help.Stdout, help.Stderr = &helpOut, &helpOut
	if err := help.Run(); err != nil {
		t.Fatalf("--help ended with %v:\n%s", err, helpOut.String())
	}
	mustContain(t, "--help", helpOut.String(), "-database-url", "DATABASE_URI")

	password := fmt.Sprintf("e2e-pw-%d", time.Now().UnixNano())
	bad := exec.Command(binary, "--database-url", "postgres://app:"+password+"@127.0.0.1:99999/postgres")
	var badOut bytes.Buffer
	bad.Stdout, bad.Stderr = &badOut, &badOut
	var exit *exec.ExitError
	if err := bad.Run(); !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("a URL with port 99999 ended with %v:\n%s", err, badOut.String())
	}
	mustContain(t, "port 99999", badOut.String(), "invalid port")
	if strings.Contains(badOut.String(), "panic") || strings.Contains(badOut.String(), password) {
		t.Fatalf("a URL with port 99999 shows a panic or the password:\n%s", badOut.String())
	}
}

func TestSwitchPrimaryTwice(t *testing.T) {
	waitForSettledPrimary(t)
	s := start(t, nil, "--database-url", testenv.GatewayURL(), "--admin-url", testenv.AdminURL(), "--access-mode", "unrestricted")
	for round := 1; round <= 2; round++ {
		before, _ := roles(t)
		text := s.mustCall("switch_primary", map[string]any{"reason": fmt.Sprintf("multigres-mcp e2e round %d", round), "wait_seconds": 60})
		mustContain(t, "switch_primary", text, "before: primary "+before, "this tool does not retry")
		outcomes := 0
		for _, outcome := range []string{"the primary changed from", "the same node was elected again", "no new term yet"} {
			outcomes += strings.Count(text, outcome)
		}
		if outcomes != 1 {
			t.Fatalf("round %d reports %d outcomes:\n%s", round, outcomes, text)
		}
		waitForSettledPrimary(t)
		now, _ := roles(t)
		if moved := switched.FindStringSubmatch(text); moved != nil && (moved[1] != before || moved[2] != now) {
			t.Fatalf("round %d reports %s to %s, the admin server now has %s:\n%s", round, moved[1], moved[2], now, text)
		}
		if got := after.FindStringSubmatch(text); got == nil || got[1] != now {
			t.Fatalf("round %d after line does not name %s:\n%s", round, now, text)
		}
	}
}

func TestClientCancelEndsTheStatement(t *testing.T) {
	waitForSettledPrimary(t)
	schema := ordersSchema(t)
	s := start(t, nil, "--database-url", testenv.GatewayURL())
	watcher := testenv.Connect(t, testenv.NodeURL(testenv.PrimaryPort(t)))
	t.Cleanup(func() { watcher.Close(context.Background()) })
	running := func() int64 {
		return testenv.Count(t, watcher, "SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND application_name = 'multigres-mcp' AND strpos(query, $1) > 0", schema+".orders a")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := s.session.CallTool(ctx, &mcp.CallToolParams{Name: "execute_sql", Arguments: map[string]any{
			"sql": fmt.Sprintf("SELECT count(*) FROM %[1]s.orders a, %[1]s.orders b", schema),
		}})
		done <- err
	}()
	deadline := time.Now().Add(20 * time.Second)
	for running() == 0 {
		select {
		case err := <-done:
			t.Fatalf("the call ended before its statement ran: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("the statement never showed up on the primary")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	cancelled := time.Now()
	for running() > 0 {
		if time.Since(cancelled) > 2*time.Second {
			t.Fatalf("the statement still runs 2s after the client cancelled; multigres-mcp sessions:\n%s", ourSessions(t))
		}
		time.Sleep(20 * time.Millisecond)
	}
	<-done
	mustContain(t, "execute_sql after a cancel", s.mustCall("execute_sql", map[string]any{"sql": "SELECT 1"}), "rows: 1")
}

type server struct {
	t       *testing.T
	session *mcp.ClientSession
	stderr  *lockedBuffer
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func start(t *testing.T, env []string, args ...string) *server {
	t.Helper()
	cmd := exec.Command(binary, args...)
	cmd.Env = append([]string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME")}, env...)
	stderr := &lockedBuffer{}
	cmd.Stderr = stderr
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	session, err := mcp.NewClient(&mcp.Implementation{Name: "multigres-mcp-e2e"}, nil).Connect(ctx, &mcp.CommandTransport{Command: cmd}, nil)
	if err != nil {
		t.Fatalf("start: %v\nstderr:\n%s", err, stderr)
	}
	t.Cleanup(func() {
		_ = session.Close()
		if t.Failed() {
			t.Logf("server stderr:\n%s", stderr)
		}
	})
	return &server{t: t, session: session, stderr: stderr}
}

func (s *server) call(name string, args map[string]any) (string, bool) {
	s.t.Helper()
	if args == nil {
		args = map[string]any{}
	}
	answer, err := s.session.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		s.t.Fatalf("%s: %v\nstderr:\n%s", name, err, s.stderr)
	}
	var text strings.Builder
	for _, content := range answer.Content {
		if part, ok := content.(*mcp.TextContent); ok {
			text.WriteString(part.Text)
		}
	}
	return text.String(), answer.IsError
}

func (s *server) mustCall(name string, args map[string]any) string {
	s.t.Helper()
	text, isError := s.call(name, args)
	if isError {
		s.t.Fatalf("%s failed:\n%s", name, text)
	}
	return text
}

func mustContain(t *testing.T, label, text string, parts ...string) {
	t.Helper()
	for _, part := range parts {
		if !strings.Contains(text, part) {
			t.Fatalf("%s result does not contain %q:\n%s", label, part, text)
		}
	}
}

func names(tools []*mcp.Tool) []string {
	var listed []string
	for _, tool := range tools {
		listed = append(listed, tool.Name)
	}
	sort.Strings(listed)
	return listed
}

func ordersSchema(t *testing.T) string {
	t.Helper()
	schema := testenv.Schema(t, "e2e")
	setup := testenv.Connect(t, testenv.GatewayURL())
	defer setup.Close(context.Background())
	testenv.Exec(t, setup, fmt.Sprintf(`CREATE TABLE %[1]s.customers (id int PRIMARY KEY, name text);
INSERT INTO %[1]s.customers SELECT g, 'customer ' || g FROM generate_series(0, 499) g;
CREATE TABLE %[1]s.orders (id bigint PRIMARY KEY, customer_id int REFERENCES %[1]s.customers, amount numeric, status text);
INSERT INTO %[1]s.orders SELECT g, g %% 500, g %% 97, CASE WHEN g %% 10 = 0 THEN 'open' ELSE 'done' END FROM generate_series(1, 50000) g;
ANALYZE %[1]s.customers; ANALYZE %[1]s.orders`, schema))
	return schema
}

func tableState(t *testing.T, schema string) string {
	t.Helper()
	conn := testenv.Connect(t, testenv.GatewayURL())
	defer conn.Close(context.Background())
	var state string
	err := conn.QueryRow(context.Background(), fmt.Sprintf(`SELECT (SELECT count(*) FROM %[1]s.customers) || '/' || (SELECT sum(amount) FROM %[1]s.orders)`, schema)).Scan(&state)
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func closedPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	return port
}

func ourSessions(t *testing.T) string {
	t.Helper()
	var report strings.Builder
	for _, port := range testenv.NodePorts() {
		conn := testenv.Connect(t, testenv.NodeURL(port))
		rows, err := conn.Query(context.Background(), "SELECT pg_is_in_recovery()::text, pid::text, state, backend_start::text, left(query, 80) FROM pg_stat_activity WHERE application_name = 'multigres-mcp' AND pid <> pg_backend_pid()")
		if err == nil {
			for rows.Next() {
				var recovery, pid, state, started, query string
				if rows.Scan(&recovery, &pid, &state, &started, &query) == nil {
					fmt.Fprintf(&report, "port %d recovery %s pid %s %s since %s: %s\n", port, recovery, pid, state, started, query)
				}
			}
			rows.Close()
		}
		conn.Close(context.Background())
	}
	return report.String()
}

func roles(t *testing.T) (primary, replica string) {
	t.Helper()
	poolers, err := admin.NewReader(testenv.AdminURL(), "").Poolers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, pooler := range poolers {
		if admin.IsPrimary(pooler) {
			primary = admin.NodeName(pooler)
		} else if replica == "" {
			replica = admin.NodeName(pooler)
		}
	}
	if primary == "" || replica == "" {
		t.Fatalf("want a primary and a replica, got %q and %q", primary, replica)
	}
	return primary, replica
}

func waitForSettledPrimary(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		if settled() {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatal("the cluster did not settle on one writable primary within 120s")
}

func settled() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	poolers, err := admin.NewReader(testenv.AdminURL(), "").Poolers(ctx)
	if err != nil {
		return false
	}
	primaries := 0
	for _, pooler := range poolers {
		if admin.IsPrimary(pooler) {
			primaries++
		}
	}
	if primaries != 1 {
		return false
	}
	conn, err := pgx.Connect(ctx, testenv.GatewayURL())
	if err != nil {
		return false
	}
	defer conn.Close(ctx)
	var writable bool
	return conn.QueryRow(ctx, "SELECT NOT pg_is_in_recovery()").Scan(&writable) == nil && writable
}
