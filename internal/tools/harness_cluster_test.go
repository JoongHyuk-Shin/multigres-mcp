//go:build cluster

package tools

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/admin"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/config"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/db"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/testenv"
)

const clientsPerMode = 40

var hypotheticalName = regexp.MustCompile(`<\d+>`)

type harness struct {
	t      *testing.T
	deps   *Deps
	client *mcp.ClientSession
}

func clusterConfig(mode config.Mode) config.Config {
	return config.Config{
		DatabaseURL: testenv.GatewayURL(),
		AdminURL:    testenv.AdminURL(),
		NodeHostMap: map[string]string{"localhost": testenv.NodeHost()},
		Mode:        mode,
		SQLTimeout:  10 * time.Second,
	}
}

func newHarness(t *testing.T, cfg config.Config) *harness {
	t.Helper()
	database, err := db.Open(db.Options{URL: cfg.DatabaseURL, Writable: cfg.Mode == config.Unrestricted})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(database.Close)
	deps := &Deps{Config: cfg, DB: database, Admin: admin.NewReader(cfg.AdminURL, cfg.AdminToken)}
	if cfg.Mode == config.Unrestricted {
		deps.Writer = admin.NewWriter(cfg.AdminURL, cfg.AdminToken)
	}
	return &harness{t: t, deps: deps, client: session(t, NewServer(deps, "test"))}
}

func (h *harness) call(name string, args any) (string, bool) {
	h.t.Helper()
	answer, err := h.client.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		h.t.Fatalf("%s: %v", name, err)
	}
	return answer.Content[0].(*mcp.TextContent).Text, answer.IsError
}

func (h *harness) mustCall(name string, args any) string {
	h.t.Helper()
	text, isError := h.call(name, args)
	if isError {
		h.t.Fatalf("%s failed: %s", name, text)
	}
	return text
}

func (h *harness) cancelWhileRunning(marker string, run func(context.Context, *output) error) (string, error) {
	h.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := output{redact: h.deps.redact}
	done := make(chan error, 1)
	go func() { done <- run(ctx, &out) }()
	watcher := primary(h.t)
	deadline := time.Now().Add(20 * time.Second)
	for {
		select {
		case err := <-done:
			h.t.Fatalf("the call ended before a statement with %q ran (error %v):\n%s", marker, err, out.finish())
		default:
		}
		running := testenv.Count(h.t, watcher, "SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND pid <> pg_backend_pid() AND strpos(query, $1) > 0", marker)
		if running > 0 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			h.t.Fatalf("no statement with %q started", marker)
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	err := <-done
	return out.finish(), err
}

func gateway(t *testing.T) *pgx.Conn {
	t.Helper()
	conn := testenv.Connect(t, testenv.GatewayURL())
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

func primary(t *testing.T) *pgx.Conn {
	t.Helper()
	conn := testenv.Connect(t, testenv.NodeURL(testenv.PrimaryPort(t)))
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

func ordersSchema(t *testing.T, prefix string) string {
	t.Helper()
	schema := testenv.Schema(t, prefix)
	setup := gateway(t)
	testenv.Exec(t, setup, fmt.Sprintf(`CREATE TABLE %[1]s.orders (id bigint PRIMARY KEY, customer_id int, amount numeric, status text);
INSERT INTO %[1]s.orders SELECT g, g %% 500, g %% 97, CASE WHEN g %% 10 = 0 THEN 'open' ELSE 'done' END FROM generate_series(1, 50000) g;
CREATE TABLE %[1]s.lines (order_id bigint, sku text);
INSERT INTO %[1]s.lines SELECT g, 'sku' || (g %% 50) FROM generate_series(1, 20000) g;
ANALYZE %[1]s.orders; ANALYZE %[1]s.lines`, schema))
	return schema
}

func lockTable(t *testing.T, table string) (release func()) {
	t.Helper()
	locker := testenv.Connect(t, testenv.GatewayURL())
	testenv.Exec(t, locker, "BEGIN")
	testenv.Exec(t, locker, "LOCK TABLE "+table+" IN ACCESS EXCLUSIVE MODE")
	return func() {
		testenv.Exec(t, locker, "ROLLBACK")
		locker.Close(context.Background())
	}
}

type visibility struct {
	autocommit, transaction int
}

type probe func(ctx context.Context, conn *pgx.Conn) (bool, error)

func planUsesHypothetical(query string) probe {
	return func(ctx context.Context, conn *pgx.Conn) (bool, error) {
		rows, err := conn.Query(ctx, "EXPLAIN "+query)
		if err != nil {
			return false, err
		}
		plan, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return false, err
		}
		return hypotheticalName.MatchString(strings.Join(plan, "\n")), nil
	}
}

func hidesIndexes(ctx context.Context, conn *pgx.Conn) (bool, error) {
	var hidden int64
	err := conn.QueryRow(ctx, "SELECT count(*) FROM hypopg_hidden_indexes").Scan(&hidden)
	return hidden > 0, err
}

func serverConnConfig(t *testing.T) *pgx.ConnConfig {
	t.Helper()
	cfg, err := db.ReadConnConfig(testenv.GatewayURL())
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func countClients(t *testing.T, check probe) visibility {
	t.Helper()
	cfg := serverConnConfig(t)
	var seen visibility
	for i := 0; i < clientsPerMode; i++ {
		if onNewClient(t, cfg, false, check) {
			seen.autocommit++
		}
		if onNewClient(t, cfg, true, check) {
			seen.transaction++
		}
	}
	return seen
}

func onNewClient(t *testing.T, cfg *pgx.ConnConfig, inTransaction bool, check probe) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("counter connection: %v", err)
	}
	defer conn.Close(ctx)
	if inTransaction {
		if _, err := conn.Exec(ctx, "BEGIN TRANSACTION READ ONLY"); err != nil {
			t.Fatal(err)
		}
	}
	found, err := check(ctx, conn)
	if err != nil {
		t.Fatalf("counter probe: %v", err)
	}
	if inTransaction {
		if _, err := conn.Exec(ctx, "ROLLBACK"); err != nil {
			t.Fatal(err)
		}
	}
	return found
}

func busySessions(t *testing.T, n int) (stop func()) {
	t.Helper()
	cfg := serverConnConfig(t)
	quit := make(chan struct{})
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			for {
				select {
				case <-quit:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if conn, err := pgx.ConnectConfig(ctx, cfg); err == nil {
					_, _ = conn.Exec(ctx, "SELECT pg_sleep(0.05)")
					conn.Close(ctx)
				}
				cancel()
			}
		}()
	}
	return func() {
		close(quit)
		for i := 0; i < n; i++ {
			<-done
		}
	}
}

func leaveOnPooledConnection(t *testing.T, statement string, inTransaction bool) (disconnect func()) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.ConnectConfig(ctx, serverConnConfig(t))
	if err != nil {
		t.Fatal(err)
	}
	if inTransaction {
		testenv.Exec(t, conn, "BEGIN TRANSACTION READ ONLY")
	}
	testenv.Exec(t, conn, statement)
	if inTransaction {
		testenv.Exec(t, conn, "ROLLBACK")
	}
	return func() { conn.Close(ctx) }
}

func resetClients(t *testing.T) {
	t.Helper()
	reset := func(ctx context.Context, conn *pgx.Conn) (bool, error) {
		_, err := conn.Exec(ctx, "SELECT hypopg_reset(), hypopg_unhide_all_indexes()")
		return false, err
	}
	countClients(t, reset)
}

func heldAdvisory(t *testing.T, base int) int64 {
	t.Helper()
	return testenv.Count(t, primary(t), "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND objid IN ($1, $2, $3)", base, base+1, base+2)
}
