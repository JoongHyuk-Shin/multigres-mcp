//go:build cluster

package tools

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/multigres/multigres/go/pb/multiadmin/multiadminconnect"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/admin"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/config"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/db"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/testenv"
)

var committedLine = regexp.MustCompile(`(?m)^committed$`)

func TestFailedCleanupIsNotReportedAsCommitted(t *testing.T) {
	schema := testenv.Schema(t, "settletool")
	testenv.Exec(t, gateway(t), fmt.Sprintf(`CREATE TABLE %[1]s.t (a int);
CREATE FUNCTION %[1]s.mk() RETURNS oid LANGUAGE sql AS $$ SELECT indexrelid FROM public.hypopg_create_index('CREATE INDEX ON %[1]s.t (a)') $$`, schema))
	h := newHarness(t, clusterConfig(config.Unrestricted))
	restore := db.BreakEndReset()
	text, isError := h.call("execute_write_sql", sqlInput{SQL: fmt.Sprintf("INSERT INTO %[1]s.t SELECT 1 FROM %[1]s.mk()", schema)})
	restore()
	if !isError || committedLine.MatchString(text) {
		t.Fatalf("a write whose cleanup failed was reported as committed:\n%s", text)
	}
	mustContain(t, text, "not committed")
	if rows := testenv.Count(t, gateway(t), fmt.Sprintf("SELECT count(*) FROM %s.t", schema)); rows != 0 {
		t.Fatalf("%d rows were committed", rows)
	}
}

func TestWriteErrorAfterTheSizeLimitFailsTheCall(t *testing.T) {
	schema := testenv.Schema(t, "latewrite")
	testenv.Exec(t, gateway(t), fmt.Sprintf(`CREATE TABLE %[1]s.t (a int);
CREATE FUNCTION %[1]s.put(g int) RETURNS text LANGUAGE plpgsql AS $$
BEGIN
	INSERT INTO %[1]s.t VALUES (g);
	RETURN repeat('x', 1000) || (1 / (300 - g));
END $$`, schema))
	h := newHarness(t, clusterConfig(config.Unrestricted))
	text, isError := h.call("execute_write_sql", sqlInput{SQL: fmt.Sprintf("SELECT %s.put(g) FROM generate_series(1, 300) g", schema)})
	if !isError || committedLine.MatchString(text) {
		t.Fatalf("a statement that failed after the size limit was reported as a success:\n%s", text[max(0, len(text)-1500):])
	}
	mustContain(t, text, "[result truncated", "division by zero")
	if rows := testenv.Count(t, gateway(t), fmt.Sprintf("SELECT count(*) FROM %s.t", schema)); rows != 0 {
		t.Fatalf("%d rows were committed", rows)
	}
}

func TestTemporaryTablesFromFunctionBodiesAreRolledBack(t *testing.T) {
	schema := testenv.Schema(t, "tempbody")
	table := unique("mcp_tmp_")
	testenv.Exec(t, gateway(t), fmt.Sprintf(`CREATE FUNCTION %s.mk_temp() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
	CREATE TEMP TABLE %s (a int);
	RETURN 1;
END $$`, schema, table))
	left := func() int64 {
		return testenv.Count(t, primary(t), "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = $1 AND relpersistence = 't'", table)
	}
	t.Cleanup(func() {
		for i := 0; i < 60 && left() > 0; i++ {
			conn := gateway(t)
			_, _ = conn.Exec(context.Background(), "BEGIN")
			_, _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS pg_temp."+table)
			_, _ = conn.Exec(context.Background(), "COMMIT")
		}
	})
	h := newHarness(t, clusterConfig(config.Unrestricted))
	text, isError := h.call("execute_write_sql", sqlInput{SQL: fmt.Sprintf("SELECT %s.mk_temp()", schema)})
	if !isError || committedLine.MatchString(text) {
		t.Fatalf("a statement that created a temporary table was committed:\n%s", text)
	}
	mustContain(t, text, "temporary schema")
	if n := left(); n != 0 {
		t.Fatalf("%d temporary tables named %s are left on the gateway's pooled connections", n, table)
	}
}

func TestObjectDetailsStopsAtTheSizeLimit(t *testing.T) {
	schema := testenv.Schema(t, "widetable")
	columns := []string{"id int PRIMARY KEY"}
	for i := range 3 {
		columns = append(columns, fmt.Sprintf("c%d text DEFAULT '%s'", i, strings.Repeat("d", 20000)))
	}
	testenv.Exec(t, gateway(t), fmt.Sprintf("CREATE TABLE %s.wide (%s)", schema, strings.Join(columns, ", ")))
	h := newHarness(t, clusterConfig(config.Restricted))
	text, isError := h.call("get_object_details", objectInput{SchemaName: schema, ObjectName: "wide"})
	if isError {
		t.Fatalf("a result cut at the size limit became an error:\n%s", text[max(0, len(text)-1500):])
	}
	mustContain(t, text, "[result truncated", "answered_by: ")
	if shown := strings.Count(text, "rows shown: "); shown != 1 {
		t.Fatalf("the statements after the cut column list also ran (%d lines of rows shown):\n%s", shown, text[max(0, len(text)-1500):])
	}
}

func TestConstraintsComeFromTheNamedTableOnly(t *testing.T) {
	schema := testenv.Schema(t, "samefk")
	testenv.Exec(t, gateway(t), fmt.Sprintf(`CREATE TABLE %[1]s.parent (id int PRIMARY KEY);
CREATE TABLE %[1]s.one (x_one int, CONSTRAINT same_fk FOREIGN KEY (x_one) REFERENCES %[1]s.parent (id));
CREATE TABLE %[1]s.two (x_two int, CONSTRAINT same_fk FOREIGN KEY (x_two) REFERENCES %[1]s.parent (id))`, schema))
	h := newHarness(t, clusterConfig(config.Restricted))
	text := h.mustCall("get_object_details", objectInput{SchemaName: schema, ObjectName: "one"})
	mustContain(t, text, `["same_fk","FOREIGN KEY","x_one"]`)
	if strings.Contains(text, "x_two") {
		t.Fatalf("the details of one show a column of two:\n%s", text)
	}
}

func TestNodeLogThatStartsInsideACharacter(t *testing.T) {
	node := primaryName(t)
	conn := primary(t)
	h := newHarness(t, clusterConfig(config.Restricted))
	for attempt := 0; attempt < 5; attempt++ {
		marker := unique("mcp_marker_")
		_, _ = conn.Exec(context.Background(), `SELECT 1 FROM "한글로_된_없는_표"`)
		_, _ = conn.Exec(context.Background(), fmt.Sprintf(`SELECT 1 FROM %q`, marker))
		size, tail := logTail(t)
		start := bytes.LastIndex(tail, []byte("한"))
		if start < 0 {
			t.Fatal("the error with a Korean name is not in the log")
		}
		maxBytes := len(tail) - start - 1
		text, isError := h.call("read_node_log", nodeLogInput{Node: node, MaxBytes: maxBytes})
		if isError {
			t.Fatalf("reading from the middle of a character failed:\n%s", text)
		}
		if after, _ := logTail(t); after != size {
			continue
		}
		mustContain(t, text, "node: "+node, marker)
		return
	}
	t.Fatal("the log kept growing during every attempt")
}

func logTail(t *testing.T) (int64, []byte) {
	t.Helper()
	conn := primary(t)
	var file string
	var size int64
	if err := conn.QueryRow(context.Background(), "SELECT coalesce(pg_current_logfile(), 'postgresql.log')").Scan(&file); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(context.Background(), "SELECT size FROM pg_stat_file($1)", file).Scan(&size); err != nil {
		t.Fatal(err)
	}
	var tail []byte
	length := min(size, maxLogBytes)
	if err := conn.QueryRow(context.Background(), "SELECT pg_read_binary_file($1, $2, $3)", file, size-length, length).Scan(&tail); err != nil {
		t.Fatal(err)
	}
	return size, tail
}

func TestClusterStatusWithStalledNodes(t *testing.T) {
	proxy := newAdminProxy(t)
	proxy.set(func(p *adminProxy) {
		p.holdFirst[multiadminconnect.MultiadminServiceGetPoolerStatusProcedure] = []time.Duration{40 * time.Second, 40 * time.Second}
	})
	cfg := clusterConfig(config.Restricted)
	cfg.AdminURL = proxy.URL()
	h := newHarness(t, cfg)
	started := time.Now()
	text := h.mustCall("cluster_status", clusterStatusInput{})
	took := time.Since(started)
	if n := strings.Count(text, `"node":`); n != 2 {
		t.Fatalf("the cluster has %d poolers; this test holds the status of two:\n%s", n, text)
	}
	if n := strings.Count(text, "no response"); n != 2 {
		t.Fatalf("%d nodes without a status, want the two held:\n%s", n, text)
	}
	if took > admin.RequestTimeout+10*time.Second {
		t.Fatalf("two held node statuses took %s, so they were not asked together", took)
	}
	if strings.Contains(text, `"gateways": "error`) || strings.Contains(text, `"orchestrators": "error`) {
		t.Fatalf("the held node status took the other calls with it:\n%s", text)
	}
}

func TestDiagnoseKeepsLaterChecksWhenOneStalls(t *testing.T) {
	testenv.Exec(t, gateway(t), "SELECT pg_stat_statements_reset()")
	proxy := newAdminProxy(t)
	proxy.set(func(p *adminProxy) {
		p.hold[multiadminconnect.MultiadminServiceGetGatewayQueriesProcedure] = 60 * time.Second
	})
	cfg := clusterConfig(config.Restricted)
	cfg.AdminURL = proxy.URL()
	cfg.SQLTimeout = 5 * time.Second
	h := newHarness(t, cfg)
	text := h.mustCall("diagnose", diagnoseInput{Checks: []string{"slow_queries", "constraints"}})
	slow := checkSection(t, text, "slow_queries")
	mustContain(t, slow, "admin API error deadline_exceeded", "gateway query statistics: none of the", "verdict: ")
	if strings.Contains(slow, "could not check") {
		t.Fatalf("the slow query check returned at its deadline but was reported as not checked:\n%s", slow)
	}
	mustContain(t, checkSection(t, text, "constraints"), "verdict: ")
}

func TestTopQueriesFailWhenNoGatewayAnswers(t *testing.T) {
	proxy := newAdminProxy(t)
	proxy.set(func(p *adminProxy) { p.swallow[multiadminconnect.MultiadminServiceGetGatewayQueriesProcedure] = true })
	cfg := clusterConfig(config.Restricted)
	cfg.AdminURL = proxy.URL()
	h := newHarness(t, cfg)
	text, isError := h.call("get_top_queries", topQueriesInput{Source: "gateway"})
	if !isError {
		t.Fatalf("gateway statistics that no gateway gave were reported as a success:\n%s", text)
	}
	mustContain(t, text, "swallowed by the test proxy")
}

func TestCancelledVerificationIsNotStartedAgain(t *testing.T) {
	verifyCall := multiadminconnect.MultiadminServiceVerifyBackupsProcedure
	proxy := newAdminProxy(t)
	proxy.set(func(p *adminProxy) {
		p.swallow[verifyCall] = true
		p.hold[verifyCall] = 10 * time.Second
	})
	cfg := clusterConfig(config.Restricted)
	cfg.AdminURL = proxy.URL()
	h := newHarness(t, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		out := output{redact: h.deps.redact}
		done <- h.deps.verifyBackups(ctx, shardInput{}, &out)
	}()
	for deadline := time.Now().Add(10 * time.Second); len(proxy.callsTo(verifyCall)) == 0; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the verification never reached the admin server")
		}
	}
	cancel()
	<-done
	text, isError := h.call("verify_backups", shardInput{})
	if !isError || len(proxy.callsTo(verifyCall)) != 1 {
		t.Fatalf("a verification started again after one was cancelled (%d calls):\n%s", len(proxy.callsTo(verifyCall)), text)
	}
	mustContain(t, text, "an earlier verification of", "was cancelled", "check the pooler's running state")
}

func TestWriteWithoutColumnsHasNoRowCount(t *testing.T) {
	schema := ordersSchema(t, "norows")
	h := newHarness(t, clusterConfig(config.Unrestricted))
	text := h.mustCall("execute_write_sql", sqlInput{SQL: fmt.Sprintf("UPDATE %s.orders SET amount = amount WHERE id <= 5", schema)})
	mustContain(t, text, "command: UPDATE 5")
	if strings.Contains(text, "rows: ") {
		t.Fatalf("an UPDATE without RETURNING shows a row count:\n%s", text)
	}
}

func TestUnansweredAddressesEndWithinTheOpenDeadline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var held []net.Conn
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			held = append(held, conn)
			mu.Unlock()
		}
	}()
	port := listener.Addr().(*net.TCPAddr).Port

	silentGateway := clusterConfig(config.Restricted)
	silentGateway.DatabaseURL = fmt.Sprintf("postgres://postgres:x@127.0.0.1:%d/postgres", port)
	proxy := newAdminProxy(t)
	proxy.set(func(p *adminProxy) {
		p.rewrite = func(list []*clustermetadatapb.Multipooler) {
			for _, pooler := range list {
				pooler.Hostname = "silent.svc"
				pooler.PortMap["postgres"] = int32(port)
			}
		}
	})
	silentNode := clusterConfig(config.Restricted)
	silentNode.AdminURL = proxy.URL()
	silentNode.NodeHostMap = map[string]string{"silent.svc": "127.0.0.1"}

	gatewayHarness, nodeHarness := newHarness(t, silentGateway), newHarness(t, silentNode)
	t.Cleanup(func() {
		listener.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range held {
			conn.Close()
		}
	})
	calls := map[string]func() (string, bool){
		"list_schemas":  func() (string, bool) { return gatewayHarness.call("list_schemas", noInput{}) },
		"read_node_log": func() (string, bool) { return nodeHarness.call("read_node_log", nodeLogInput{MaxBytes: 1024}) },
	}
	type outcome struct {
		name    string
		text    string
		isError bool
		took    time.Duration
	}
	results := make(chan outcome, len(calls))
	for name, call := range calls {
		go func() {
			started := time.Now()
			text, isError := call()
			results <- outcome{name, text, isError, time.Since(started)}
		}()
	}
	watchdog := time.After(25 * time.Second)
	for range calls {
		select {
		case got := <-results:
			if !got.isError || got.took > 15*time.Second {
				t.Errorf("%s against an address that never answers: error %t after %s:\n%s", got.name, got.isError, got.took, got.text)
			}
		case <-watchdog:
			t.Fatal("a call against an address that never answers was still waiting after 25s")
		}
	}
}

const gatewayReadPool = 4

func TestPoolRecoversAfterUnansweredConnects(t *testing.T) {
	cfg := clusterConfig(config.Restricted)
	target, err := url.Parse(cfg.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gatewayAddress := target.Host
	var mu sync.Mutex
	var held []net.Conn
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			silent := len(held) < gatewayReadPool
			held = append(held, conn)
			mu.Unlock()
			if !silent {
				go forward(conn, gatewayAddress)
			}
		}
	}()
	target.Host = listener.Addr().String()
	cfg.DatabaseURL = target.String()
	h := newHarness(t, cfg)
	t.Cleanup(func() {
		listener.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, conn := range held {
			conn.Close()
		}
	})
	var wg sync.WaitGroup
	for range gatewayReadPool {
		wg.Go(func() {
			if text, isError := h.call("list_schemas", noInput{}); !isError {
				t.Errorf("a connection that never answered gave a result:\n%s", text)
			}
		})
	}
	wg.Wait()
	if text, isError := h.call("list_schemas", noInput{}); isError {
		t.Fatalf("after %d unanswered connections the gateway pool did not recover:\n%s", gatewayReadPool, text)
	}
}

func forward(conn net.Conn, address string) {
	defer conn.Close()
	upstream, err := net.Dial("tcp", address)
	if err != nil {
		return
	}
	defer upstream.Close()
	go func() {
		_, _ = io.Copy(upstream, conn)
		upstream.Close()
	}()
	_, _ = io.Copy(conn, upstream)
}

func TestBusyPoolCallEndsAtTheOpenDeadline(t *testing.T) {
	h := newHarness(t, clusterConfig(config.Restricted))
	release := make(chan struct{})
	time.AfterFunc(15*time.Second, func() { close(release) })
	holding := make(chan struct{}, gatewayReadPool)
	var wg sync.WaitGroup
	defer wg.Wait()
	for range gatewayReadPool {
		wg.Go(func() {
			_, _ = h.deps.DB.Read(context.Background(), db.Plan{Timeout: 30 * time.Second}, func(ctx context.Context, _ *db.Tx) error {
				holding <- struct{}{}
				<-release
				return nil
			})
		})
	}
	for range gatewayReadPool {
		<-holding
	}
	started := time.Now()
	text, isError := h.call("list_schemas", noInput{})
	if took := time.Since(started); !isError || took > 15*time.Second {
		t.Fatalf("a call that found every gateway connection busy: error %t after %s:\n%s", isError, took, text)
	}
}
