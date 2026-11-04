//go:build cluster

package tools

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/multigres/multigres/go/pb/multiadmin/multiadminconnect"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/admin"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/config"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/testenv"
)

var (
	switchedTo = regexp.MustCompile(`the primary changed from (\S+) to (\S+)`)
	afterTerm  = regexp.MustCompile(`after: primary \S+, term (\d+)`)
	promotion  = regexp.MustCompile(`latest promotion in multigres.rule_history: term (\d+)`)
	restarted  = regexp.MustCompile(`tables whose cumulative row count is 0 while the catalog has rows: (\d+)`)
)

func unique(prefix string) string {
	return prefix + strconv.FormatInt(time.Now().UnixNano()%1_000_000_000_000, 36)
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

func gatewayURLAs(user, password string, port int) string {
	return (&url.URL{Scheme: "postgres", User: url.UserPassword(user, password), Host: fmt.Sprintf("localhost:%d", port), Path: "/postgres"}).String()
}

func replicaName(t *testing.T) string {
	t.Helper()
	for _, pooler := range poolerList(t) {
		if !admin.IsPrimary(pooler) {
			return admin.NodeName(pooler)
		}
	}
	t.Fatal("no replica")
	return ""
}

func primaryName(t *testing.T) string {
	t.Helper()
	for _, pooler := range poolerList(t) {
		if admin.IsPrimary(pooler) {
			return admin.NodeName(pooler)
		}
	}
	t.Fatal("no primary")
	return ""
}

func waitForSettledPrimary(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		primaries := 0
		if poolers, err := admin.NewReader(testenv.AdminURL(), "").Poolers(context.Background()); err == nil {
			for _, pooler := range poolers {
				if admin.IsPrimary(pooler) {
					primaries++
				}
			}
		}
		if primaries == 1 {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			conn, err := connectQuietly(ctx)
			if err == nil {
				var writable bool
				err = conn.QueryRow(ctx, "SELECT NOT pg_is_in_recovery()").Scan(&writable)
				conn.Close(ctx)
				cancel()
				if err == nil && writable {
					return
				}
			} else {
				cancel()
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatal("the cluster did not settle on one writable primary within 120s")
}

func writeToolCalls(schema, replica string) []toolCall {
	return []toolCall{
		{"execute_write_sql", sqlInput{SQL: fmt.Sprintf("UPDATE %s.orders SET amount = amount WHERE id = 1", schema)}},
		{"execute_write_sql", sqlInput{SQL: "SET search_path = public"}},
		{"create_backup", backupInput{Type: "full"}},
		{"create_backup", backupInput{Type: "nope"}},
		{"expire_backups", shardInput{}},
		{"set_postgres_restarts", restartsInput{Node: replica, Enabled: true}},
		{"set_postgres_restarts", restartsInput{Node: "primary", Enabled: true}},
		{"switch_primary", switchInput{Reason: " "}},
	}
}

func mustNotLeak(t *testing.T, label, text string, secrets ...string) {
	t.Helper()
	for _, secret := range secrets {
		if strings.Contains(text, secret) {
			t.Fatalf("%s leaked %q:\n%s", label, secret, text)
		}
	}
}

func TestClusterWritesKeepSecretsAndReportTheirOutcome(t *testing.T) {
	waitForSettledPrimary(t)
	schema := ordersSchema(t, "writes")
	role, password, token := unique("mcp_secret_"), unique("Pw-"), unique("Tok-")
	setup := gateway(t)
	testenv.Exec(t, setup, fmt.Sprintf("CREATE ROLE %s LOGIN SUPERUSER PASSWORD '%s'", role, password))
	t.Cleanup(func() { testenv.Exec(t, gateway(t), "DROP ROLE "+role) })
	proxy := newAdminProxy(t)
	proxy.set(func(p *adminProxy) { p.requireToken = token })
	proxyHost := strings.TrimPrefix(proxy.URL(), "http://")
	secrets := []string{password, token, proxyHost}

	good := config.Config{
		DatabaseURL: gatewayURLAs(role, password, 15432),
		AdminURL:    proxy.URL(),
		AdminToken:  token,
		NodeHostMap: map[string]string{"localhost": testenv.NodeHost()},
		Mode:        config.Unrestricted,
		SQLTimeout:  10 * time.Second,
	}
	calls := append(readToolCalls(schema), writeToolCalls(schema, replicaName(t))...)
	t.Run("every tool on success", func(t *testing.T) {
		h := newHarness(t, good)
		named := map[string]bool{"switch_primary": true}
		for _, call := range calls {
			named[call.name] = true
			text, _ := h.call(call.name, call.args)
			mustNotLeak(t, call.name, text, secrets...)
		}
		if len(named) != len(designAnnotations) {
			t.Fatalf("the sweep calls %d of %d tools", len(named), len(designAnnotations))
		}
	})

	closed := closedPort(t)
	failures := map[string]func(*config.Config){
		"closed gateway port": func(c *config.Config) { c.DatabaseURL = gatewayURLAs(role, password, closed) },
		"wrong password":      func(c *config.Config) { c.DatabaseURL = gatewayURLAs(role, unique("Pw-wrong-"), 15432) },
		"wrong token":         func(c *config.Config) { c.AdminToken = unique("Tok-wrong-") },
		"closed admin server": func(c *config.Config) { c.AdminURL = fmt.Sprintf("http://127.0.0.1:%d", closed) },
	}
	for name, change := range failures {
		t.Run(name, func(t *testing.T) {
			cfg := good
			change(&cfg)
			extra := append([]string(nil), secrets...)
			if u, err := url.Parse(cfg.DatabaseURL); err == nil {
				if p, ok := u.User.Password(); ok {
					extra = append(extra, p)
				}
			}
			extra = append(extra, cfg.AdminToken, strings.TrimPrefix(cfg.AdminURL, "http://"))
			h := newHarness(t, cfg)
			failed := 0
			for _, call := range calls {
				text, isError := h.call(call.name, call.args)
				if isError {
					failed++
				}
				mustNotLeak(t, call.name, text, extra...)
			}
			if failed == 0 {
				t.Fatal("no call failed")
			}
		})
	}

	t.Run("switch, then the statistics period and the index advisor", func(t *testing.T) {
		switchThenReadPeriod(t, newHarness(t, good), schema, secrets)
	})
}

func switchThenReadPeriod(t *testing.T, h *harness, schema string, secrets []string) {
	before := primaryName(t)
	var text string
	var changed []string
	for attempt := 0; attempt < 4 && changed == nil; attempt++ {
		if attempt > 0 {
			mustContain(t, text, "the same node was elected again")
			waitForSettledPrimary(t)
		}
		text = h.mustCall("switch_primary", switchInput{Reason: "multigres-mcp integration test", WaitSeconds: 60})
		mustNotLeak(t, "switch_primary", text, secrets...)
		changed = switchedTo.FindStringSubmatch(text)
	}
	if changed == nil || changed[1] != before {
		t.Fatalf("switch_primary did not report a change from %s:\n%s", before, text)
	}
	term := afterTerm.FindStringSubmatch(text)[1]
	waitForSettledPrimary(t)
	advice := h.mustCall("analyze_query_indexes", queryIndexesInput{Queries: []string{fmt.Sprintf("SELECT * FROM %s.orders WHERE customer_id = 5", schema)}})
	mustContain(t, advice, `(\"customer_id\")`)
	for _, node := range []string{changed[2], changed[1]} {
		period := h.mustCall("diagnose", diagnoseInput{Node: node, Checks: []string{"cache"}})
		if got := promotion.FindStringSubmatch(period); got == nil || got[1] != term {
			t.Fatalf("%s: the period does not show the promotion of term %s:\n%s", node, term, period)
		}
		if node == changed[2] {
			if got := restarted.FindStringSubmatch(period); got == nil || got[1] == "0" {
				t.Fatalf("new primary %s: no table with restarted statistics:\n%s", node, period)
			}
			mustContain(t, period, "counts only writes made on that node and discards its counts when it starts with crash recovery", "0 does not mean nothing is missing")
		}
	}
}

func TestClusterLateAdminResponsesAreReadAgain(t *testing.T) {
	late := newLateAdmin(t)
	h := late.h
	switchCall := multiadminconnect.MultiadminServiceSwitchPrimaryProcedure
	poolersCall := multiadminconnect.MultiadminServiceGetPoolersProcedure
	expireCall := multiadminconnect.MultiadminServiceExpireBackupsProcedure
	backupsCall := multiadminconnect.MultiadminServiceGetBackupsProcedure

	late.proxy.set(func(p *adminProxy) { p.hold[switchCall] = 8 * time.Second })
	text, _, started := late.within(3*time.Second, func(ctx context.Context, out *output) error {
		return h.deps.switchPrimary(ctx, switchInput{Reason: "late response test", WaitSeconds: 3}, out)
	})
	mustContain(t, text, "switch request:", "after: primary")
	late.rereadAfter(started.Add(3*time.Second), poolersCall)
	late.calledOnce(switchCall)
	waitForSettledPrimary(t)

	late.proxy.set(func(p *adminProxy) { p.swallow[switchCall] = true })
	late.proxy.refuse(poolersCall, switchCall)
	text, err, _ := late.within(time.Minute, func(ctx context.Context, out *output) error {
		return h.deps.switchPrimary(ctx, switchInput{Reason: "late response test", WaitSeconds: 3}, out)
	})
	if err == nil {
		t.Fatalf("an unknown switch outcome did not fail:\n%s", text)
	}
	mustContain(t, text, "result unknown: the switch request and the re-read both failed", "refused by the test proxy")
	if n := len(late.proxy.callsTo(switchCall)); n != 2 {
		t.Fatalf("SwitchPrimary was called %d times over two calls", n)
	}
	late.proxy.set(func(p *adminProxy) { delete(p.refuseAfter, poolersCall) })

	late.proxy.set(func(p *adminProxy) { p.hold[expireCall] = 6 * time.Second })
	text, err, started = late.within(2*time.Second, func(ctx context.Context, out *output) error {
		return h.deps.expireBackups(ctx, shardInput{}, out)
	})
	if err == nil {
		t.Fatalf("a late expire did not fail:\n%s", text)
	}
	mustContain(t, text, "expire failed:", "Current list:")
	late.rereadAfter(started.Add(2*time.Second), backupsCall)
	late.calledOnce(expireCall)

	late.proxy.refuse(backupsCall, expireCall)
	text, err, _ = late.within(2*time.Second, func(ctx context.Context, out *output) error {
		return h.deps.expireBackups(ctx, shardInput{}, out)
	})
	if err == nil {
		t.Fatalf("expire with a refused re-read did not fail:\n%s", text)
	}
	mustContain(t, text, "expire failed:", "could not read the list:", "refused by the test proxy")

	late.proxy.set(func(p *adminProxy) {
		p.hold[multiadminconnect.MultiadminServiceSetPostgresRestartsEnabledProcedure] = 6 * time.Second
	})
	text, err, _ = late.within(2*time.Second, func(ctx context.Context, out *output) error {
		return h.deps.setPostgresRestarts(ctx, restartsInput{Node: replicaName(t), Enabled: true}, out)
	})
	if err == nil {
		t.Fatalf("a late restart switch did not fail:\n%s", text)
	}
	mustContain(t, text, "result unknown")
}

func TestClusterVerificationsDoNotOverlap(t *testing.T) {
	late := newLateAdmin(t)
	h := late.h
	verifyCall := multiadminconnect.MultiadminServiceVerifyBackupsProcedure
	late.proxy.set(func(p *adminProxy) { p.hold[verifyCall] = 6 * time.Second })
	first := make(chan error, 1)
	go func() {
		out := output{redact: h.deps.redact}
		first <- h.deps.verifyBackups(context.Background(), shardInput{}, &out)
	}()
	for deadline := time.Now().Add(10 * time.Second); len(late.proxy.callsTo(verifyCall)) == 0; time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the first verification never reached the admin server")
		}
	}
	text, isError := h.call("verify_backups", shardInput{})
	if !isError {
		t.Fatalf("a second verification ran while the first was running:\n%s", text)
	}
	mustContain(t, text, "is already running in this server")
	<-first
	text, err, _ := late.within(2*time.Second, func(ctx context.Context, out *output) error {
		return h.deps.verifyBackups(ctx, shardInput{}, out)
	})
	if err == nil {
		t.Fatalf("a late verification did not fail:\n%s", text)
	}
	mustContain(t, text, "check the pooler's running state before starting another verification")
	text, isError = h.call("verify_backups", shardInput{})
	if !isError {
		t.Fatalf("a verification started after one timed out:\n%s", text)
	}
	mustContain(t, text, "an earlier verification of", "timed out", "check the pooler's running state")
	if n := len(late.proxy.callsTo(verifyCall)); n != 2 {
		t.Fatalf("VerifyBackups was called %d times", n)
	}
}

type lateAdmin struct {
	t     *testing.T
	proxy *adminProxy
	h     *harness
}

func newLateAdmin(t *testing.T) *lateAdmin {
	waitForSettledPrimary(t)
	proxy := newAdminProxy(t)
	cfg := clusterConfig(config.Unrestricted)
	cfg.AdminURL = proxy.URL()
	return &lateAdmin{t: t, proxy: proxy, h: newHarness(t, cfg)}
}

func (l *lateAdmin) within(limit time.Duration, run func(context.Context, *output) error) (string, error, time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), limit)
	defer cancel()
	started := time.Now()
	out := output{redact: l.h.deps.redact}
	err := run(ctx, &out)
	return out.finish(), err, started
}

func (l *lateAdmin) rereadAfter(deadline time.Time, procedure string) {
	for _, call := range l.proxy.callsTo(procedure) {
		if call.at.After(deadline) {
			return
		}
	}
	l.t.Fatalf("no %s after the deadline %s; calls %v", procedure, deadline, l.proxy.recorded())
}

func (l *lateAdmin) calledOnce(procedure string) {
	if n := len(l.proxy.callsTo(procedure)); n != 1 {
		l.t.Fatalf("%s was called %d times", procedure, n)
	}
}

func connectQuietly(ctx context.Context) (*pgx.Conn, error) {
	return pgx.Connect(ctx, testenv.GatewayURL())
}
