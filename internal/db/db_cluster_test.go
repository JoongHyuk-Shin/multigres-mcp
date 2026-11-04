//go:build cluster

package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/testenv"
)

func singleSessionDB(t *testing.T, url string, readOnlyOption bool) *DB {
	t.Helper()
	pool, err := newPool(url, readOnlyOption, 1, protocolCancel)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	canceler, err := newCanceler(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(canceler.close)
	return &DB{read: pool, write: pool, canceler: canceler}
}

func gatewayDB(t *testing.T) *DB {
	t.Helper()
	d, err := Open(Options{URL: testenv.GatewayURL(), Writable: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)
	return d
}

func sqlState(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func drain(ctx context.Context, tx *Tx, q Query) error {
	rows, err := tx.Query(ctx, q)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
	}
	return rows.Err()
}

func TestReadOnlyStartupOptionSurvivesBatchRollback(t *testing.T) {
	schema := testenv.Schema(t, "layer3")
	setup := testenv.Connect(t, testenv.GatewayURL())
	defer setup.Close(context.Background())
	testenv.Exec(t, setup, fmt.Sprintf("CREATE TABLE %s.kept (a int); CREATE TABLE %s.dropped (a int)", schema, schema))

	batch := func(readOnly bool, table string) error {
		cfg, err := connConfig(testenv.GatewayURL(), readOnly, nil)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := pgx.ConnectConfig(context.Background(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close(context.Background())
		if _, err := conn.Exec(context.Background(), "BEGIN READ ONLY"); err != nil {
			t.Fatal(err)
		}
		_, err = conn.PgConn().Exec(context.Background(), fmt.Sprintf("ROLLBACK; DROP TABLE %s.%s", schema, table)).ReadAll()
		return err
	}
	if err := batch(true, "kept"); sqlState(err) != "25006" {
		t.Fatalf("with the startup option the DROP should fail with 25006, got %v", err)
	}
	if err := batch(false, "dropped"); err != nil {
		t.Fatalf("control without the option should drop the table, got %v", err)
	}
	tables := testenv.Count(t, setup, "SELECT count(*) FROM pg_tables WHERE schemaname = $1", schema)
	if tables != 1 {
		t.Fatalf("expected only the kept table to remain, found %d tables", tables)
	}
}

func TestExtendedProtocolRefusesTwoStatements(t *testing.T) {
	d := gatewayDB(t)
	_, err := d.Read(context.Background(), Plan{Timeout: 5 * time.Second}, func(ctx context.Context, tx *Tx) error {
		rows, err := tx.queryText(ctx, "SELECT 1; SELECT 2")
		if err != nil {
			return err
		}
		rows.Close()
		return rows.Err()
	})
	if sqlState(err) != "42601" {
		t.Fatalf("two statements in one extended-protocol message: got %v, want SQLSTATE 42601", err)
	}
}

func TestReadTransactionRejectsWritesWithoutStartupOption(t *testing.T) {
	schema := testenv.Schema(t, "layer4")
	setup := testenv.Connect(t, testenv.GatewayURL())
	defer setup.Close(context.Background())
	testenv.Exec(t, setup, fmt.Sprintf("CREATE TABLE %s.t (a int)", schema))
	d := singleSessionDB(t, testenv.GatewayURL(), false)
	_, err := d.Read(context.Background(), Plan{Timeout: 5 * time.Second}, func(ctx context.Context, tx *Tx) error {
		return drain(ctx, tx, Query(fmt.Sprintf("INSERT INTO %s.t VALUES (1)", schema)))
	})
	if sqlState(err) != "25006" {
		t.Fatalf("expected 25006, got %v", err)
	}
	if rows := testenv.Count(t, setup, fmt.Sprintf("SELECT count(*) FROM %s.t", schema)); rows != 0 {
		t.Fatalf("expected no rows, found %d", rows)
	}
}

func advisoryView(t *testing.T, schema string, base int) string {
	t.Helper()
	setup := testenv.Connect(t, testenv.GatewayURL())
	defer setup.Close(context.Background())
	testenv.Exec(t, setup, fmt.Sprintf(`CREATE FUNCTION %[1]s.take() RETURNS int LANGUAGE sql AS $$
SELECT pg_advisory_lock_shared(%[2]d);
SELECT pg_advisory_lock(%[2]d, %[3]d);
SELECT pg_advisory_lock(%[4]d);
SELECT pg_advisory_lock(%[4]d);
SELECT 1 $$`, schema, base, base+1, base+2))
	testenv.Exec(t, setup, fmt.Sprintf("CREATE VIEW %[1]s.locks AS SELECT %[1]s.take()", schema))
	return fmt.Sprintf("SELECT * FROM %s.locks", schema)
}

func heldAdvisory(t *testing.T, base int) int64 {
	t.Helper()
	primary := testenv.Connect(t, testenv.NodeURL(testenv.PrimaryPort(t)))
	defer primary.Close(context.Background())
	return testenv.Count(t, primary, "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND objid IN ($1, $2, $3)", base, base+1, base+2)
}

func TestAdvisoryLocksFromViewsAreReleased(t *testing.T) {
	schema := testenv.Schema(t, "advisory")
	base := 700100
	view := Query(advisoryView(t, schema, base))
	d := gatewayDB(t)
	cases := map[string]func(context.Context, *Tx, context.CancelFunc) error{
		"normal": func(ctx context.Context, tx *Tx, _ context.CancelFunc) error { return drain(ctx, tx, view) },
		"error after the view": func(ctx context.Context, tx *Tx, _ context.CancelFunc) error {
			if err := drain(ctx, tx, view); err != nil {
				return err
			}
			return drain(ctx, tx, "SELECT 1/0")
		},
		"cancel after the view": func(ctx context.Context, tx *Tx, _ context.CancelFunc) error {
			if err := drain(ctx, tx, view); err != nil {
				return err
			}
			return drain(ctx, tx, "SELECT pg_sleep(5)")
		},
		"caller gives up after the view": func(ctx context.Context, tx *Tx, cancelCaller context.CancelFunc) error {
			if err := drain(ctx, tx, view); err != nil {
				return err
			}
			time.AfterFunc(300*time.Millisecond, cancelCaller)
			return drain(ctx, tx, "SELECT pg_sleep(5)")
		},
	}
	for name, fn := range cases {
		callerCtx, cancelCaller := context.WithCancel(context.Background())
		report, err := d.Read(callerCtx, Plan{Timeout: 10 * time.Second, CallTimeout: 2 * time.Second}, func(ctx context.Context, tx *Tx) error {
			return fn(ctx, tx, cancelCaller)
		})
		cancelCaller()
		if name == "normal" && err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if report.AdvisoryReleased != 3 || report.SessionTerminated || report.MayHaveLeft {
			t.Fatalf("%s: report %+v err %v", name, report, err)
		}
		if held := heldAdvisory(t, base); held != 0 {
			t.Fatalf("%s: %d advisory locks still held", name, held)
		}
	}
	report, err := d.Write(context.Background(), Plan{Timeout: 10 * time.Second}, func(ctx context.Context, tx *Tx) error { return drain(ctx, tx, view) })
	if err != nil || report.AdvisoryReleased != 3 {
		t.Fatalf("write path: report %+v err %v", report, err)
	}
	if held := heldAdvisory(t, base); held != 0 {
		t.Fatalf("write path: %d advisory locks still held", held)
	}

	control := testenv.Connect(t, testenv.GatewayURL())
	testenv.Count(t, control, string(view))
	control.Close(context.Background())
	held := heldAdvisory(t, base)
	primary := testenv.Connect(t, testenv.NodeURL(testenv.PrimaryPort(t)))
	defer primary.Close(context.Background())
	testenv.Exec(t, primary, "SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype = 'advisory' AND objid IN ($1, $2, $3)", base, base+1, base+2)
	if held != 3 {
		t.Fatalf("control: expected the gateway to keep 3 advisory locks after disconnect, found %d", held)
	}
}

func TestTransactionAdvisoryLockIsNotTreatedAsLeak(t *testing.T) {
	schema := testenv.Schema(t, "xactlock")
	setup := testenv.Connect(t, testenv.GatewayURL())
	defer setup.Close(context.Background())
	testenv.Exec(t, setup, fmt.Sprintf("CREATE FUNCTION %[1]s.take() RETURNS int LANGUAGE sql AS $$ SELECT pg_advisory_xact_lock(700200); SELECT 1 $$", schema))
	testenv.Exec(t, setup, fmt.Sprintf("CREATE VIEW %[1]s.locks AS SELECT %[1]s.take()", schema))
	d := gatewayDB(t)
	report, err := d.Read(context.Background(), Plan{Timeout: 5 * time.Second}, func(ctx context.Context, tx *Tx) error {
		return drain(ctx, tx, Query(fmt.Sprintf("SELECT * FROM %s.locks", schema)))
	})
	if err != nil || report.SessionTerminated || report.MayHaveLeft {
		t.Fatalf("report %+v err %v", report, err)
	}
}

func hypopgCount(ctx context.Context, tx *Tx) (int64, int64, error) {
	return tx.countHypopg(ctx)
}

func TestHypopgCleanupOnOneSession(t *testing.T) {
	d := singleSessionDB(t, testenv.NodeURL(testenv.PrimaryPort(t)), true)
	schema := testenv.Schema(t, "hypo")
	setup := testenv.Connect(t, testenv.GatewayURL())
	defer setup.Close(context.Background())
	testenv.Exec(t, setup, fmt.Sprintf("CREATE TABLE %s.t (a int, b int)", schema))
	definition := fmt.Sprintf("CREATE INDEX ON %s.t (b)", schema)
	plan := Plan{Timeout: 10 * time.Second, CallTimeout: 2 * time.Second, Hypopg: true}
	ends := map[string]func(context.Context, *Tx) error{
		"normal": func(ctx context.Context, tx *Tx) error { return nil },
		"error":  func(ctx context.Context, tx *Tx) error { return drain(ctx, tx, "SELECT 1/0") },
		"cancel": func(ctx context.Context, tx *Tx) error { return drain(ctx, tx, "SELECT pg_sleep(5)") },
	}
	for name, end := range ends {
		report, _ := d.Read(context.Background(), plan, func(ctx context.Context, tx *Tx) error {
			if _, _, err := tx.CreateHypotheticalIndex(ctx, definition); err != nil {
				return err
			}
			return end(ctx, tx)
		})
		if report.SessionTerminated || report.MayHaveLeft || report.HypotheticalAfter != 0 {
			t.Fatalf("%s: report %+v", name, report)
		}
		var left int64
		after, _ := d.Read(context.Background(), Plan{Timeout: 5 * time.Second}, func(ctx context.Context, tx *Tx) error {
			var err error
			left, _, err = hypopgCount(ctx, tx)
			return err
		})
		if after.BackendPID != report.BackendPID {
			t.Fatalf("%s: the check ran on another session (%d, %d)", name, report.BackendPID, after.BackendPID)
		}
		if left != 0 {
			t.Fatalf("%s: %d hypothetical indexes left on the session", name, left)
		}
	}

	endResetTemplate = "SELECT %[1]s.hypopg_reset(), 1/0"
	defer func() { endResetTemplate = hypopgResetTemplate }()
	report, _ := d.Read(context.Background(), plan, func(ctx context.Context, tx *Tx) error {
		_, _, err := tx.CreateHypotheticalIndex(ctx, definition)
		return err
	})
	if !report.SessionTerminated {
		t.Fatalf("reset failure: expected the session to be ended, report %+v", report)
	}
	primary := testenv.Connect(t, testenv.NodeURL(testenv.PrimaryPort(t)))
	defer primary.Close(context.Background())
	if alive := testenv.Count(t, primary, "SELECT count(*) FROM pg_stat_activity WHERE pid = $1", report.BackendPID); alive != 0 {
		t.Fatalf("reset failure: session %d still exists", report.BackendPID)
	}
}

func TestWriteIsNotCommittedWhenCleanupFails(t *testing.T) {
	schema := testenv.Schema(t, "settle")
	setup := testenv.Connect(t, testenv.GatewayURL())
	defer setup.Close(context.Background())
	testenv.Exec(t, setup, fmt.Sprintf("CREATE TABLE %s.t (a int)", schema))
	d := gatewayDB(t)
	endResetTemplate = "SELECT %[1]s.hypopg_reset(), 1/0"
	defer func() { endResetTemplate = hypopgResetTemplate }()
	report, err := d.Write(context.Background(), Plan{Timeout: 5 * time.Second}, func(ctx context.Context, tx *Tx) error {
		if err := drain(ctx, tx, Query(fmt.Sprintf("INSERT INTO %s.t VALUES (1)", schema))); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, Query(tx.hypopg(hypopgCreateTemplate)), fmt.Sprintf("CREATE INDEX ON %s.t (a)", schema))
		if err != nil {
			return err
		}
		rows.Close()
		return rows.Err()
	})
	if err == nil || !strings.Contains(err.Error(), "not committed") {
		t.Fatalf("a write whose cleanup failed returned %v, report %+v", err, report)
	}
	if rows := testenv.Count(t, setup, fmt.Sprintf("SELECT count(*) FROM %s.t", schema)); rows != 0 {
		t.Fatalf("%d rows were committed", rows)
	}
}

func TestSessionIsEndedAfterCleanupUsesItsWholeDeadline(t *testing.T) {
	d := gatewayDB(t)
	endResetTemplate = "SELECT %[1]s.hypopg_reset(), pg_catalog.pg_sleep(8)"
	defer func() { endResetTemplate = hypopgResetTemplate }()
	report, _ := d.Read(context.Background(), Plan{Timeout: 20 * time.Second, Hypopg: true}, func(ctx context.Context, tx *Tx) error { return nil })
	if !report.SessionTerminated || report.MayHaveLeft {
		t.Fatalf("report %+v", report)
	}
	primary := testenv.Connect(t, testenv.NodeURL(testenv.PrimaryPort(t)))
	defer primary.Close(context.Background())
	if alive := testenv.Count(t, primary, "SELECT count(*) FROM pg_stat_activity WHERE pid = $1", report.BackendPID); alive != 0 {
		t.Fatalf("session %d still exists", report.BackendPID)
	}
}

func TestHypopgStateCreatedInsideViewsIsCleared(t *testing.T) {
	d := singleSessionDB(t, testenv.NodeURL(testenv.PrimaryPort(t)), true)
	schema := testenv.Schema(t, "hypoview")
	setup := testenv.Connect(t, testenv.GatewayURL())
	defer setup.Close(context.Background())
	testenv.Exec(t, setup, fmt.Sprintf("CREATE TABLE %s.t (a int PRIMARY KEY, b int)", schema))
	testenv.Exec(t, setup, fmt.Sprintf("CREATE FUNCTION %[1]s.mk() RETURNS oid LANGUAGE sql AS $$ SELECT indexrelid FROM public.hypopg_create_index('CREATE INDEX ON %[1]s.t (b)') $$", schema))
	testenv.Exec(t, setup, fmt.Sprintf("CREATE FUNCTION %[1]s.hide() RETURNS bool LANGUAGE sql AS $$ SELECT public.hypopg_hide_index('%[1]s.t_pkey'::regclass) $$", schema))
	testenv.Exec(t, setup, fmt.Sprintf("CREATE VIEW %[1]s.mk_view AS SELECT %[1]s.mk(); CREATE VIEW %[1]s.hide_view AS SELECT %[1]s.hide()", schema))
	cases := map[string]string{"hypothetical": "mk_view", "hidden": "hide_view"}
	for name, view := range cases {
		report, err := d.Read(context.Background(), Plan{Timeout: 5 * time.Second}, func(ctx context.Context, tx *Tx) error {
			return drain(ctx, tx, Query(fmt.Sprintf("SELECT * FROM %s.%s", schema, view)))
		})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		found := report.HypotheticalFound
		if name == "hidden" {
			found = report.HiddenFound
		}
		if found != 1 || report.HypotheticalAfter != 0 || report.HiddenAfter != 0 || report.SessionTerminated {
			t.Fatalf("%s: report %+v", name, report)
		}
		if !strings.Contains(strings.Join(report.Notes(), "\n"), "hypopg cleanup") {
			t.Fatalf("%s: notes %v", name, report.Notes())
		}
		var left, hiddenLeft int64
		after, _ := d.Read(context.Background(), Plan{Timeout: 5 * time.Second}, func(ctx context.Context, tx *Tx) error {
			var err error
			left, hiddenLeft, err = hypopgCount(ctx, tx)
			return err
		})
		if after.BackendPID != report.BackendPID {
			t.Fatalf("%s: the check ran on another session (%d, %d)", name, report.BackendPID, after.BackendPID)
		}
		if left != 0 || hiddenLeft != 0 {
			t.Fatalf("%s: %d hypothetical and %d hidden indexes left on the session", name, left, hiddenLeft)
		}
	}
}

func TestServerCancelNeedsTheSessionStart(t *testing.T) {
	d := gatewayDB(t)
	sleep := func(target func(session) session) (string, time.Duration) {
		start := time.Now()
		_, err := d.Read(context.Background(), Plan{Timeout: 10 * time.Second}, func(ctx context.Context, tx *Tx) error {
			go func() {
				time.Sleep(300 * time.Millisecond)
				_ = d.canceler.cancelSession(context.Background(), target(tx.session))
			}()
			return drain(ctx, tx, "SELECT pg_sleep(1.5)")
		})
		return sqlState(err), time.Since(start)
	}
	if code, took := sleep(func(s session) session { return s }); code != "57014" || took > time.Second {
		t.Fatalf("matching session: state %q after %v", code, took)
	}
	if code, took := sleep(func(s session) session { s.start = s.start.Add(-time.Second); return s }); code != "" || took < 1500*time.Millisecond {
		t.Fatalf("another start time: state %q after %v", code, took)
	}
}

func TestTerminateNeedsTheSessionStart(t *testing.T) {
	d := gatewayDB(t)
	other := testenv.Connect(t, testenv.NodeURL(testenv.PrimaryPort(t)))
	defer other.Close(context.Background())
	var target session
	if err := other.QueryRow(context.Background(), "SELECT pid, backend_start FROM pg_stat_activity WHERE pid = pg_backend_pid()").Scan(&target.pid, &target.start); err != nil {
		t.Fatal(err)
	}
	target.start = target.start.Add(-time.Second)
	_, err := d.Read(context.Background(), Plan{Timeout: 5 * time.Second}, func(ctx context.Context, tx *Tx) error {
		stranger := *tx
		stranger.session = target
		return stranger.terminateSession(ctx)
	})
	if err == nil {
		t.Fatal("a session that was not found was counted as ended")
	}
	if err := other.Ping(context.Background()); err != nil {
		t.Fatalf("a session with another start time was ended: %v", err)
	}
}

func TestStatementTimeoutAndCancelEndTheQuery(t *testing.T) {
	d := gatewayDB(t)
	primary := testenv.Connect(t, testenv.NodeURL(testenv.PrimaryPort(t)))
	defer primary.Close(context.Background())
	sleeping := func() int64 {
		return testenv.Count(t, primary, "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND state = 'active' AND query LIKE '%pg_sleep%'", ApplicationName)
	}
	start := time.Now()
	_, err := d.Read(context.Background(), Plan{Timeout: 500 * time.Millisecond}, func(ctx context.Context, tx *Tx) error {
		return drain(ctx, tx, "SELECT pg_sleep(5)")
	})
	if sqlState(err) != "57014" || time.Since(start) > 2500*time.Millisecond {
		t.Fatalf("statement timeout: err %v after %v", err, time.Since(start))
	}
	if n := sleeping(); n != 0 {
		t.Fatalf("statement timeout: %d sleeping sessions remain", n)
	}

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			if sleeping() > 0 {
				close(started)
				cancel()
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		close(started)
		cancel()
	}()
	start = time.Now()
	_, err = d.Read(ctx, Plan{Timeout: 30 * time.Second}, func(ctx context.Context, tx *Tx) error {
		return drain(ctx, tx, "SELECT pg_sleep(20)")
	})
	<-started
	if err == nil || time.Since(start) > 7*time.Second {
		t.Fatalf("cancel: err %v after %v", err, time.Since(start))
	}
	time.Sleep(300 * time.Millisecond)
	if n := sleeping(); n != 0 {
		t.Fatalf("cancel: %d sleeping sessions remain", n)
	}
}

func TestCancelIsSentAgainWhenTheStatementSurvivesIt(t *testing.T) {
	schema := testenv.Schema(t, "cancel")
	setup := testenv.Connect(t, testenv.GatewayURL())
	defer setup.Close(context.Background())
	testenv.Exec(t, setup, fmt.Sprintf(`CREATE FUNCTION %s.survive_one_cancel() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
	BEGIN
		PERFORM pg_catalog.pg_sleep(10);
	EXCEPTION WHEN query_canceled THEN
		NULL;
	END;
	PERFORM pg_catalog.pg_sleep(10);
	RETURN 1;
END $$`, schema))
	primary := testenv.Connect(t, testenv.NodeURL(testenv.PrimaryPort(t)))
	defer primary.Close(context.Background())
	d := gatewayDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := d.Read(ctx, Plan{Timeout: 30 * time.Second}, func(ctx context.Context, tx *Tx) error {
			return drain(ctx, tx, Query(fmt.Sprintf("SELECT %s.survive_one_cancel()", schema)))
		})
		done <- err
	}()
	deadline := time.Now().Add(10 * time.Second)
	for testenv.Count(t, primary, "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1 AND wait_event = 'PgSleep' AND query LIKE '%survive_one_cancel%'", ApplicationName) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the function never started sleeping")
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	cancelled := time.Now()
	err := <-done
	if took := time.Since(cancelled); sqlState(err) != "57014" || took > 2*time.Second {
		t.Fatalf("a statement that survived the first cancel ended with %v after %v", err, took)
	}
}

func TestReadNodeConnectsToThatNode(t *testing.T) {
	primaryPort := testenv.PrimaryPort(t)
	for _, port := range testenv.NodePorts() {
		var recovering bool
		err := ReadNode(context.Background(), testenv.GatewayURL(), testenv.NodeHost(), port, Plan{Timeout: 5 * time.Second}, func(ctx context.Context, tx *Tx) error {
			return tx.QueryRow(ctx, "SELECT pg_is_in_recovery()").Scan(&recovering)
		})
		if err != nil {
			t.Fatalf("port %d: %v", port, err)
		}
		if recovering != (port != primaryPort) {
			t.Fatalf("port %d: pg_is_in_recovery() = %t, so the connection did not reach that node", port, recovering)
		}
	}
}
