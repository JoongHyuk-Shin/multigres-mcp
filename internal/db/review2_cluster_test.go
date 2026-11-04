//go:build cluster

package db

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/testenv"
)

func TestWriteCreatesUnqualifiedObjectsWhereTheSessionWould(t *testing.T) {
	name := fmt.Sprintf("mcp_sp_%d", time.Now().UnixNano())
	setup := testenv.Connect(t, testenv.GatewayURL())
	t.Cleanup(func() { setup.Close(context.Background()) })
	t.Cleanup(func() { testenv.Exec(t, setup, "DROP TABLE IF EXISTS "+name) })
	var want string
	if err := setup.QueryRow(context.Background(), "SELECT pg_catalog.current_schema()").Scan(&want); err != nil {
		t.Fatal(err)
	}
	d := gatewayDB(t)
	_, err := d.Write(context.Background(), Plan{Timeout: 5 * time.Second}, func(ctx context.Context, tx *Tx) error {
		return drain(ctx, tx, Query("CREATE TABLE "+name+" (a int)"))
	})
	if err != nil {
		t.Fatalf("CREATE TABLE without a schema failed: %v", err)
	}
	var got string
	if err := setup.QueryRow(context.Background(), "SELECT n.nspname FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = $1", name).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("the table landed in %s, but a plain session creates it in %s", got, want)
	}
}

func TestWriteRejectsATemporaryCollationMadeInsideAFunction(t *testing.T) {
	schema := testenv.Schema(t, "tempkinds")
	collation := fmt.Sprintf("mcp_c_%d", time.Now().UnixNano())
	setup := testenv.Connect(t, testenv.GatewayURL())
	defer setup.Close(context.Background())
	testenv.Exec(t, setup, fmt.Sprintf(`CREATE FUNCTION %s.make_collation() RETURNS void LANGUAGE plpgsql AS $f$BEGIN EXECUTE 'CREATE COLLATION pg_temp.%s (locale = ''C'')'; END$f$`, schema, collation))
	d := gatewayDB(t)
	_, err := d.Write(context.Background(), Plan{Timeout: 5 * time.Second}, func(ctx context.Context, tx *Tx) error {
		return drain(ctx, tx, Query(fmt.Sprintf("SELECT %s.make_collation()", schema)))
	})
	if err == nil || !strings.Contains(err.Error(), "temporary schema") {
		t.Fatalf("a temporary collation made inside a function returned %v", err)
	}
	primary := testenv.Connect(t, testenv.NodeURL(testenv.PrimaryPort(t)))
	defer primary.Close(context.Background())
	if left := testenv.Count(t, primary, "SELECT count(*) FROM pg_catalog.pg_collation WHERE collname = $1", collation); left != 0 {
		t.Fatalf("%d collations named %s were left", left, collation)
	}
}

func TestGlobalDefaultPrivilegesAreNotTakenForTemporaryObjects(t *testing.T) {
	role := fmt.Sprintf("mcp_dp_%d", time.Now().UnixNano())
	setup := testenv.Connect(t, testenv.GatewayURL())
	t.Cleanup(func() { setup.Close(context.Background()) })
	testenv.Exec(t, setup, "CREATE ROLE "+role)
	t.Cleanup(func() { testenv.Exec(t, setup, fmt.Sprintf("DROP OWNED BY %[1]s; DROP ROLE %[1]s", role)) })
	d := singleSessionDB(t, testenv.NodeURL(testenv.PrimaryPort(t)), false)
	var temporarySchema uint32
	_, err := d.Write(context.Background(), Plan{Timeout: 5 * time.Second}, func(ctx context.Context, tx *Tx) error {
		if err := tx.QueryRow(ctx, "SELECT pg_catalog.pg_my_temp_schema()::int8").Scan(&temporarySchema); err != nil {
			return err
		}
		return drain(ctx, tx, Query(fmt.Sprintf("ALTER DEFAULT PRIVILEGES FOR ROLE %s GRANT SELECT ON TABLES TO PUBLIC", role)))
	})
	if temporarySchema != 0 {
		t.Fatalf("the new session already has temporary schema %d, so this test cannot see the global row", temporarySchema)
	}
	if err != nil {
		t.Fatalf("a global default privilege was taken for a temporary object: %v", err)
	}
}

func TestTemporaryCountCoversEveryNamespaceCatalog(t *testing.T) {
	conn := testenv.Connect(t, testenv.GatewayURL())
	defer conn.Close(context.Background())
	rows, err := conn.Query(context.Background(), `SELECT c.relname, a.attname
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'pg_catalog' AND c.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped
  AND a.atttypid = 'pg_catalog.oid'::pg_catalog.regtype AND a.attname LIKE '%namespace'
ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	catalogs := 0
	for rows.Next() {
		var table, column string
		if err := rows.Scan(&table, &column); err != nil {
			t.Fatal(err)
		}
		catalogs++
		branch := regexp.MustCompile(`SELECT ` + column + `( AS namespace)? FROM pg_catalog\.` + table + `\b`)
		if !branch.MatchString(string(temporaryQuery)) {
			t.Errorf("the temporary object count does not read %s.%s", table, column)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if catalogs < 16 {
		t.Fatalf("the catalog query found %d namespace columns; 16 were measured on this cluster", catalogs)
	}
}

func TestReadFailsWhenNeitherCleanupNorEndingTheSessionWorks(t *testing.T) {
	d := gatewayDB(t)
	endResetTemplate = "SELECT %[1]s.hypopg_reset(), 1/0"
	endSessionQuery = "SELECT false FROM pg_catalog.pg_stat_activity a WHERE a.pid = $1 AND a.backend_start = $2"
	defer func() { endResetTemplate, endSessionQuery = hypopgResetTemplate, terminateQuery }()
	report, err := d.Read(context.Background(), Plan{Timeout: 5 * time.Second, Hypopg: true}, func(ctx context.Context, tx *Tx) error { return nil })
	notes := strings.Join(report.Notes(), "\n")
	if err == nil || !strings.Contains(err.Error(), "the session could not be ended either") {
		t.Fatalf("a read whose cleanup and session end both failed returned %v\nnotes:\n%s", err, notes)
	}
	if !strings.Contains(notes, "also failed while the connection was open") {
		t.Fatalf("notes do not say the connection was open:\n%s", notes)
	}
	if strings.Contains(notes, "connection was lost") || strings.Contains(notes, "indexes left on this session") {
		t.Fatalf("notes claim a lost connection or a count that was not taken:\n%s", notes)
	}
}

func TestFailedInnerTryDoesNotShieldTheOuterOne(t *testing.T) {
	d := gatewayDB(t)
	var before, after string
	_, err := d.Read(context.Background(), Plan{Timeout: 5 * time.Second}, func(ctx context.Context, tx *Tx) error {
		if err := tx.QueryRow(ctx, "SHOW work_mem").Scan(&before); err != nil {
			return err
		}
		outer := tx.Try(ctx, func() error {
			if _, err := tx.conn.Exec(ctx, "SET LOCAL work_mem = '7MB'"); err != nil {
				return err
			}
			_ = tx.Try(ctx, func() error { return drain(ctx, tx, "SELECT 1/0") })
			return errors.New("the outer step failed")
		})
		if outer == nil || outer.Error() != "the outer step failed" {
			return fmt.Errorf("the outer Try returned %v", outer)
		}
		return tx.QueryRow(ctx, "SHOW work_mem").Scan(&after)
	})
	if err != nil {
		t.Fatal(err)
	}
	if before == "7MB" || after != before {
		t.Fatalf("work_mem was %s before and %s after the outer Try rolled back", before, after)
	}
}

func TestCommitWithoutAReplyIsReportedAsUnknown(t *testing.T) {
	schema := testenv.Schema(t, "commitlost")
	setup := testenv.Connect(t, testenv.GatewayURL())
	defer setup.Close(context.Background())
	testenv.Exec(t, setup, fmt.Sprintf("CREATE TABLE %s.t (a int)", schema))
	d := throughDropper(t, string(commit))
	_, err := d.Write(context.Background(), Plan{Timeout: 5 * time.Second}, func(ctx context.Context, tx *Tx) error {
		return drain(ctx, tx, Query(fmt.Sprintf("INSERT INTO %s.t VALUES (1)", schema)))
	})
	if err == nil || !strings.Contains(err.Error(), "whether the write was committed is unknown") {
		t.Fatalf("a COMMIT without a reply returned %v", err)
	}
	if rows := testenv.Count(t, setup, fmt.Sprintf("SELECT count(*) FROM %s.t", schema)); rows != 1 {
		t.Fatalf("%d rows after the lost COMMIT reply; the dropped reply was not for a committed write", rows)
	}
}

func TestRollbackFailureKeepsTheOriginalError(t *testing.T) {
	d := throughDropper(t, string(rollback))
	_, err := d.Write(context.Background(), Plan{Timeout: 5 * time.Second}, func(ctx context.Context, tx *Tx) error {
		return drain(ctx, tx, "SELECT 1/0")
	})
	if err == nil || !strings.Contains(err.Error(), "division by zero") || !strings.Contains(err.Error(), "ROLLBACK failed") {
		t.Fatalf("a failed statement whose ROLLBACK got no reply returned %v", err)
	}
}

func throughDropper(t *testing.T, statement string) *DB {
	t.Helper()
	gatewayURL, err := url.Parse(testenv.GatewayURL())
	if err != nil {
		t.Fatal("the gateway URL does not parse")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			go relayUntil(client, gatewayURL.Host, statement)
		}
	}()
	proxied := *gatewayURL
	proxied.Host = listener.Addr().String()
	query := proxied.Query()
	query.Set("sslmode", "disable")
	proxied.RawQuery = query.Encode()
	d, err := Open(Options{URL: proxied.String(), Writable: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(d.Close)
	return d
}

func relayUntil(client net.Conn, target, statement string) {
	defer client.Close()
	server, err := net.Dial("tcp", target)
	if err != nil {
		return
	}
	defer server.Close()
	var dropping atomic.Bool
	go func() {
		buffer := make([]byte, 32*1024)
		for {
			n, err := server.Read(buffer)
			if n > 0 && !dropping.Load() {
				if _, err := client.Write(buffer[:n]); err != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
	startup, err := readFrame(client, false)
	if err != nil {
		return
	}
	if _, err := server.Write(startup); err != nil {
		return
	}
	for {
		frame, err := readFrame(client, true)
		if err != nil {
			return
		}
		if frame[0] == 'Q' && strings.TrimRight(string(frame[5:]), "\x00") == statement {
			dropping.Store(true)
			_, _ = server.Write(frame)
			time.Sleep(500 * time.Millisecond)
			return
		}
		if _, err := server.Write(frame); err != nil {
			return
		}
	}
}

func readFrame(conn net.Conn, typed bool) ([]byte, error) {
	header := make([]byte, 4)
	if typed {
		header = make([]byte, 5)
	}
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header[len(header)-4:])
	if length < 4 {
		return nil, fmt.Errorf("message length %d", length)
	}
	frame := make([]byte, len(header)+int(length)-4)
	copy(frame, header)
	_, err := io.ReadFull(conn, frame[len(header):])
	return frame, err
}
