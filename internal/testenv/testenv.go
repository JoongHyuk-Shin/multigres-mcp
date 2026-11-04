package testenv

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func GatewayURL() string {
	return get("MULTIGRES_MCP_TEST_GATEWAY_URL", "postgres://postgres:postgres@localhost:15432/postgres")
}

func AdminURL() string {
	return get("MULTIGRES_MCP_TEST_ADMIN_URL", "http://localhost:15000")
}

func NodeHost() string {
	return get("MULTIGRES_MCP_TEST_NODE_HOST", "172.18.0.2")
}

func NodePorts() []int {
	return []int{25432, 25433}
}

func NodeURL(port int) string {
	return fmt.Sprintf("postgres://postgres:postgres@%s:%d/postgres", NodeHost(), port)
}

func PrimaryPort(t testing.TB) int {
	t.Helper()
	for _, port := range NodePorts() {
		conn := Connect(t, NodeURL(port))
		var recovering bool
		err := conn.QueryRow(context.Background(), "SELECT pg_is_in_recovery()").Scan(&recovering)
		conn.Close(context.Background())
		if err == nil && !recovering {
			return port
		}
	}
	t.Fatal("no primary node found on the test node ports")
	return 0
}

func Connect(t testing.TB, url string) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("cannot reach the test cluster at %s: %v", url, err)
	}
	return conn
}

func Exec(t testing.TB, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func Count(t testing.TB, conn *pgx.Conn, sql string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := conn.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	return n
}

func Schema(t testing.TB, prefix string) string {
	t.Helper()
	name := prefix + "_" + strconv.FormatInt(time.Now().UnixNano()%1_000_000_000, 36)
	setup := Connect(t, GatewayURL())
	Exec(t, setup, "CREATE SCHEMA "+name)
	setup.Close(context.Background())
	t.Cleanup(func() {
		teardown := Connect(t, GatewayURL())
		defer teardown.Close(context.Background())
		Exec(t, teardown, "DROP SCHEMA "+name+" CASCADE")
	})
	return name
}

func get(key string, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
