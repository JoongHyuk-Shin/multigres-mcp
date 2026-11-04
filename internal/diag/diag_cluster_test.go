//go:build cluster

package diag

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/db"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/testenv"
)

func TestExtensionSchemaReportsFailedLookups(t *testing.T) {
	d, err := db.Open(db.Options{URL: testenv.GatewayURL()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	_, err = d.Read(context.Background(), db.Plan{Timeout: 5 * time.Second}, func(ctx context.Context, tx *db.Tx) error {
		if schema, err := ExtensionSchema(ctx, tx, "pg_stat_statements"); schema == "" || err != nil {
			t.Errorf("installed extension: schema %q, error %v", schema, err)
		}
		if schema, err := ExtensionSchema(ctx, tx, "no_such_extension"); schema != "" || err != nil {
			t.Errorf("missing extension: schema %q, error %v", schema, err)
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := ExtensionSchema(cancelled, tx, "pg_stat_statements"); err == nil {
			t.Error("a lookup that never ran was reported as a missing extension")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

type lines []string

func (l *lines) Line(format string, args ...any) bool {
	*l = append(*l, fmt.Sprintf(format, args...))
	return true
}

func (l *lines) JSON(label string, value any) bool { return true }

type fullSink struct{ full bool }

func (s *fullSink) Line(format string, args ...any) bool { return !s.full }

func (s *fullSink) JSON(label string, value any) bool {
	s.full = true
	return false
}

func TestChecksStopOnceTheResultIsFull(t *testing.T) {
	var ran []string
	record := func(name string) func(context.Context, *db.Tx, Sink, Callbacks) error {
		return func(ctx context.Context, tx *db.Tx, sink Sink, cb Callbacks) error {
			ran = append(ran, name)
			sink.JSON("rows", nil)
			return nil
		}
	}
	defer func(saved []check) { checks = saved }(checks)
	checks = []check{{"fills", record("fills")}, {"after", record("after")}}
	d, err := db.Open(db.Options{URL: testenv.GatewayURL()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	_, err = d.Read(context.Background(), db.Plan{Timeout: 5 * time.Second}, func(ctx context.Context, tx *db.Tx) error {
		return Run(ctx, tx, nil, &fullSink{}, Callbacks{CheckTimeout: 5 * time.Second, Describe: func(err error) string { return err.Error() }})
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(ran, ",") != "fills" {
		t.Fatalf("checks that ran after the result was full: %v", ran)
	}
}

func TestPeriodLeavesTheTransactionUsableWhenItsLookupsFail(t *testing.T) {
	roleQuery, extensionLookupQuery, restartedQuery = "SELECT (1/0) = 1", "SELECT $1::text || (1/0)::text", "SELECT (1/0)::int8"
	defer func() {
		roleQuery, extensionLookupQuery, restartedQuery = nodeRoleQuery, extensionSchemaQuery, restartedTablesQuery
	}()
	d, err := db.Open(db.Options{URL: testenv.GatewayURL()})
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	var period lines
	_, err = d.Read(context.Background(), db.Plan{Timeout: 5 * time.Second}, func(ctx context.Context, tx *db.Tx) error {
		Period(ctx, tx, &period, "test node", func(err error) string { return err.Error() })
		var one int
		return tx.QueryRow(ctx, "SELECT 1").Scan(&one)
	})
	text := strings.Join(period, "\n")
	if err != nil {
		t.Fatalf("the statement after the statistics period failed: %v\n%s", err, text)
	}
	for _, want := range []string{"role unknown: ", "pg_stat_statements reset: unknown (", "while the catalog has rows: unknown ("} {
		if !strings.Contains(text, want) {
			t.Fatalf("the period does not report the failed lookup %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "database statistics reset: unknown") {
		t.Fatalf("a lookup that was not made to fail could not run:\n%s", text)
	}
}
