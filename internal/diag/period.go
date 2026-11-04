package diag

import (
	"context"
	"time"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/db"
)

func Period(ctx context.Context, tx *db.Tx, sink Sink, answeredBy string, describe func(error) string) {
	sink.Line("statistics period (judge for yourself whether it is long enough):")
	var replica bool
	if err := tx.Try(ctx, func() error { return tx.QueryRow(ctx, roleQuery).Scan(&replica) }); err != nil {
		sink.Line("- answered by: %s (role unknown: %s)", answeredBy, describe(err))
	} else {
		role := "primary"
		if replica {
			role = "replica"
		}
		sink.Line("- answered by: %s, role: %s; cumulative statistics are kept per node", answeredBy, role)
	}
	err := tx.Try(ctx, func() error {
		var term int64
		var leader *string
		var at time.Time
		if err := tx.QueryRow(ctx, latestPromotionQuery).Scan(&term, &leader, &at); err != nil {
			return err
		}
		sink.Line("- latest promotion in multigres.rule_history: term %d, %s at %s; the new primary's statistics start around then (whether this row's time equals the actual promotion moment is not confirmed)", term, deref(leader), at.UTC().Format(time.RFC3339))
		return nil
	})
	if err != nil {
		sink.Line("- latest promotion: unknown (%s)", describe(err))
	}
	var schema string
	err = tx.Try(ctx, func() error {
		var lookupErr error
		schema, lookupErr = ExtensionSchema(ctx, tx, "pg_stat_statements")
		return lookupErr
	})
	switch {
	case err != nil:
		sink.Line("- pg_stat_statements reset: unknown (%s)", describe(err))
	case schema == "":
		sink.Line("- pg_stat_statements reset: the extension is not installed in this database")
	default:
		sink.Line("- pg_stat_statements reset: %s", timestamp(ctx, tx, withSchema(statementsResetQuery, schema), describe))
	}
	sink.Line("- database statistics reset: %s", timestamp(ctx, tx, databaseResetQuery, describe))
	var restarted int64
	if err := tx.Try(ctx, func() error { return tx.QueryRow(ctx, restartedQuery).Scan(&restarted) }); err != nil {
		sink.Line("- tables whose cumulative row count is 0 while the catalog has rows: unknown (%s)", describe(err))
	} else {
		sink.Line("- tables whose cumulative row count is 0 while the catalog has rows: %d (Postgres counts only writes made on that node and discards its counts when it starts with crash recovery, so a large number means this node's counts miss part of the history, and 0 does not mean nothing is missing)", restarted)
	}
}

func timestamp(ctx context.Context, tx *db.Tx, query db.Query, describe func(error) string) string {
	var value *time.Time
	err := tx.Try(ctx, func() error { return tx.QueryRow(ctx, query).Scan(&value) })
	switch {
	case err != nil:
		return "unknown (" + describe(err) + ")"
	case value == nil:
		return "never"
	default:
		return value.UTC().Format(time.RFC3339)
	}
}

func deref(text *string) string {
	if text == nil {
		return "(no leader)"
	}
	return *text
}
