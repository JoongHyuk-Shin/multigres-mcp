package db

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/sqlguard"
)

type control string

const (
	lockTimeoutValue = "2s"
	searchPathValue  = `pg_catalog, "$user", public`
)

const (
	beginReadOnly control = "BEGIN TRANSACTION READ ONLY"
	beginWrite    control = "BEGIN"
	rollback      control = "ROLLBACK"
	commit        control = "COMMIT"
	searchPath    control = "SET LOCAL search_path = " + searchPathValue
	lockTimeout   control = "SET LOCAL lock_timeout = '" + lockTimeoutValue + "'"
	toolSavepoint control = "SAVEPOINT multigres_mcp_tool"
	toolRollback  control = "ROLLBACK TO SAVEPOINT multigres_mcp_tool"
	trySavepoint  control = "SAVEPOINT multigres_mcp_try"
	tryRollback   control = "ROLLBACK TO SAVEPOINT multigres_mcp_try"
	tryRelease    control = "RELEASE SAVEPOINT multigres_mcp_try"
)

type Query string

const (
	sessionQuery Query = `SELECT pg_catalog.pg_backend_pid(),
       (SELECT a.backend_start FROM pg_catalog.pg_stat_activity a WHERE a.pid = pg_catalog.pg_backend_pid()),
       (SELECT n.nspname FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = 'hypopg'),
       CASE WHEN pg_catalog.pg_has_role('pg_read_all_settings', 'USAGE') THEN pg_catalog.current_setting('data_directory') END,
       pg_catalog.current_setting('statement_timeout')`
	advisoryCountQuery  Query = "SELECT pg_catalog.count(*) FROM pg_catalog.pg_locks WHERE locktype = 'advisory' AND pid = pg_catalog.pg_backend_pid()"
	advisoryUnlockQuery Query = "SELECT pg_catalog.pg_advisory_unlock_all()"
	terminateQuery      Query = `SELECT pg_catalog.pg_terminate_backend(a.pid, 3000)
FROM pg_catalog.pg_stat_activity a
WHERE a.pid = $1 AND a.backend_start = $2`
	temporaryQuery Query = `SELECT pg_catalog.count(*) FROM (
          SELECT relnamespace AS namespace FROM pg_catalog.pg_class
UNION ALL SELECT collnamespace FROM pg_catalog.pg_collation
UNION ALL SELECT connamespace FROM pg_catalog.pg_constraint
UNION ALL SELECT connamespace FROM pg_catalog.pg_conversion
UNION ALL SELECT defaclnamespace FROM pg_catalog.pg_default_acl
UNION ALL SELECT extnamespace FROM pg_catalog.pg_extension
UNION ALL SELECT opcnamespace FROM pg_catalog.pg_opclass
UNION ALL SELECT oprnamespace FROM pg_catalog.pg_operator
UNION ALL SELECT opfnamespace FROM pg_catalog.pg_opfamily
UNION ALL SELECT pronamespace FROM pg_catalog.pg_proc
UNION ALL SELECT stxnamespace FROM pg_catalog.pg_statistic_ext
UNION ALL SELECT cfgnamespace FROM pg_catalog.pg_ts_config
UNION ALL SELECT dictnamespace FROM pg_catalog.pg_ts_dict
UNION ALL SELECT prsnamespace FROM pg_catalog.pg_ts_parser
UNION ALL SELECT tmplnamespace FROM pg_catalog.pg_ts_template
UNION ALL SELECT typnamespace FROM pg_catalog.pg_type) objects
WHERE namespace = pg_catalog.pg_my_temp_schema() AND namespace <> 0`
	hypopgCountTemplate  Query = "SELECT (SELECT pg_catalog.count(*) FROM %[1]s.hypopg_list_indexes), (SELECT pg_catalog.count(*) FROM %[1]s.hypopg_hidden_indexes)"
	hypopgResetTemplate  Query = "SELECT %[1]s.hypopg_reset(), %[1]s.hypopg_unhide_all_indexes()"
	hypopgCreateTemplate Query = "SELECT indexrelid, indexname FROM %[1]s.hypopg_create_index($1)"
	hypopgSizeTemplate   Query = "SELECT %[1]s.hypopg_relation_size($1)"
	shadowQuery          Query = `SELECT DISTINCT p.proname, n.nspname
FROM pg_catalog.pg_proc p
JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE p.proname = ANY($1) AND n.nspname <> 'pg_catalog'
ORDER BY 1, 2`
)

var (
	endResetTemplate = hypopgResetTemplate
	sizeTemplate     = hypopgSizeTemplate
	endSessionQuery  = terminateQuery
)

type Tx struct {
	conn           *pgx.Conn
	plan           Plan
	session        session
	hypopgSchema   string
	dataDirectory  string
	canceler       *canceler
	temporary      int64
	settings       string
	sessionTimeout string
}

func (t *Tx) BackendPID() int32 { return t.session.pid }

func (t *Tx) Settings() string { return t.settings }

func (t *Tx) SessionTimeout() string { return t.sessionTimeout }

func (t *Tx) DataDirectory() string { return t.dataDirectory }

func (t *Tx) HasHypopg() bool { return t.hypopgSchema != "" }

func (t *Tx) Query(ctx context.Context, q Query, args ...any) (pgx.Rows, error) {
	return t.conn.Query(ctx, string(q), args...)
}

func (t *Tx) QueryRow(ctx context.Context, q Query, args ...any) pgx.Row {
	return t.conn.QueryRow(ctx, string(q), args...)
}

func (t *Tx) QueryChecked(ctx context.Context, statement sqlguard.Statement) (pgx.Rows, error) {
	return t.queryText(ctx, statement.Text())
}

func (t *Tx) queryText(ctx context.Context, text string) (pgx.Rows, error) {
	return t.conn.Query(ctx, text, pgx.QueryExecModeExec, pgx.QueryResultFormats{pgx.TextFormatCode})
}

func (t *Tx) ExplainGeneric(ctx context.Context, checked *sqlguard.Checked, json bool) ([]string, error) {
	text, sent, err := checked.GenericPlanText(json)
	if err != nil {
		return nil, err
	}
	if err := t.CheckShadowing(ctx, sent); err != nil {
		return nil, err
	}
	results, err := t.conn.PgConn().Exec(ctx, text).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(results) != 1 {
		return nil, fmt.Errorf("expected one result from the generic plan, got %d", len(results))
	}
	lines := make([]string, 0, len(results[0].Rows))
	for _, row := range results[0].Rows {
		if len(row) != 1 {
			return nil, fmt.Errorf("expected one column in the generic plan, got %d", len(row))
		}
		lines = append(lines, string(row[0]))
	}
	return lines, nil
}

func (t *Tx) Try(ctx context.Context, fn func() error) error {
	if _, err := t.conn.Exec(ctx, string(trySavepoint)); err != nil {
		return err
	}
	fnErr := fn()
	cleanupCtx, cancel := detached(ctx)
	defer cancel()
	if fnErr != nil {
		for _, statement := range []control{tryRollback, tryRelease} {
			if _, err := t.conn.Exec(cleanupCtx, string(statement)); err != nil {
				return errors.Join(fnErr, err)
			}
		}
		return fnErr
	}
	_, err := t.conn.Exec(cleanupCtx, string(tryRelease))
	return err
}

func (t *Tx) CreateHypotheticalIndex(ctx context.Context, definition string) (uint32, string, error) {
	if !t.plan.Hypopg {
		return 0, "", errors.New("this transaction was not opened for hypothetical indexes")
	}
	if !t.HasHypopg() {
		return 0, "", ErrNoHypopg
	}
	var oid uint32
	var name string
	err := t.conn.QueryRow(ctx, t.hypopg(hypopgCreateTemplate), definition).Scan(&oid, &name)
	return oid, name, err
}

func (t *Tx) HypotheticalIndexSize(ctx context.Context, oid uint32) (int64, error) {
	if !t.plan.Hypopg || !t.HasHypopg() {
		return 0, errors.New("this transaction has no hypothetical indexes to measure")
	}
	var size int64
	err := t.conn.QueryRow(ctx, t.hypopg(sizeTemplate), oid).Scan(&size)
	return size, err
}

func (t *Tx) CheckShadowing(ctx context.Context, checked *sqlguard.Checked) error {
	names := checked.UnqualifiedFunctions()
	if len(names) == 0 {
		return nil
	}
	rows, err := t.conn.Query(ctx, string(shadowQuery), names)
	if err != nil {
		return err
	}
	defer rows.Close()
	var found []string
	for rows.Next() {
		var name, schema string
		if err := rows.Scan(&name, &schema); err != nil {
			return err
		}
		found = append(found, schema+"."+name)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(found) > 0 {
		return fmt.Errorf("the statement was not sent: functions with the same name exist outside pg_catalog (%s); write the built-in function with the pg_catalog. prefix", strings.Join(found, ", "))
	}
	return nil
}

func (t *Tx) ResetHypothetical(ctx context.Context) error {
	if !t.plan.Hypopg || !t.HasHypopg() {
		return errors.New("this transaction has no hypothetical indexes to reset")
	}
	_, err := t.conn.Exec(ctx, t.hypopg(hypopgResetTemplate))
	return err
}

func (t *Tx) hypopg(template Query) string {
	return fmt.Sprintf(string(template), pgx.Identifier{t.hypopgSchema}.Sanitize())
}

var ErrNoHypopg = errors.New("the hypopg extension is not installed in this database (it is not in the Multigres docker image; install the package on every node and run CREATE EXTENSION hypopg)")

func (d *DB) run(ctx context.Context, pool *pgxpool.Pool, begin control, finish control, plan Plan, fn func(context.Context, *Tx) error) (Report, error) {
	pooled, tx, err := d.acquire(ctx, pool, begin, finish, plan)
	if err != nil {
		return Report{}, err
	}
	conn := pooled.Conn()
	report := Report{BackendPID: tx.session.pid, Hypopg: plan.Hypopg}
	callCtx, cancel := context.WithTimeout(ctx, plan.callTimeout())
	fnErr := fn(callCtx, tx)
	if fnErr == nil && finish == commit {
		fnErr = tx.checkTemporary(callCtx)
	}
	cancel()
	cleanupCtx, cancelCleanup := detached(ctx)
	defer cancelCleanup()
	if err := tx.settle(cleanupCtx, fnErr != nil || finish == rollback, &report); err != nil {
		report.CleanupError = err
		terminateErr := tx.terminate(ctx, &report)
		report.MayHaveLeft = terminateErr != nil
		report.ConnectionLost = conn.IsClosed()
		hand(pooled, false)
		return report, cleanupFailure(fnErr, err, terminateErr, finish == commit)
	}
	end := finish
	if fnErr != nil {
		end = rollback
	}
	if _, err := conn.Exec(cleanupCtx, string(end)); err != nil {
		hand(pooled, false)
		if end == commit {
			return report, fmt.Errorf("COMMIT did not return a success reply, so whether the write was committed is unknown; check the data before running it again: %w", err)
		}
		return report, errors.Join(fnErr, fmt.Errorf("ROLLBACK failed: %w", err))
	}
	hand(pooled, true)
	return report, fnErr
}

func cleanupFailure(fnErr, cleanupErr, terminateErr error, committing bool) error {
	errs := []error{fnErr}
	switch {
	case committing:
		errs = append(errs, fmt.Errorf("cleanup failed, so the transaction was not committed: %w", cleanupErr))
	case terminateErr != nil:
		errs = append(errs, fmt.Errorf("cleanup failed: %w", cleanupErr))
	}
	if terminateErr != nil {
		errs = append(errs, fmt.Errorf("the session could not be ended either: %w", terminateErr))
	}
	return errors.Join(errs...)
}

func (d *DB) acquire(ctx context.Context, pool *pgxpool.Pool, begin control, finish control, plan Plan) (*pgxpool.Conn, *Tx, error) {
	openCtx, cancel := context.WithTimeout(ctx, openTimeout)
	defer cancel()
	pooled, err := pool.Acquire(openCtx)
	if err != nil {
		return nil, nil, err
	}
	tx, err := open(openCtx, pooled.Conn(), begin, plan)
	if err == nil && finish == commit {
		err = tx.QueryRow(openCtx, temporaryQuery).Scan(&tx.temporary)
	}
	if err != nil {
		hand(pooled, false)
		return nil, nil, err
	}
	tx.canceler = d.canceler
	return pooled, tx, nil
}

func (t *Tx) checkTemporary(ctx context.Context) error {
	var now int64
	if err := t.QueryRow(ctx, temporaryQuery).Scan(&now); err != nil {
		return err
	}
	if now > t.temporary {
		return fmt.Errorf("the statement created %d objects in the session's temporary schema, which would stay on the gateway's pooled connection after this call; it was rolled back", now-t.temporary)
	}
	return nil
}

func (t *Tx) settle(ctx context.Context, rollbackTool bool, report *Report) error {
	if rollbackTool {
		if _, err := t.conn.Exec(ctx, string(toolRollback)); err != nil {
			return err
		}
	}
	return t.cleanup(ctx, report)
}

func (p Plan) callTimeout() time.Duration {
	if p.CallTimeout > 0 {
		return p.CallTimeout
	}
	return p.Timeout + clientMargin
}

func open(ctx context.Context, conn *pgx.Conn, begin control, plan Plan) (*Tx, error) {
	settings := fmt.Sprintf("statement_timeout %dms, lock_timeout %s", plan.Timeout.Milliseconds(), lockTimeoutValue)
	statements := []control{begin, searchPath, lockTimeout}
	if begin == beginWrite {
		statements = []control{begin, lockTimeout}
	} else {
		settings += ", search_path " + searchPathValue
	}
	for _, statement := range statements {
		if _, err := conn.Exec(ctx, string(statement)); err != nil {
			return nil, err
		}
	}
	tx := &Tx{conn: conn, plan: plan, settings: settings}
	var schema, dataDirectory *string
	if err := conn.QueryRow(ctx, string(sessionQuery)).Scan(&tx.session.pid, &tx.session.start, &schema, &dataDirectory, &tx.sessionTimeout); err != nil {
		return nil, err
	}
	watchSession(conn, tx.session)
	if _, err := conn.Exec(ctx, fmt.Sprintf("SET LOCAL statement_timeout = %d", plan.Timeout.Milliseconds())); err != nil {
		return nil, err
	}
	if schema != nil {
		tx.hypopgSchema = *schema
	}
	if dataDirectory != nil {
		tx.dataDirectory = *dataDirectory
	}
	if plan.Hypopg && tx.HasHypopg() {
		if _, err := conn.Exec(ctx, tx.hypopg(hypopgResetTemplate)); err != nil {
			return nil, err
		}
	}
	if _, err := conn.Exec(ctx, string(toolSavepoint)); err != nil {
		return nil, err
	}
	return tx, nil
}

func (t *Tx) cleanup(ctx context.Context, report *Report) error {
	if t.HasHypopg() {
		indexes, hidden, err := t.countHypopg(ctx)
		if err != nil {
			return err
		}
		report.HypotheticalFound = indexes
		report.HiddenFound = hidden
		if t.plan.Hypopg || indexes > 0 || hidden > 0 {
			if _, err := t.conn.Exec(ctx, t.hypopg(endResetTemplate)); err != nil {
				return err
			}
			indexes, hidden, err = t.countHypopg(ctx)
			if err != nil {
				return err
			}
			report.HypotheticalAfter = indexes
			report.HiddenAfter = hidden
			report.HypopgRecounted = true
			if indexes != 0 || hidden != 0 {
				return fmt.Errorf("hypopg still reports %d hypothetical and %d hidden indexes after reset", indexes, hidden)
			}
		}
	}
	var locks int64
	if err := t.conn.QueryRow(ctx, string(advisoryCountQuery)).Scan(&locks); err != nil {
		return err
	}
	if locks == 0 {
		return nil
	}
	if _, err := t.conn.Exec(ctx, string(advisoryUnlockQuery)); err != nil {
		return err
	}
	var after int64
	if err := t.conn.QueryRow(ctx, string(advisoryCountQuery)).Scan(&after); err != nil {
		return err
	}
	report.AdvisoryReleased = int(locks - after)
	return nil
}

func (t *Tx) countHypopg(ctx context.Context) (int64, int64, error) {
	var indexes, hidden int64
	err := t.conn.QueryRow(ctx, t.hypopg(hypopgCountTemplate)).Scan(&indexes, &hidden)
	return indexes, hidden, err
}

func (t *Tx) terminate(ctx context.Context, report *Report) error {
	ownCtx, cancelOwn := detached(ctx)
	defer cancelOwn()
	err := t.terminateSession(ownCtx)
	if err != nil {
		separateCtx, cancelSeparate := detached(ctx)
		defer cancelSeparate()
		if separateErr := t.canceler.terminateSession(separateCtx, t.session); separateErr != nil {
			return errors.Join(err, separateErr)
		}
	}
	report.SessionTerminated = true
	return nil
}

func (t *Tx) terminateSession(ctx context.Context) error {
	var ended bool
	return endedOrError(t.conn.QueryRow(ctx, string(endSessionQuery), t.session.pid, t.session.start).Scan(&ended), ended)
}

func endedOrError(err error, ended bool) error {
	switch {
	case isTerminated(err), err == nil && ended:
		return nil
	case errors.Is(err, pgx.ErrNoRows):
		return errors.New("the session was not found, so its end is not confirmed")
	case err == nil:
		return errors.New("the session did not end within 3 seconds of pg_terminate_backend")
	default:
		return err
	}
}

func hand(pooled *pgxpool.Conn, reusable bool) {
	unwatchSession(pooled.Conn())
	if reusable {
		pooled.Release()
		return
	}
	conn := pooled.Hijack()
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = conn.Close(closeCtx)
}

func detached(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), cleanupTimeout)
}
