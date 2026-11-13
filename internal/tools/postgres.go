package tools

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/admin"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/db"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/sqlguard"
)

const (
	schemasQuery db.Query = `SELECT n.nspname AS schema_name, pg_catalog.pg_get_userbyid(n.nspowner) AS schema_owner,
       CASE
           WHEN n.nspname = 'multigres' THEN 'Multigres Internal Schema'
           WHEN n.nspname LIKE 'pg_%' THEN 'System Schema'
           WHEN n.nspname = 'information_schema' THEN 'System Information Schema'
           ELSE 'User Schema'
       END AS schema_type
FROM pg_catalog.pg_namespace n
ORDER BY schema_type, schema_name`
	tablesQuery db.Query = `SELECT n.nspname AS table_schema, c.relname AS table_name, k.table_type
FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL (SELECT CASE WHEN c.relkind IN ('r', 'p') THEN 'BASE TABLE' WHEN c.relkind = 'v' THEN 'VIEW' END AS table_type) k
WHERE n.nspname = $1 AND k.table_type = $2
ORDER BY table_name`
	sequencesQuery db.Query = `SELECT n.nspname AS sequence_schema, c.relname AS sequence_name, pg_catalog.format_type(s.seqtypid, NULL) AS data_type
FROM pg_catalog.pg_sequence s
JOIN pg_catalog.pg_class c ON c.oid = s.seqrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1
ORDER BY sequence_name`
	extensionsQuery db.Query = `SELECT e.extname, e.extversion, e.extrelocatable, n.nspname AS extschema
FROM pg_catalog.pg_extension e
JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
ORDER BY e.extname`
	columnsQuery db.Query = `SELECT a.attname AS column_name, pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
       CASE WHEN a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) THEN 'NO' ELSE 'YES' END AS is_nullable,
       CASE WHEN a.attgenerated = '' THEN pg_catalog.pg_get_expr(d.adbin, d.adrelid) END AS column_default
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p', 'v', 'f') AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum`
	constraintsQuery db.Query = `SELECT con.conname AS constraint_name,
       CASE con.contype WHEN 'p' THEN 'PRIMARY KEY' WHEN 'f' THEN 'FOREIGN KEY' WHEN 'u' THEN 'UNIQUE' WHEN 'c' THEN 'CHECK' WHEN 'x' THEN 'EXCLUDE' END AS constraint_type,
       a.attname AS column_name
FROM pg_catalog.pg_constraint con
JOIN pg_catalog.pg_class c ON c.oid = con.conrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN LATERAL pg_catalog.unnest(con.conkey) WITH ORDINALITY AS k(attnum, position) ON true
LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
WHERE n.nspname = $1 AND c.relname = $2 AND con.contype IN ('p', 'f', 'u', 'c', 'x')
ORDER BY constraint_name, k.position`
	indexesQuery db.Query = `SELECT indexname, indexdef
FROM pg_catalog.pg_indexes
WHERE schemaname = $1 AND tablename = $2
ORDER BY indexname`
	sequenceQuery db.Query = `SELECT n.nspname AS sequence_schema, c.relname AS sequence_name, pg_catalog.format_type(s.seqtypid, NULL) AS data_type,
       s.seqstart::pg_catalog.text AS start_value, s.seqincrement::pg_catalog.text AS increment
FROM pg_catalog.pg_sequence s
JOIN pg_catalog.pg_class c ON c.oid = s.seqrelid
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2`
	extensionQuery db.Query = `SELECT e.extname, e.extversion, e.extrelocatable, n.nspname AS extschema
FROM pg_catalog.pg_extension e
JOIN pg_catalog.pg_namespace n ON n.oid = e.extnamespace
WHERE e.extname = $1`
)

const callSettings = "note: set by this server for this call only: %s. SHOW and current_setting report these values. pg_settings.setting shows them too except statement_timeout, which the Multigres gateway keeps itself: it enforces it and answers SHOW and current_setting for it, but does not set it on the node. Before this call set its own, this server's session had statement_timeout %s: the value the gateway gives a session that connects the same way and does not set it. pg_settings.reset_val shows what a new session starts with for the other settings"

var indexMethods = map[string]bool{"btree": true, "hash": true, "gist": true, "gin": true, "brin": true, "spgist": true}

type section struct {
	label string
	query db.Query
	args  []any
}

type noInput struct{}

type objectsInput struct {
	SchemaName string `json:"schema_name" jsonschema:"Schema name (not used for extension)"`
	ObjectType string `json:"object_type,omitempty" jsonschema:"Object type: table, view, sequence or extension (default table)"`
}

type objectInput struct {
	SchemaName string `json:"schema_name" jsonschema:"Schema name (not used for extension)"`
	ObjectName string `json:"object_name" jsonschema:"Object name"`
	ObjectType string `json:"object_type,omitempty" jsonschema:"Object type: table, view, sequence or extension (default table)"`
}

type sqlInput struct {
	SQL string `json:"sql" jsonschema:"One SQL statement"`
}

type hypotheticalIndex struct {
	Table   string   `json:"table" jsonschema:"Table to add the index to, optionally schema-qualified (e.g. users or app.users)"`
	Columns []string `json:"columns" jsonschema:"Column names in index order"`
	Using   string   `json:"using,omitempty" jsonschema:"Index method: btree (default), hash, gist, gin, brin or spgist"`
}

type explainInput struct {
	SQL                 string              `json:"sql" jsonschema:"SQL query to explain"`
	Analyze             bool                `json:"analyze,omitempty" jsonschema:"Run the query to show real execution statistics; only for a SELECT that passes the read check, inside the time limit"`
	HypotheticalIndexes []hypotheticalIndex `json:"hypothetical_indexes,omitempty" jsonschema:"Hypothetical indexes to simulate with hypopg; they are removed before the call ends"`
}

func registerPostgres(server *mcp.Server, d *Deps) {
	add(server, d, "list_schemas", "List Schemas", "List all schemas in the database. The multigres schema is internal to Multigres.", reading, d.listSchemas)
	add(server, d, "list_objects", "List Objects", "List the tables, views or sequences in one schema, or every extension in the database (schema_name is not used for extension). Rows give the schema, name and type of each table or view, the schema, name and data type of each sequence, or the name, version, relocatable flag and the schema holding the objects of each extension. Read from the system catalogs, so the role's privileges do not hide objects. Use list_schemas for schema names and get_object_details for one object.", reading, d.listObjects)
	add(server, d, "get_object_details", "Get Object Details", "Show the columns (data type, nullability, default), constraints and indexes of a table or view, the data type, start and increment of a sequence, or the version, relocatable flag and the schema holding the objects of an extension (schema_name is not used for extension). Read from the system catalogs, so the role's privileges do not hide anything. Use list_objects to find names.", reading, d.objectDetails)
	add(server, d, "execute_sql", "Execute Read-Only SQL", "Run one read-only SQL statement (SELECT, EXPLAIN of a SELECT, or SHOW) through the Multigres gateway. The statement is checked before it is sent and runs in a read-only transaction that is always rolled back. Functions must be on the allow list.", reading, d.executeSQL)
	add(server, d, "explain_query", "Explain Query", "Show the execution plan of a SQL query with cost estimates. A query with $1-style placeholders (as in query statistics) is explained with a generic plan. Hypothetical indexes need the hypopg extension.", reading, d.explainQuery)
	registerDiagnostics(server, d)
}

func registerPostgresWrites(server *mcp.Server, d *Deps) {
	add(server, d, "execute_write_sql", "Execute Write SQL", "Run one SQL statement that may change data or schema, in a transaction that is committed if it succeeds. Transaction control, session-level SET and RESET, multigres.* settings, hypopg functions, and statements that leave session state behind (temporary objects, PREPARE, LISTEN, UNLISTEN, DECLARE WITH HOLD, LOAD, DISCARD) are rejected.", changing, d.executeWriteSQL)
}

func (d *Deps) readPlan() db.Plan { return db.Plan{Timeout: d.Config.SQLTimeout} }

func (d *Deps) listSchemas(ctx context.Context, _ noInput, out *output) error {
	return d.fixedRows(ctx, out, schemasQuery)
}

func (d *Deps) listObjects(ctx context.Context, in objectsInput, out *output) error {
	switch objectType(in.ObjectType) {
	case "table":
		return d.fixedRows(ctx, out, tablesQuery, in.SchemaName, "BASE TABLE")
	case "view":
		return d.fixedRows(ctx, out, tablesQuery, in.SchemaName, "VIEW")
	case "sequence":
		return d.fixedRows(ctx, out, sequencesQuery, in.SchemaName)
	case "extension":
		return d.fixedRows(ctx, out, extensionsQuery)
	default:
		return fmt.Errorf("unsupported object type %q; use table, view, sequence or extension", in.ObjectType)
	}
}

func objectType(given string) string {
	if given == "" {
		return "table"
	}
	return strings.ToLower(given)
}

func (d *Deps) objectDetails(ctx context.Context, in objectInput, out *output) error {
	kind := objectType(in.ObjectType)
	var sections []section
	switch kind {
	case "table", "view":
		args := []any{in.SchemaName, in.ObjectName}
		sections = []section{{"columns", columnsQuery, args}, {"constraints", constraintsQuery, args}, {"indexes", indexesQuery, args}}
	case "sequence":
		sections = []section{{"sequence", sequenceQuery, []any{in.SchemaName, in.ObjectName}}}
	case "extension":
		sections = []section{{"extension", extensionQuery, []any{in.ObjectName}}}
	default:
		return fmt.Errorf("unsupported object type %q; use table, view, sequence or extension", in.ObjectType)
	}
	if kind == "extension" {
		out.Line("object: %s (%s)", in.ObjectName, kind)
	} else {
		out.Line("object: %s.%s (%s)", in.SchemaName, in.ObjectName, kind)
	}
	nodes := d.poolerLookup(ctx)
	report, err := d.DB.Read(ctx, d.readPlan(), func(ctx context.Context, tx *db.Tx) error {
		for _, section := range sections {
			if out.truncated {
				break
			}
			out.Line("%s:", section.label)
			if err := readRows(ctx, out, func(ctx context.Context) (pgx.Rows, error) {
				return tx.Query(ctx, section.query, section.args...)
			}); err != nil {
				return err
			}
		}
		out.Tail("answered_by: %s", nodes.answeredBy(tx.DataDirectory()))
		return nil
	})
	d.notes(out, report)
	return err
}

func (d *Deps) fixedRows(ctx context.Context, out *output, query db.Query, args ...any) error {
	nodes := d.poolerLookup(ctx)
	report, err := d.DB.Read(ctx, d.readPlan(), func(ctx context.Context, tx *db.Tx) error {
		if err := readRows(ctx, out, func(ctx context.Context) (pgx.Rows, error) {
			return tx.Query(ctx, query, args...)
		}); err != nil {
			return err
		}
		out.Tail("answered_by: %s", nodes.answeredBy(tx.DataDirectory()))
		return nil
	})
	d.notes(out, report)
	return err
}

func (d *Deps) executeSQL(ctx context.Context, in sqlInput, out *output) error {
	checked, err := sqlguard.CheckRead(in.SQL)
	if err != nil {
		return fmt.Errorf("the statement was not sent: %w", err)
	}
	if checked.Placeholders() > 0 {
		return errors.New("the statement was not sent: it has $n placeholders; put the values in the statement, or use explain_query for a generic plan")
	}
	nodes := d.poolerLookup(ctx)
	report, err := d.DB.Read(ctx, d.readPlan(), func(ctx context.Context, tx *db.Tx) error {
		if err := tx.CheckShadowing(ctx, checked); err != nil {
			return err
		}
		if err := readRows(ctx, out, func(ctx context.Context) (pgx.Rows, error) {
			return tx.QueryChecked(ctx, checked)
		}); err != nil {
			return err
		}
		out.Tail(callSettings, tx.Settings(), tx.SessionTimeout())
		out.Tail("answered_by: %s", nodes.answeredBy(tx.DataDirectory()))
		return nil
	})
	d.notes(out, report)
	return err
}

func (d *Deps) explainQuery(ctx context.Context, in explainInput, out *output) error {
	checked, err := sqlguard.CheckRead(in.SQL)
	if err != nil {
		return fmt.Errorf("the statement was not sent: %w", err)
	}
	if !checked.IsSelect() {
		return errors.New("the statement was not sent: explain_query takes a SELECT")
	}
	if in.Analyze && len(in.HypotheticalIndexes) > 0 {
		return errors.New("analyze and hypothetical indexes cannot be used together")
	}
	if in.Analyze && checked.Placeholders() > 0 {
		return errors.New("analyze needs a statement without $n placeholders")
	}
	definitions, err := indexDefinitions(in.HypotheticalIndexes)
	if err != nil {
		return err
	}
	var explain *sqlguard.Checked
	if checked.Placeholders() == 0 {
		prefix := "EXPLAIN "
		if in.Analyze {
			prefix = "EXPLAIN (ANALYZE) "
		}
		explain, err = sqlguard.CheckRead(prefix + in.SQL)
		if err != nil {
			return fmt.Errorf("the statement was not sent: %w", err)
		}
	}
	plan := d.readPlan()
	plan.Hypopg = len(definitions) > 0
	nodes := d.poolerLookup(ctx)
	report, err := d.DB.Read(ctx, plan, func(ctx context.Context, tx *db.Tx) error {
		if err := tx.CheckShadowing(ctx, checked); err != nil {
			return err
		}
		for _, definition := range definitions {
			_, name, err := tx.CreateHypotheticalIndex(ctx, definition)
			if err != nil {
				return fmt.Errorf("hypothetical index %q: %w", definition, err)
			}
			out.Line("hypothetical index %s: %s", name, definition)
		}
		if explain == nil {
			out.Line("generic plan (the query has $n placeholders; costs do not use real values):")
			lines, err := tx.ExplainGeneric(ctx, checked, false)
			if err != nil {
				return err
			}
			for _, line := range lines {
				if !out.add(line + "\n") {
					break
				}
			}
		} else {
			rows, err := tx.QueryChecked(ctx, explain)
			if err != nil {
				return err
			}
			if err := writePlan(out, rows); err != nil {
				return err
			}
		}
		out.Tail("answered_by: %s", nodes.answeredBy(tx.DataDirectory()))
		return nil
	})
	d.notes(out, report)
	return err
}

func indexDefinitions(indexes []hypotheticalIndex) ([]string, error) {
	definitions := make([]string, 0, len(indexes))
	for _, index := range indexes {
		if index.Table == "" || len(index.Columns) == 0 {
			return nil, errors.New("each hypothetical index needs a table and at least one column")
		}
		method := strings.ToLower(index.Using)
		if method == "" {
			method = "btree"
		}
		if !indexMethods[method] {
			return nil, fmt.Errorf("unsupported index method %q", index.Using)
		}
		columns := make([]string, len(index.Columns))
		for i, column := range index.Columns {
			columns[i] = pgx.Identifier{column}.Sanitize()
		}
		table := pgx.Identifier(strings.Split(index.Table, ".")).Sanitize()
		definitions = append(definitions, fmt.Sprintf("CREATE INDEX ON %s USING %s (%s)", table, method, strings.Join(columns, ", ")))
	}
	return definitions, nil
}

func writePlan(out *output, rows pgx.Rows) error {
	defer rows.Close()
	for rows.Next() {
		raw := rows.RawValues()
		if len(raw) == 0 {
			continue
		}
		if !out.add(string(raw[0]) + "\n") {
			return nil
		}
	}
	return rows.Err()
}

func (d *Deps) executeWriteSQL(ctx context.Context, in sqlInput, out *output) error {
	checked, err := sqlguard.CheckWrite(in.SQL)
	if err != nil {
		return fmt.Errorf("the statement was not sent: %w", err)
	}
	nodes := d.poolerLookup(ctx)
	report, err := d.DB.Write(ctx, d.readPlan(), func(ctx context.Context, tx *db.Tx) error {
		rows, err := tx.QueryChecked(ctx, checked)
		if err != nil {
			return err
		}
		if err := writeRows(out, rows, nil); err != nil {
			return err
		}
		out.Tail("command: %s", rows.CommandTag().String())
		out.Tail(callSettings, tx.Settings(), tx.SessionTimeout())
		out.Tail("answered_by: %s", nodes.answeredBy(tx.DataDirectory()))
		return nil
	})
	d.notes(out, report)
	if err == nil {
		out.Tail("committed")
	}
	return err
}

func readRows(ctx context.Context, out *output, query func(context.Context) (pgx.Rows, error)) error {
	queryCtx, stop := context.WithCancel(ctx)
	defer stop()
	rows, err := query(queryCtx)
	if err != nil {
		return err
	}
	return writeRows(out, rows, stop)
}

func writeRows(out *output, rows pgx.Rows, stop context.CancelFunc) error {
	defer rows.Close()
	fields := rows.FieldDescriptions()
	names := make([]string, len(fields))
	for i, field := range fields {
		names[i] = field.Name
	}
	if len(names) > 0 {
		out.JSON("", names)
	}
	count := 0
	for rows.Next() {
		raw := rows.RawValues()
		values := make([]any, len(raw))
		for i, value := range raw {
			if value != nil {
				values[i] = string(value)
			}
		}
		line, err := compactJSON(values)
		if err != nil {
			return err
		}
		if !out.add(line + "\n") {
			if stop != nil {
				stop()
				out.Tail("rows shown: %d (stopped the statement at the size limit)", count)
				return nil
			}
			for rows.Next() {
			}
			if err := rows.Err(); err != nil {
				return err
			}
			out.Tail("rows shown: %d (the statement ran to the end; the other rows are not shown)", count)
			return nil
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(names) > 0 {
		out.Line("rows: %d", count)
	}
	return nil
}

type poolerLookup struct {
	poolers []*clustermetadatapb.Multipooler
	failure string
}

func (d *Deps) poolerLookup(ctx context.Context) poolerLookup {
	if d.Admin == nil {
		return poolerLookup{failure: "unknown (the admin server is not configured)"}
	}
	lookupCtx, cancel := context.WithTimeout(ctx, answeredTimeout)
	defer cancel()
	poolers, err := d.Admin.Poolers(lookupCtx)
	if err != nil {
		return poolerLookup{failure: "unknown (admin API: " + describe(err) + ")"}
	}
	return poolerLookup{poolers: poolers}
}

func (l poolerLookup) answeredBy(dataDirectory string) string {
	if dataDirectory == "" {
		return "unknown (this database role cannot read data_directory)"
	}
	if l.failure != "" {
		return l.failure
	}
	var names []string
	for _, pooler := range l.poolers {
		if pooler.GetPgDataDir() == dataDirectory {
			role := "replica"
			if admin.IsPrimary(pooler) {
				role = "primary"
			}
			names = append(names, admin.NodeName(pooler)+" ("+role+")")
		}
	}
	if len(names) == 1 {
		return names[0]
	}
	return fmt.Sprintf("unknown (%d nodes report this data directory)", len(names))
}
