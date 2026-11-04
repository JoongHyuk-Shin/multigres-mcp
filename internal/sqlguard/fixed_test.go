package sqlguard_test

import (
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	pgparser "github.com/multigres/multigres/go/common/parser"
	pgast "github.com/multigres/multigres/go/common/parser/ast"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/sqlguard"
)

var allowedFailures = map[string]struct{ file, functions string }{
	"logSizeQuery":         {"tools/cluster.go", "pg_stat_file on a direct node connection"},
	"logReadQuery":         {"tools/cluster.go", "pg_read_binary_file on a direct node connection"},
	"replicationLagQuery":  {"diag/queries.go", "pg_last_wal_receive_lsn, pg_last_wal_replay_lsn, pg_last_xact_replay_timestamp"},
	"hypopgResetTemplate":  {"db/tx.go", "hypopg_reset and hypopg_unhide_all_indexes"},
	"hypopgCreateTemplate": {"db/tx.go", "hypopg_create_index"},
	"hypopgSizeTemplate":   {"db/tx.go", "hypopg_relation_size"},
	"freeSpaceTemplate":    {"diag/queries.go", "pgstattuple_approx"},
	"advisoryUnlockQuery":  {"db/tx.go", "pg_advisory_unlock_all"},
	"terminateQuery":       {"db/tx.go", "pg_terminate_backend"},
	"cancelSessionQuery":   {"db/cancel.go", "pg_cancel_backend"},
}

var wordsThatParse = map[string]string{
	"Cluster Status": "tools/cluster.go",
	"vacuum":         "diag/diag.go",
	"analyze":        "sqlguard/guard.go",
}

type fixedQuery struct {
	name, file, text string
}

func TestFixedQueriesPassTheReadCheck(t *testing.T) {
	queries := fixedQueries(t, "..")
	if len(queries) < 40 {
		t.Fatalf("found only %d fixed queries; the scan is not reading the packages", len(queries))
	}
	failed := map[string]bool{}
	for _, query := range queries {
		_, err := sqlguard.CheckRead(query.text)
		if err == nil {
			continue
		}
		failed[query.name] = true
		exception, ok := allowedFailures[query.name]
		if !ok || !strings.HasSuffix(filepath.ToSlash(query.file), "/"+exception.file) {
			t.Errorf("%s (%s) does not pass the read check and is not in the exception table for that file: %v", query.name, query.file, err)
		}
	}
	for name := range allowedFailures {
		if !failed[name] {
			t.Errorf("%s is in the exception table but passes the read check or no longer exists", name)
		}
	}
	t.Logf("checked %d fixed queries, %d are exceptions", len(queries), len(failed))
}

func TestFixedQueriesQualifyEveryFunction(t *testing.T) {
	for _, query := range fixedQueries(t, "..") {
		statements, err := pgparser.ParseSQL(query.text)
		if err != nil {
			t.Errorf("%s (%s) does not parse: %v", query.name, query.file, err)
			continue
		}
		for _, statement := range statements {
			pgast.Rewrite(statement, func(cursor *pgast.Cursor) bool {
				call, ok := cursor.Node().(*pgast.FuncCall)
				if ok && call.Funcname != nil && len(call.Funcname.Items) < 2 {
					t.Errorf("%s (%s) calls %s without a schema, so a same-named function earlier in the search path can run instead", query.name, query.file, call.Funcname.SqlString())
				}
				return true
			}, nil)
		}
	}
}

func TestQueryLiteralsAreTypedConstants(t *testing.T) {
	scanned := 0
	for path, file := range shippedFiles(t) {
		typed := typedQueryLiterals(file)
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			text, err := strconv.Unquote(literal.Value)
			if err != nil || !parsesAsStatements(text) {
				return true
			}
			scanned++
			if file, ok := wordsThatParse[text]; ok && strings.HasSuffix(filepath.ToSlash(path), "/"+file) {
				return true
			}
			if !typed[literal] {
				t.Errorf("%s: an SQL statement literal outside a Query or control constant, so the fixed-query check does not see it: %q", path, text)
			}
			return true
		})
	}
	if scanned < 40 {
		t.Fatalf("found only %d query literals; the scan is not reading the packages", scanned)
	}
}

func parsesAsStatements(text string) bool {
	statements, err := pgparser.ParseSQL(text)
	return err == nil && len(statements) > 0
}

func shippedFiles(t *testing.T) map[string]*ast.File {
	t.Helper()
	const module = "github.com/JoongHyuk-Shin/multigres-mcp/"
	files := map[string]*ast.File{}
	queue := []string{"../../cmd/multigres-mcp"}
	seen := map[string]bool{queue[0]: true}
	for len(queue) > 0 {
		dir := queue[0]
		queue = queue[1:]
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			name := entry.Name()
			if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				continue
			}
			if match, err := build.Default.MatchFile(dir, name); err != nil || !match {
				continue
			}
			path := filepath.Join(dir, name)
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			files[path] = file
			for _, spec := range file.Imports {
				imported, _ := strconv.Unquote(spec.Path.Value)
				if local, ok := strings.CutPrefix(imported, module); ok && !seen["../../"+local] {
					seen["../../"+local] = true
					queue = append(queue, "../../"+local)
				}
			}
		}
	}
	return files
}

func typedQueryLiterals(file *ast.File) map[*ast.BasicLit]bool {
	typed := map[*ast.BasicLit]bool{}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value := spec.(*ast.ValueSpec)
			if !isQueryType(value.Type) && !isControlType(value.Type) {
				continue
			}
			for _, expr := range value.Values {
				if literal, ok := expr.(*ast.BasicLit); ok {
					typed[literal] = true
				}
			}
		}
	}
	return typed
}

func isControlType(expr ast.Expr) bool {
	ident, ok := expr.(*ast.Ident)
	return ok && ident.Name == "control"
}

func TestTransactionControlStaysInDB(t *testing.T) {
	scanned := 0
	err := filepath.WalkDir("../..", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if filepath.ToSlash(path) == "../../internal/db" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			text, err := strconv.Unquote(literal.Value)
			if err != nil {
				return true
			}
			scanned++
			if isTransactionControl(text) {
				t.Errorf("%s sends transaction control outside internal/db: %q", path, text)
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned < 300 {
		t.Fatalf("scanned only %d string literals; the walk is not reading the packages", scanned)
	}
}

func isTransactionControl(text string) bool {
	statements, err := pgparser.ParseSQL(text)
	if err != nil {
		return false
	}
	for _, statement := range statements {
		switch typed := statement.(type) {
		case *pgast.TransactionStmt:
			return true
		case *pgast.VariableSetStmt:
			if typed.IsLocal {
				return true
			}
		}
	}
	return false
}

func fixedQueries(t *testing.T, root string) []fixedQuery {
	t.Helper()
	var queries []fixedQuery
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			return err
		}
		queries = append(queries, queriesIn(t, path, file)...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(queries, func(i, j int) bool { return queries[i].name < queries[j].name })
	return queries
}

func queriesIn(t *testing.T, path string, file *ast.File) []fixedQuery {
	var queries []fixedQuery
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value := spec.(*ast.ValueSpec)
			if !isQueryType(value.Type) {
				continue
			}
			for i, name := range value.Names {
				literal, ok := value.Values[i].(*ast.BasicLit)
				if !ok {
					t.Fatalf("%s in %s is not a string literal", name.Name, path)
				}
				text, err := strconv.Unquote(literal.Value)
				if err != nil {
					t.Fatalf("%s in %s: %v", name.Name, path, err)
				}
				queries = append(queries, fixedQuery{name: name.Name, file: path, text: fillTemplate(text)})
			}
		}
	}
	return queries
}

func isQueryType(expr ast.Expr) bool {
	switch typed := expr.(type) {
	case *ast.Ident:
		return typed.Name == "Query"
	case *ast.SelectorExpr:
		return typed.Sel.Name == "Query"
	}
	return false
}

func fillTemplate(text string) string {
	text = strings.ReplaceAll(text, "%[1]s", `"s"`)
	return strings.ReplaceAll(text, "%%", "%")
}
