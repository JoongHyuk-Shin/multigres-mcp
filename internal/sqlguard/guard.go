package sqlguard

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"
)

const (
	genericPlanPrefix     = "EXPLAIN (GENERIC_PLAN) "
	genericPlanJSONPrefix = "EXPLAIN (GENERIC_PLAN, FORMAT JSON) "
)

var allowedExplainOptions = map[string]bool{
	"analyze": true, "verbose": true, "costs": true, "settings": true, "generic_plan": true,
	"buffers": true, "serialize": true, "wal": true, "timing": true, "summary": true,
	"memory": true, "format": true,
}

type Statement interface {
	Text() string
	passedGuard()
}

type Checked struct {
	text         string
	tree         ast.Stmt
	unqualified  []string
	placeholders int
}

type CheckedWrite struct {
	text string
}

func (c *CheckedWrite) Text() string { return c.text }

func (c *CheckedWrite) passedGuard() {}

func (c *Checked) passedGuard() {}

func CheckRead(sql string) (*Checked, error) {
	tree, err := parseOne(sql)
	if err != nil {
		return nil, err
	}
	if err := checkReadTop(tree); err != nil {
		return nil, err
	}
	walk := readWalk{unqualified: map[string]bool{}}
	ast.Rewrite(tree, walk.visit, nil)
	if walk.err != nil {
		return nil, walk.err
	}
	return &Checked{text: sql, tree: tree, unqualified: sortedKeys(walk.unqualified), placeholders: walk.maxParam}, nil
}

func (c *Checked) Text() string { return c.text }

func (c *Checked) UnqualifiedFunctions() []string { return c.unqualified }

func (c *Checked) Placeholders() int { return c.placeholders }

func (c *Checked) IsSelect() bool {
	_, ok := c.tree.(*ast.SelectStmt)
	return ok
}

func (c *Checked) GenericPlanText(json bool) (string, *Checked, error) {
	inner := c.tree.SqlString()
	sent, err := verifyOneReadStatement(inner)
	if err != nil {
		return "", nil, fmt.Errorf("rewritten query failed the re-check: %w", err)
	}
	if json {
		return genericPlanJSONPrefix + inner, sent, nil
	}
	return genericPlanPrefix + inner, sent, nil
}

func CheckWrite(sql string) (*CheckedWrite, error) {
	tree, err := parseOne(sql)
	if err != nil {
		return nil, err
	}
	if err := checkWriteTop(tree); err != nil {
		return nil, err
	}
	walk := writeWalk{}
	ast.Rewrite(tree, walk.visit, nil)
	if walk.err != nil {
		return nil, walk.err
	}
	return &CheckedWrite{text: sql}, nil
}

func verifyOneReadStatement(text string) (*Checked, error) {
	checked, err := CheckRead(text)
	if err != nil {
		return nil, err
	}
	if !checked.IsSelect() {
		return nil, errors.New("the re-check expected a SELECT")
	}
	return checked, nil
}

func parseOne(sql string) (ast.Stmt, error) {
	if strings.TrimSpace(sql) == "" {
		return nil, errors.New("the SQL is empty")
	}
	statements, err := parser.ParseSQL(sql)
	if err != nil {
		return nil, fmt.Errorf("the SQL does not parse: %w", err)
	}
	if len(statements) != 1 {
		return nil, fmt.Errorf("exactly one statement is allowed, got %d", len(statements))
	}
	return statements[0], nil
}

func checkReadTop(tree ast.Stmt) error {
	switch stmt := tree.(type) {
	case *ast.SelectStmt, *ast.VariableShowStmt:
		return nil
	case *ast.ExplainStmt:
		if _, ok := stmt.Query.(*ast.SelectStmt); !ok {
			return errors.New("EXPLAIN is allowed only on SELECT")
		}
		return checkExplainOptions(stmt.Options)
	default:
		return fmt.Errorf("only SELECT, EXPLAIN SELECT and SHOW are allowed, got %s", tree.StatementType())
	}
}

func checkExplainOptions(options *ast.NodeList) error {
	if options == nil {
		return nil
	}
	for _, item := range options.Items {
		option, ok := item.(*ast.DefElem)
		if !ok || !allowedExplainOptions[strings.ToLower(option.Defname)] {
			return fmt.Errorf("EXPLAIN option %s is not allowed", item.SqlString())
		}
	}
	return nil
}

type readWalk struct {
	unqualified map[string]bool
	maxParam    int
	err         error
}

func (w *readWalk) visit(cursor *ast.Cursor) bool {
	if w.err != nil {
		return false
	}
	switch node := cursor.Node().(type) {
	case *ast.InsertStmt, *ast.UpdateStmt, *ast.DeleteStmt, *ast.MergeStmt:
		w.err = errors.New("statements that change data are not allowed")
	case *ast.IntoClause:
		w.err = errors.New("an INTO clause (SELECT INTO) is not allowed")
	case *ast.LockingClause:
		w.err = errors.New("locking clauses such as FOR UPDATE are not allowed")
	case *ast.FuncCall:
		w.err = w.checkFunction(node)
	case *ast.ParamRef:
		w.maxParam = max(w.maxParam, node.Number)
	}
	return w.err == nil
}

func (w *readWalk) checkFunction(call *ast.FuncCall) error {
	schema, name, err := functionName(call)
	if err != nil {
		return err
	}
	if schema != "" && schema != "pg_catalog" {
		return fmt.Errorf("function %s.%s is not allowed: only pg_catalog functions may be schema-qualified", schema, name)
	}
	if !allowedFunctions[name] {
		return fmt.Errorf("function %s is not allowed in read-only SQL", name)
	}
	if schema == "" {
		w.unqualified[name] = true
	}
	return nil
}

func functionName(call *ast.FuncCall) (string, string, error) {
	if call.Funcname == nil || len(call.Funcname.Items) == 0 || len(call.Funcname.Items) > 2 {
		return "", "", fmt.Errorf("function name %s is not allowed", call.SqlString())
	}
	parts := make([]string, len(call.Funcname.Items))
	for i, item := range call.Funcname.Items {
		part, ok := item.(*ast.String)
		if !ok {
			return "", "", fmt.Errorf("function name %s is not allowed", call.SqlString())
		}
		parts[i] = part.SVal
	}
	if len(parts) == 1 {
		return "", parts[0], nil
	}
	return parts[0], parts[1], nil
}

func checkWriteTop(tree ast.Stmt) error {
	switch stmt := tree.(type) {
	case *ast.TransactionStmt:
		return errors.New("transaction control is done by the server")
	case *ast.VariableSetStmt:
		if !stmt.IsLocal {
			return errors.New("only SET LOCAL is allowed; session SET and RESET would stay on the pooled connection")
		}
		if strings.HasPrefix(strings.ToLower(stmt.Name), "multigres.") {
			return errors.New("multigres.* settings are not allowed")
		}
	case *ast.PrepareStmt, *ast.ListenStmt, *ast.UnlistenStmt, *ast.LoadStmt, *ast.DiscardStmt:
		return fmt.Errorf("%s is not allowed: it leaves session state on the pooled connection", tree.StatementType())
	case *ast.DoStmt:
		return errors.New("DO is not allowed: its body is not checked and can leave session state on the pooled connection")
	case *ast.DeclareCursorStmt:
		if stmt.Options&ast.CURSOR_OPT_HOLD != 0 {
			return errors.New("DECLARE ... WITH HOLD is not allowed: the cursor outlives the transaction")
		}
	case *ast.CreateStmt:
		return rejectTemp(stmt.Relation)
	case *ast.ViewStmt:
		return rejectTemp(stmt.View)
	case *ast.CreateSeqStmt:
		return rejectTemp(stmt.Sequence)
	}
	return nil
}

func rejectTemp(relation *ast.RangeVar) error {
	if relation != nil && relation.RelPersistence == ast.RELPERSISTENCE_TEMP {
		return errors.New("temporary objects are not allowed: they stay on the pooled connection")
	}
	return nil
}

type writeWalk struct {
	err error
}

func (w *writeWalk) visit(cursor *ast.Cursor) bool {
	if w.err != nil {
		return false
	}
	switch node := cursor.Node().(type) {
	case *ast.IntoClause:
		w.err = rejectTemp(node.Rel)
	case *ast.FuncCall:
		w.err = checkWriteFunction(node)
	}
	return w.err == nil
}

func checkWriteFunction(call *ast.FuncCall) error {
	schema, name, err := functionName(call)
	if err != nil {
		return err
	}
	if strings.HasPrefix(strings.ToLower(name), "hypopg") {
		return errors.New("hypopg functions are not allowed in write SQL")
	}
	if schema != "" && schema != "pg_catalog" {
		return nil
	}
	switch {
	case name == "set_config" && !isLocalSetting(call):
		return errors.New("set_config is allowed only with the literal true as its third argument; a session setting would stay on the pooled connection")
	case name == "setseed":
		return errors.New("setseed is not allowed: the seed stays on the pooled connection and the gateway pins that connection")
	case isTemporarySlot(name, call):
		return fmt.Errorf("%s with temporary not given as the literal false is not allowed: a temporary slot stays on the pooled connection", name)
	}
	return nil
}

func isTemporarySlot(name string, call *ast.FuncCall) bool {
	index, ok := ast.ReplicationSlotFuncTemporaryArgIndex[name]
	if !ok {
		return false
	}
	arg, given := ast.FuncCallArg(call, index, ast.ReplicationSlotTemporaryParamName)
	if !given {
		return false
	}
	constant, ok := arg.(*ast.A_Const)
	if !ok {
		return true
	}
	value, ok := constant.Val.(*ast.Boolean)
	return !ok || value.BoolVal
}

func isLocalSetting(call *ast.FuncCall) bool {
	if call.Args == nil || len(call.Args.Items) != 3 {
		return false
	}
	constant, ok := call.Args.Items[2].(*ast.A_Const)
	if !ok {
		return false
	}
	value, ok := constant.Val.(*ast.Boolean)
	return ok && value.BoolVal
}

func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
