package advisor

import (
	"github.com/multigres/multigres/go/common/parser/ast"
)

type relation struct {
	schema, name string
}

type columnUse struct {
	levels [][]relation
	column string
}

type scope struct {
	parent  *scope
	tables  []relation
	byName  map[string]relation
	targets map[string]ast.Node
	ctes    map[string]bool
}

type collector struct {
	relations []relation
	uses      []columnUse
}

func collectColumns(stmt *ast.SelectStmt) *collector {
	c := &collector{}
	c.selectStmt(stmt, nil)
	return c
}

func (c *collector) selectStmt(stmt *ast.SelectStmt, parent *scope) {
	if stmt == nil {
		return
	}
	if stmt.Op != ast.SETOP_NONE {
		c.selectStmt(stmt.Larg, parent)
		c.selectStmt(stmt.Rarg, parent)
		return
	}
	s := &scope{parent: parent, byName: map[string]relation{}, targets: map[string]ast.Node{}, ctes: map[string]bool{}}
	if stmt.WithClause != nil && stmt.WithClause.Ctes != nil {
		for _, item := range stmt.WithClause.Ctes.Items {
			if cte, ok := item.(*ast.CommonTableExpr); ok {
				s.ctes[cte.Ctename] = true
			}
		}
		for _, item := range stmt.WithClause.Ctes.Items {
			if cte, ok := item.(*ast.CommonTableExpr); ok {
				c.subquery(cte.Ctequery, s)
			}
		}
	}
	var joins []*ast.JoinExpr
	if stmt.FromClause != nil {
		for _, item := range stmt.FromClause.Items {
			c.fromItem(item, s, &joins)
		}
	}
	if stmt.TargetList != nil {
		for _, item := range stmt.TargetList.Items {
			if target, ok := item.(*ast.ResTarget); ok && target.Name != "" && target.Val != nil {
				s.targets[target.Name] = target.Val
			}
		}
	}
	c.condition(stmt.WhereClause, s)
	for _, join := range joins {
		c.condition(join.Quals, s)
		c.using(join, s)
	}
	c.condition(stmt.HavingClause, s)
	if stmt.SortClause != nil {
		for _, item := range stmt.SortClause.Items {
			if sort, ok := item.(*ast.SortBy); ok {
				c.condition(s.outputAlias(sort.Node), s)
			}
		}
	}
	if stmt.GroupClause != nil {
		for _, item := range stmt.GroupClause.Items {
			c.condition(s.outputAlias(item), s)
		}
	}
}

func (c *collector) fromItem(item ast.Node, s *scope, joins *[]*ast.JoinExpr) {
	switch node := item.(type) {
	case *ast.RangeVar:
		if node.SchemaName == "" && s.isCTE(node.RelName) {
			return
		}
		rel := relation{schema: node.SchemaName, name: node.RelName}
		key := node.RelName
		if node.Alias != nil && node.Alias.AliasName != "" {
			key = node.Alias.AliasName
		}
		s.tables = append(s.tables, rel)
		s.byName[key] = rel
		c.relations = append(c.relations, rel)
	case *ast.JoinExpr:
		c.fromItem(node.Larg, s, joins)
		c.fromItem(node.Rarg, s, joins)
		*joins = append(*joins, node)
	case *ast.RangeSubselect:
		outer := s.parent
		if node.Lateral {
			outer = s
		}
		c.subquery(node.Subquery, outer)
	}
}

func (c *collector) subquery(node ast.Node, parent *scope) {
	if stmt, ok := node.(*ast.SelectStmt); ok {
		c.selectStmt(stmt, parent)
	}
}

func (c *collector) using(join *ast.JoinExpr, s *scope) {
	if join.UsingClause == nil {
		return
	}
	var sides []relation
	for _, side := range []ast.Node{join.Larg, join.Rarg} {
		sides = append(sides, relationsOf(side, s)...)
	}
	for _, item := range join.UsingClause.Items {
		if name, ok := item.(*ast.String); ok {
			c.uses = append(c.uses, columnUse{levels: [][]relation{sides}, column: name.SVal})
		}
	}
}

func relationsOf(node ast.Node, s *scope) []relation {
	switch item := node.(type) {
	case *ast.RangeVar:
		if item.SchemaName == "" && s.isCTE(item.RelName) {
			return nil
		}
		return []relation{{schema: item.SchemaName, name: item.RelName}}
	case *ast.JoinExpr:
		return append(relationsOf(item.Larg, s), relationsOf(item.Rarg, s)...)
	}
	return nil
}

func (c *collector) condition(node ast.Node, s *scope) {
	if node == nil {
		return
	}
	ast.Rewrite(node, func(cursor *ast.Cursor) bool {
		switch item := cursor.Node().(type) {
		case *ast.SelectStmt:
			c.selectStmt(item, s)
			return false
		case *ast.ColumnRef:
			c.columnRef(item, s)
			return false
		}
		return true
	}, nil)
}

func (c *collector) columnRef(ref *ast.ColumnRef, s *scope) {
	names := fieldNames(ref)
	switch len(names) {
	case 1:
		var levels [][]relation
		for at := s; at != nil; at = at.parent {
			if len(at.tables) > 0 {
				levels = append(levels, at.tables)
			}
		}
		c.uses = append(c.uses, columnUse{levels: levels, column: names[0]})
	case 2:
		if rel, ok := s.lookup(names[0]); ok {
			c.uses = append(c.uses, columnUse{levels: [][]relation{{rel}}, column: names[1]})
		}
	case 3:
		if rel, ok := s.lookupQualified(names[0], names[1]); ok {
			c.uses = append(c.uses, columnUse{levels: [][]relation{{rel}}, column: names[2]})
		}
	}
}

func fieldNames(ref *ast.ColumnRef) []string {
	if ref.Fields == nil {
		return nil
	}
	names := make([]string, 0, len(ref.Fields.Items))
	for _, item := range ref.Fields.Items {
		name, ok := item.(*ast.String)
		if !ok {
			return nil
		}
		names = append(names, name.SVal)
	}
	return names
}

func (s *scope) outputAlias(node ast.Node) ast.Node {
	ref, ok := node.(*ast.ColumnRef)
	if !ok {
		return node
	}
	names := fieldNames(ref)
	if len(names) != 1 {
		return node
	}
	if target, ok := s.targets[names[0]]; ok {
		return target
	}
	return node
}

func (s *scope) isCTE(name string) bool {
	for at := s; at != nil; at = at.parent {
		if at.ctes[name] {
			return true
		}
	}
	return false
}

func (s *scope) lookup(name string) (relation, bool) {
	for at := s; at != nil; at = at.parent {
		if rel, ok := at.byName[name]; ok {
			return rel, true
		}
	}
	return relation{}, false
}

func (s *scope) lookupQualified(schema, name string) (relation, bool) {
	for at := s; at != nil; at = at.parent {
		for _, rel := range at.tables {
			if rel.schema == schema && rel.name == name {
				return rel, true
			}
		}
	}
	return relation{}, false
}
