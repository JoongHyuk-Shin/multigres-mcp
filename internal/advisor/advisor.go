package advisor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/multigres/multigres/go/common/parser"
	"github.com/multigres/multigres/go/common/parser/ast"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/db"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/sqlguard"
)

const (
	DefaultBudgetMB    = 10000
	SearchLimit        = 30 * time.Second
	CallTimeout        = 60 * time.Second
	maxIndexWidth      = 3
	paretoAlpha        = 2.0
	minTimeImprovement = 0.1
	longTextWidth      = maxTextLength * 0.4
)

type Query struct {
	Text    string
	Weight  float64
	QueryID int64
}

type Options struct {
	BudgetMB int
	Describe func(error) string
}

type Skipped struct {
	Query  Query
	Reason string
}

type NotIndexable struct {
	Index  string `json:"index"`
	Reason string `json:"reason"`
}

type Recommendation struct {
	Definition string
	Table      string
	Columns    []string
	SizeBytes  int64
	LongText   bool
	CostBefore float64
	CostAfter  float64
	CostAlone  float64
}

type QueryCost struct {
	Query  Query
	Before float64
	After  float64
}

type Analysis struct {
	Analyzed        int
	Skipped         []Skipped
	NoStatistics    []string
	NotIndexable    []NotIndexable
	Candidates      int
	BaseCost        float64
	FinalCost       float64
	Recommendations []Recommendation
	Queries         []QueryCost
	TimedOut        bool
}

type workloadQuery struct {
	Query
	checked *sqlguard.Checked
	explain *sqlguard.Checked
	uses    []columnUse
	rels    []relation
}

type candidate struct {
	table      table
	columns    []string
	definition string
	size       int64
	longText   bool
}

type session struct {
	tx         *db.Tx
	queries    []*workloadQuery
	candidates []*candidate
	costs      map[string][]float64
	deadline   time.Time
}

func Recommend(ctx context.Context, tx *db.Tx, queries []Query, opts Options) (*Analysis, error) {
	analysis := &Analysis{}
	s := &session{tx: tx, costs: map[string][]float64{}, deadline: time.Now().Add(SearchLimit)}
	parsed := parseWorkload(queries, analysis)
	tables, err := resolve(ctx, tx, allRelations(parsed))
	if err != nil {
		return nil, err
	}
	parsed = keepUserTables(parsed, tables, analysis)
	columns, err := loadColumns(ctx, tx, ordinaryOIDs(tables))
	if err != nil {
		return nil, err
	}
	analysis.NoStatistics = withoutStatistics(parsed, tables, columns)
	before, err := s.baseline(ctx, parsed, analysis, opts.Describe)
	if err != nil {
		return nil, err
	}
	analysis.Analyzed = len(s.queries)
	if len(s.queries) == 0 {
		return analysis, nil
	}
	if err := s.buildCandidates(ctx, tables, columns, analysis, opts.Describe); err != nil {
		return nil, err
	}
	analysis.Candidates = len(s.candidates)
	base, err := s.baseSize(ctx)
	if err != nil {
		return nil, err
	}
	chosen, timedOut, err := s.greedy(ctx, base, opts.BudgetMB)
	if err != nil {
		return nil, err
	}
	analysis.TimedOut = timedOut
	return analysis, s.report(ctx, analysis, before, chosen)
}

func parseWorkload(queries []Query, analysis *Analysis) []*workloadQuery {
	var parsed []*workloadQuery
	for _, query := range queries {
		item, reason := parseQuery(query)
		if reason != "" {
			analysis.Skipped = append(analysis.Skipped, Skipped{Query: query, Reason: reason})
			continue
		}
		parsed = append(parsed, item)
	}
	return parsed
}

func parseQuery(query Query) (*workloadQuery, string) {
	checked, err := sqlguard.CheckRead(query.Text)
	if err != nil {
		return nil, "did not pass the read check: " + err.Error()
	}
	if !checked.IsSelect() {
		return nil, "not a SELECT"
	}
	statements, err := parser.ParseSQL(query.Text)
	if err != nil || len(statements) != 1 {
		return nil, "did not parse as one statement"
	}
	stmt, ok := statements[0].(*ast.SelectStmt)
	if !ok {
		return nil, "not a SELECT"
	}
	item := &workloadQuery{Query: query, checked: checked}
	if checked.Placeholders() == 0 {
		item.explain, err = sqlguard.CheckRead("EXPLAIN (FORMAT JSON) " + query.Text)
		if err != nil {
			return nil, "its EXPLAIN did not pass the read check: " + err.Error()
		}
	}
	found := collectColumns(stmt)
	item.uses = found.uses
	item.rels = found.relations
	return item, ""
}

func allRelations(queries []*workloadQuery) []relation {
	var rels []relation
	for _, query := range queries {
		rels = append(rels, query.rels...)
	}
	return rels
}

func keepUserTables(queries []*workloadQuery, tables map[relation]table, analysis *Analysis) []*workloadQuery {
	var kept []*workloadQuery
	for _, query := range queries {
		ordinary := false
		for _, rel := range query.rels {
			ordinary = ordinary || tables[rel].ordinary
		}
		if !ordinary {
			analysis.Skipped = append(analysis.Skipped, Skipped{Query: query.Query, Reason: "reads no ordinary user table"})
			continue
		}
		kept = append(kept, query)
	}
	return kept
}

func ordinaryOIDs(tables map[relation]table) []uint32 {
	seen := map[uint32]bool{}
	var oids []uint32
	for _, t := range tables {
		if t.ordinary && !seen[t.oid] {
			seen[t.oid] = true
			oids = append(oids, t.oid)
		}
	}
	sort.Slice(oids, func(i, j int) bool { return oids[i] < oids[j] })
	return oids
}

func withoutStatistics(queries []*workloadQuery, tables map[relation]table, columns map[uint32]map[string]column) []string {
	seen := map[uint32]bool{}
	var missing []string
	for _, query := range queries {
		for _, rel := range query.rels {
			t := tables[rel]
			if !t.ordinary || seen[t.oid] {
				continue
			}
			seen[t.oid] = true
			if !hasStatistics(columns[t.oid]) {
				missing = append(missing, t.String())
			}
		}
	}
	return missing
}

func hasStatistics(columns map[string]column) bool {
	for _, info := range columns {
		if info.avgWidth != nil {
			return true
		}
	}
	return false
}

func (s *session) baseline(ctx context.Context, queries []*workloadQuery, analysis *Analysis, describe func(error) string) ([]float64, error) {
	if err := s.tx.ResetHypothetical(ctx); err != nil {
		return nil, err
	}
	var before []float64
	for _, query := range queries {
		var cost float64
		err := s.tx.Try(ctx, func() error {
			if err := s.tx.CheckShadowing(ctx, query.checked); err != nil {
				return err
			}
			var err error
			cost, err = s.planCost(ctx, query)
			return err
		})
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil {
			analysis.Skipped = append(analysis.Skipped, Skipped{Query: query.Query, Reason: "EXPLAIN failed: " + describe(err)})
			continue
		}
		s.queries = append(s.queries, query)
		before = append(before, cost)
	}
	s.costs[configKey(nil)] = before
	return before, nil
}

func (s *session) planCost(ctx context.Context, query *workloadQuery) (float64, error) {
	var text string
	if query.explain == nil {
		lines, err := s.tx.ExplainGeneric(ctx, query.checked, true)
		if err != nil {
			return 0, err
		}
		text = strings.Join(lines, "\n")
	} else {
		rows, err := s.tx.QueryChecked(ctx, query.explain)
		if err != nil {
			return 0, err
		}
		text, err = pgx.CollectExactlyOneRow(rows, pgx.RowTo[string])
		if err != nil {
			return 0, err
		}
	}
	return totalCost(text)
}

func totalCost(planJSON string) (float64, error) {
	var plans []struct {
		Plan struct {
			TotalCost *float64 `json:"Total Cost"`
		} `json:"Plan"`
	}
	if err := json.Unmarshal([]byte(planJSON), &plans); err != nil {
		return 0, fmt.Errorf("could not read the JSON plan: %w", err)
	}
	if len(plans) != 1 || plans[0].Plan.TotalCost == nil {
		return 0, errors.New("the JSON plan has no Total Cost")
	}
	return *plans[0].Plan.TotalCost, nil
}

func (s *session) buildCandidates(ctx context.Context, tables map[relation]table, columns map[uint32]map[string]column, analysis *Analysis, describe func(error) string) error {
	order, used := conditionColumns(s.queries, tables, columns)
	existing, err := loadExisting(ctx, s.tx, ordinaryOIDs(tables))
	if err != nil {
		return err
	}
	for _, t := range order {
		for _, combo := range combinations(used[t.oid], maxIndexWidth) {
			if existing[indexKey(t.oid, "btree", combo)] {
				continue
			}
			long, drop := longText(columns[t.oid], combo)
			if drop {
				continue
			}
			s.candidates = append(s.candidates, &candidate{table: t, columns: combo, definition: definition(t, combo), longText: long})
		}
	}
	return s.measureCandidates(ctx, analysis, describe)
}

func conditionColumns(queries []*workloadQuery, tables map[relation]table, columns map[uint32]map[string]column) ([]table, map[uint32][]string) {
	var order []table
	used := map[uint32][]string{}
	seen := map[string]bool{}
	for _, use := range allUses(queries) {
		for _, t := range bindUse(use, tables, columns) {
			key := strconv.FormatUint(uint64(t.oid), 10) + "." + use.column
			if seen[key] {
				continue
			}
			seen[key] = true
			if used[t.oid] == nil {
				order = append(order, t)
			}
			used[t.oid] = append(used[t.oid], use.column)
		}
	}
	return order, used
}

func allUses(queries []*workloadQuery) []columnUse {
	var uses []columnUse
	for _, query := range queries {
		uses = append(uses, query.uses...)
	}
	return uses
}

func bindUse(use columnUse, tables map[relation]table, columns map[uint32]map[string]column) []table {
	for _, level := range use.levels {
		var found []table
		for _, rel := range level {
			t := tables[rel]
			if _, ok := columns[t.oid][use.column]; ok && t.ordinary {
				found = append(found, t)
			}
		}
		if len(found) > 0 {
			return found
		}
	}
	return nil
}

func combinations(columns []string, maxWidth int) [][]string {
	var all [][]string
	var pick func(start int, chosen []string, width int)
	pick = func(start int, chosen []string, width int) {
		if len(chosen) == width {
			all = append(all, append([]string(nil), chosen...))
			return
		}
		for i := start; i < len(columns); i++ {
			pick(i+1, append(chosen, columns[i]), width)
		}
	}
	for width := 1; width <= maxWidth && width <= len(columns); width++ {
		pick(0, nil, width)
	}
	return all
}

func longText(columns map[string]column, combo []string) (long bool, drop bool) {
	for _, name := range combo {
		info := columns[name]
		if !info.longText {
			continue
		}
		if info.avgWidth == nil || float64(*info.avgWidth) > longTextWidth {
			return true, true
		}
		long = true
	}
	return long, false
}

func definition(t table, columns []string) string {
	quoted := make([]string, len(columns))
	for i, column := range columns {
		quoted[i] = pgx.Identifier{column}.Sanitize()
	}
	return fmt.Sprintf("CREATE INDEX ON %s USING btree (%s)", t.sanitized(), strings.Join(quoted, ", "))
}

func (s *session) measureCandidates(ctx context.Context, analysis *Analysis, describe func(error) string) error {
	buildable := s.candidates[:0]
	for _, c := range s.candidates {
		var oid uint32
		err := s.tx.Try(ctx, func() error {
			var err error
			oid, _, err = s.tx.CreateHypotheticalIndex(ctx, c.definition)
			return err
		})
		if isNoDefaultOperatorClass(err) {
			analysis.NotIndexable = append(analysis.NotIndexable, NotIndexable{Index: c.definition, Reason: describe(err)})
			continue
		}
		if err != nil {
			return fmt.Errorf("hypothetical index %q: %w", c.definition, err)
		}
		if c.size, err = s.tx.HypotheticalIndexSize(ctx, oid); err != nil {
			return err
		}
		buildable = append(buildable, c)
	}
	s.candidates = buildable
	return s.tx.ResetHypothetical(ctx)
}

func isNoDefaultOperatorClass(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42704"
}

func (s *session) greedy(ctx context.Context, base int64, budgetMB int) ([]int, bool, error) {
	budget := int64(budgetMB) * 1024 * 1024
	var chosen []int
	currentTime, err := s.average(ctx, chosen)
	if err != nil || currentTime <= 0 {
		return nil, false, err
	}
	currentSpace := base
	currentObjective := objective(currentTime, currentSpace)
	remaining := map[int]bool{}
	for i := range s.candidates {
		remaining[i] = true
	}
	for {
		best, bestTime, bestSpace, bestObjective, bestImprovement := -1, currentTime, currentSpace, currentObjective, 0.0
		for _, i := range sortedKeys(remaining) {
			if time.Now().After(s.deadline) {
				return chosen, true, nil
			}
			space := currentSpace + s.candidates[i].size
			if budget > 0 && space-base > budget {
				continue
			}
			cost, err := s.average(ctx, append(append([]int(nil), chosen...), i))
			if err != nil {
				return nil, false, err
			}
			improvement := (currentTime - cost) / currentTime
			if improvement < minTimeImprovement {
				continue
			}
			value := objective(cost, space)
			if value < bestObjective && improvement > bestImprovement {
				best, bestTime, bestSpace, bestObjective, bestImprovement = i, cost, space, value, improvement
			}
		}
		if best < 0 {
			return chosen, false, nil
		}
		chosen = append(chosen, best)
		delete(remaining, best)
		currentTime, currentSpace, currentObjective = bestTime, bestSpace, bestObjective
	}
}

func objective(cost float64, space int64) float64 {
	if cost <= 0 || space <= 0 {
		return math.Inf(1)
	}
	return math.Log(cost) + paretoAlpha*math.Log(float64(space))
}

func (s *session) baseSize(ctx context.Context) (int64, error) {
	seen := map[uint32]bool{}
	var total int64
	for _, c := range s.candidates {
		if seen[c.table.oid] {
			continue
		}
		seen[c.table.oid] = true
		size, err := tableSize(ctx, s.tx, c.table.oid)
		if err != nil {
			return 0, err
		}
		total += size
	}
	return total, nil
}

func (s *session) average(ctx context.Context, config []int) (float64, error) {
	costs, err := s.evaluate(ctx, config)
	if err != nil {
		return 0, err
	}
	total := 0.0
	for i, cost := range costs {
		total += cost * s.queries[i].Weight
	}
	return total / float64(len(costs)), nil
}

func (s *session) evaluate(ctx context.Context, config []int) ([]float64, error) {
	key := configKey(config)
	if costs, ok := s.costs[key]; ok {
		return costs, nil
	}
	if err := s.tx.ResetHypothetical(ctx); err != nil {
		return nil, err
	}
	for _, i := range config {
		if _, _, err := s.tx.CreateHypotheticalIndex(ctx, s.candidates[i].definition); err != nil {
			return nil, err
		}
	}
	costs := make([]float64, len(s.queries))
	for i, query := range s.queries {
		cost, err := s.planCost(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("EXPLAIN of %q: %w", query.Text, err)
		}
		costs[i] = cost
	}
	s.costs[key] = costs
	return costs, nil
}

func (s *session) report(ctx context.Context, analysis *Analysis, before []float64, chosen []int) error {
	baseCost, err := s.average(ctx, nil)
	if err != nil {
		return err
	}
	analysis.BaseCost, analysis.FinalCost = baseCost, baseCost
	previous := baseCost
	for n, i := range chosen {
		after, err := s.average(ctx, chosen[:n+1])
		if err != nil {
			return err
		}
		alone, err := s.average(ctx, []int{i})
		if err != nil {
			return err
		}
		c := s.candidates[i]
		analysis.Recommendations = append(analysis.Recommendations, Recommendation{
			Definition: c.definition, Table: c.table.String(), Columns: c.columns, SizeBytes: c.size,
			LongText: c.longText, CostBefore: previous, CostAfter: after, CostAlone: alone,
		})
		previous, analysis.FinalCost = after, after
	}
	final, err := s.evaluate(ctx, chosen)
	if err != nil {
		return err
	}
	for i, query := range s.queries {
		analysis.Queries = append(analysis.Queries, QueryCost{Query: query.Query, Before: before[i], After: final[i]})
	}
	return s.tx.ResetHypothetical(ctx)
}

func configKey(config []int) string {
	sorted := append([]int(nil), config...)
	sort.Ints(sorted)
	parts := make([]string, len(sorted))
	for i, n := range sorted {
		parts[i] = strconv.Itoa(n)
	}
	return strings.Join(parts, ",")
}

func sortedKeys(set map[int]bool) []int {
	keys := make([]int, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	return keys
}
