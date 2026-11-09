package tools

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/admin"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/advisor"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/db"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/diag"
)

const (
	defaultTopLimit = 10
	topPreview      = 2000
	diagnosePreview = 500
	maxGivenQueries = 10
	diagLogBytes    = 64 * 1024
	diagLogLines    = 5
)

var logLevels = []string{"PANIC", "FATAL", "ERROR", "WARNING"}

type topQueriesInput struct {
	SortBy string `json:"sort_by,omitempty" jsonschema:"Ranking: total_time, mean_time, or resources for queries using more than 5% of any resource (default resources)"`
	Limit  int    `json:"limit,omitempty" jsonschema:"Number of queries when ranking by total_time or mean_time (default 10)"`
	Source string `json:"source,omitempty" jsonschema:"both (default), pg_stat_statements, or gateway for the query statistics each Multigres gateway keeps"`
	Node   string `json:"node,omitempty" jsonschema:"Read pg_stat_statements on this node over a direct connection (cell/name, name, or primary); default is through the gateway"`
}

type workloadIndexesInput struct {
	MaxIndexSizeMB int `json:"max_index_size_mb,omitempty" jsonschema:"Size budget for all recommended indexes together, in MB (default 10000)"`
}

type queryIndexesInput struct {
	Queries        []string `json:"queries" jsonschema:"Up to 10 SELECT queries, with values or with $1-style placeholders"`
	MaxIndexSizeMB int      `json:"max_index_size_mb,omitempty" jsonschema:"Size budget for all recommended indexes together, in MB (default 10000)"`
}

type diagnoseInput struct {
	Checks []string `json:"checks,omitempty" jsonschema:"Checks to run, in any order (default all): bloat, vacuum, locks, activity, waits, slow_queries, missing_indexes, cache, log, index_health, sequences, replication, constraints"`
	Node   string   `json:"node,omitempty" jsonschema:"Run the checks on this node over a direct connection (cell/name, name, or primary); default is through the gateway"`
}

type gatewayQuery struct {
	Gateway       string  `json:"gateway"`
	NormalizedSQL string  `json:"normalized_sql"`
	Calls         uint64  `json:"calls"`
	Errors        uint64  `json:"errors"`
	TotalMs       float64 `json:"total_ms"`
	MeanMs        float64 `json:"mean_ms"`
	P50Ms         float64 `json:"p50_ms"`
	P99Ms         float64 `json:"p99_ms"`
	Rows          uint64  `json:"total_rows"`
	LastSeen      string  `json:"last_seen"`
}

type recommendation struct {
	Definition   string   `json:"definition"`
	Table        string   `json:"table"`
	Columns      []string `json:"columns"`
	SizeBytes    int64    `json:"estimated_size_bytes"`
	CostBefore   float64  `json:"workload_cost_before"`
	CostAfter    float64  `json:"workload_cost_after"`
	CostAlone    float64  `json:"workload_cost_with_only_this_index"`
	LongTextNote string   `json:"note,omitempty"`
}

func registerDiagnostics(server *mcp.Server, d *Deps) {
	add(server, d, "get_top_queries", "Get Top Queries", "Report the slowest or most resource-intensive queries from pg_stat_statements and from the query statistics each Multigres gateway keeps. The two sources cover different periods and are shown separately.", reading, d.topQueries)
	add(server, d, "analyze_workload_indexes", "Analyze Workload Indexes", "Recommend indexes for the current workload in pg_stat_statements, by comparing planner costs with hypothetical indexes (hypopg). Nothing is created.", reading, d.workloadIndexes)
	add(server, d, "analyze_query_indexes", "Analyze Query Indexes", "Recommend indexes for up to 10 given SELECT queries, by comparing planner costs with hypothetical indexes (hypopg). Nothing is created.", reading, d.queryIndexes)
	add(server, d, "diagnose", "Diagnose Database", "Run health checks (bloat, vacuum, locks, activity, waits, slow queries, missing indexes, cache, log, index health, sequences, replication, constraints), after the statistics period of the node that answered.", reading, d.diagnose)
}

func (d *Deps) topQueries(ctx context.Context, in topQueriesInput, out *output) error {
	sortBy, limit, source, err := topQueriesOptions(in)
	if err != nil {
		return err
	}
	var errs []error
	if source != "gateway" {
		err := d.onTarget(ctx, out, in.Node, d.readPlan(), func(ctx context.Context, tx *db.Tx, answeredBy string) error {
			diag.Period(ctx, tx, out, answeredBy, describe)
			return diag.TopStatements(ctx, tx, out, sortBy, limit, topPreview)
		})
		if err != nil {
			out.Line("pg_stat_statements: %s", describe(err))
			errs = append(errs, err)
		}
	}
	if source != "pg_stat_statements" {
		out.Line("")
		if err := d.gatewayTop(ctx, out, sortBy, limit); err != nil {
			out.Line("gateway query statistics: %s", describe(err))
			errs = append(errs, err)
		}
	}
	if source == "both" && len(errs) < 2 {
		return nil
	}
	return errors.Join(errs...)
}

func topQueriesOptions(in topQueriesInput) (string, int, string, error) {
	sortBy := in.SortBy
	if sortBy == "" {
		sortBy = "resources"
	}
	if sortBy != "total_time" && sortBy != "mean_time" && sortBy != "resources" {
		return "", 0, "", fmt.Errorf("sort_by must be total_time, mean_time or resources, got %q", in.SortBy)
	}
	limit := in.Limit
	if limit == 0 {
		limit = defaultTopLimit
	}
	if limit < 0 {
		return "", 0, "", errors.New("limit must be positive")
	}
	source := in.Source
	if source == "" {
		source = "both"
	}
	if source != "both" && source != "pg_stat_statements" && source != "gateway" {
		return "", 0, "", fmt.Errorf("source must be both, pg_stat_statements or gateway, got %q", in.Source)
	}
	return sortBy, limit, source, nil
}

func (d *Deps) gatewayTop(ctx context.Context, sink diag.Sink, sortBy string, limit int) error {
	if d.Admin == nil {
		return admin.ErrNotConfigured
	}
	gateways, err := adminValue(ctx, d.Admin.Gateways)
	if err != nil {
		return err
	}
	answers := make([]*multiadminpb.GetGatewayQueriesResponse, len(gateways))
	errs := make([]error, len(gateways))
	var wg sync.WaitGroup
	for i, gateway := range gateways {
		wg.Go(func() {
			answers[i], errs[i] = adminValue(ctx, func(ctx context.Context) (*multiadminpb.GetGatewayQueriesResponse, error) {
				return d.Admin.GatewayQueries(ctx, gateway.GetId())
			})
		})
	}
	wg.Wait()
	rows := []gatewayQuery{}
	answered := 0
	for i, gateway := range gateways {
		name := gateway.GetId().GetCell() + "/" + gateway.GetId().GetName()
		if errs[i] != nil {
			sink.Line("gateway %s: %s", name, describe(errs[i]))
			continue
		}
		answered++
		snapshot := answers[i].GetSnapshot()
		sink.Line("gateway %s: %d fingerprints returned, %d tracked", name, len(snapshot.GetQueries()), snapshot.GetTrackedFingerprints())
		for _, query := range snapshot.GetQueries() {
			rows = append(rows, gatewayQuery{
				Gateway: name, NormalizedSQL: query.GetNormalizedSql(), Calls: query.GetCalls(), Errors: query.GetErrors(),
				TotalMs: milliseconds(query.GetTotalDuration()), MeanMs: milliseconds(query.GetAverageDuration()),
				P50Ms: milliseconds(query.GetP50Duration()), P99Ms: milliseconds(query.GetP99Duration()),
				Rows: query.GetTotalRows(), LastSeen: query.GetLastSeen().AsTime().UTC().Format("2006-01-02T15:04:05Z"),
			})
		}
	}
	if answered == 0 {
		return fmt.Errorf("none of the %d gateways in the admin server's list answered", len(gateways))
	}
	key := "total"
	if sortBy == "mean_time" {
		key = "mean"
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].MeanMs > rows[j].MeanMs })
	} else {
		sort.SliceStable(rows, func(i, j int) bool { return rows[i].TotalMs > rows[j].TotalMs })
	}
	if sortBy == "resources" {
		sink.Line("the resources ranking exists only in pg_stat_statements; gateway rows are ranked by total time")
	}
	rows = rows[:min(limit, len(rows))]
	sink.JSON(fmt.Sprintf("gateway query statistics, %d queries with the highest %s time across all gateways", len(rows), key), rows)
	sink.Line("normalized_sql may have been cut by the gateway without a mark (--query-metrics-sql-max-bytes, default 4096); do not pass it to explain_query or the index advisors; use the full pg_stat_statements text (read by queryid when query_length is longer than query) or the full statement instead")
	return nil
}

func milliseconds(value *durationpb.Duration) float64 {
	return float64(value.AsDuration().Microseconds()) / 1000
}

func (d *Deps) onTarget(ctx context.Context, out *output, node string, plan db.Plan, fn func(context.Context, *db.Tx, string) error) error {
	if node == "" {
		nodes := d.poolerLookup(ctx)
		report, err := d.DB.Read(ctx, plan, func(ctx context.Context, tx *db.Tx) error {
			return fn(ctx, tx, nodes.answeredBy(tx.DataDirectory()))
		})
		d.notes(out, report)
		return err
	}
	_, err := d.readNode(ctx, node, plan, func(ctx context.Context, tx *db.Tx, name string) error {
		return fn(ctx, tx, name+" (direct connection)")
	})
	return err
}

func (d *Deps) diagnose(ctx context.Context, in diagnoseInput, out *output) error {
	if err := diag.Validate(in.Checks); err != nil {
		return err
	}
	callbacks := diag.Callbacks{
		SlowQueries: func(ctx context.Context, tx *db.Tx, sink diag.Sink) error {
			statementsErr := diag.TopStatements(ctx, tx, sink, "resources", defaultTopLimit, diagnosePreview)
			gatewayErr := d.gatewayTop(ctx, sink, "total_time", defaultTopLimit)
			if statementsErr != nil && gatewayErr != nil {
				return errors.Join(fmt.Errorf("pg_stat_statements: %w", statementsErr), fmt.Errorf("gateway query statistics: %w", gatewayErr))
			}
			if statementsErr != nil {
				sink.Line("pg_stat_statements: %s", describe(statementsErr))
			}
			if gatewayErr != nil {
				sink.Line("gateway query statistics: %s", describe(gatewayErr))
			}
			sink.Line("verdict: none (no threshold in postgres-mcp)")
			return nil
		},
		Log:          func(ctx context.Context, sink diag.Sink) error { return d.logCheck(ctx, in.Node, sink) },
		Describe:     describe,
		CheckTimeout: d.Config.SQLTimeout + time.Second,
	}
	plan := d.readPlan()
	plan.CallTimeout = time.Duration(diag.Count(in.Checks)+1) * callbacks.CheckTimeout
	return d.onTarget(ctx, out, in.Node, plan, func(ctx context.Context, tx *db.Tx, answeredBy string) error {
		diag.Period(ctx, tx, out, answeredBy, describe)
		out.Line("")
		return diag.Run(ctx, tx, in.Checks, out, callbacks)
	})
}

func (d *Deps) logCheck(ctx context.Context, node string, sink diag.Sink) error {
	text, name, err := d.nodeLog(ctx, node, diagLogBytes)
	if err != nil {
		return err
	}
	counts := map[string]int{}
	last := []string{}
	for _, line := range strings.Split(text, "\n") {
		for _, level := range logLevels {
			if strings.Contains(line, level+":") {
				counts[level]++
				last = append(last, line)
				break
			}
		}
	}
	sink.Line("node %s, last %d bytes of the Postgres log (direct connection)", name, len(text))
	sink.Line("PANIC %d, FATAL %d, ERROR %d, WARNING %d (lines containing the word followed by a colon)", counts["PANIC"], counts["FATAL"], counts["ERROR"], counts["WARNING"])
	sink.JSON("last matching lines", last[max(len(last)-diagLogLines, 0):])
	sink.Line("verdict: none (no threshold in postgres-mcp); call read_node_log for more")
	return nil
}

func (d *Deps) workloadIndexes(ctx context.Context, in workloadIndexesInput, out *output) error {
	return d.recommend(ctx, out, in.MaxIndexSizeMB, nil)
}

func (d *Deps) queryIndexes(ctx context.Context, in queryIndexesInput, out *output) error {
	if len(in.Queries) == 0 || len(in.Queries) > maxGivenQueries {
		return fmt.Errorf("give between 1 and %d queries", maxGivenQueries)
	}
	return d.recommend(ctx, out, in.MaxIndexSizeMB, in.Queries)
}

func (d *Deps) recommend(ctx context.Context, out *output, budgetMB int, given []string) error {
	if budgetMB < 0 {
		return errors.New("max_index_size_mb must be positive")
	}
	if budgetMB == 0 {
		budgetMB = advisor.DefaultBudgetMB
	}
	plan := d.readPlan()
	plan.Hypopg = true
	plan.CallTimeout = advisor.CallTimeout
	nodes := d.poolerLookup(ctx)
	report, err := d.DB.Read(ctx, plan, func(ctx context.Context, tx *db.Tx) error {
		if !tx.HasHypopg() {
			return db.ErrNoHypopg
		}
		diag.Period(ctx, tx, out, nodes.answeredBy(tx.DataDirectory()), describe)
		out.Line("")
		queries, err := workloadQueries(ctx, tx, out, given)
		if err != nil {
			return err
		}
		analysis, err := advisor.Recommend(ctx, tx, queries, advisor.Options{BudgetMB: budgetMB, Describe: describe})
		if err != nil {
			return err
		}
		writeRecommendations(out, analysis)
		return nil
	})
	d.notes(out, report)
	return err
}

func workloadQueries(ctx context.Context, tx *db.Tx, out *output, given []string) ([]advisor.Query, error) {
	if given != nil {
		queries := make([]advisor.Query, len(given))
		for i, text := range given {
			queries[i] = advisor.Query{Text: text, Weight: 1}
		}
		out.Line("workload: %d given queries, each with weight 1", len(given))
		return queries, nil
	}
	schema, err := diag.ExtensionSchema(ctx, tx, "pg_stat_statements")
	if err != nil {
		return nil, err
	}
	if schema == "" {
		return nil, diag.ErrNoStatements
	}
	queries, err := advisor.Workload(ctx, tx, schema)
	if err != nil {
		return nil, err
	}
	out.Line("workload: %d queries of this database from pg_stat_statements (at least 50 calls and 5 ms mean, most total time first, up to 100; postgres-mcp defaults), weight = calls x mean ms", len(queries))
	return queries, nil
}

func writeRecommendations(out *output, analysis *advisor.Analysis) {
	out.Line("queries analyzed: %d, skipped: %d, candidate indexes: %d", analysis.Analyzed, len(analysis.Skipped), analysis.Candidates)
	if len(analysis.NoStatistics) > 0 {
		out.JSON("tables without planner statistics in pg_stats (their costs use planner defaults; ANALYZE them on the primary)", analysis.NoStatistics)
	}
	if len(analysis.Skipped) > 0 {
		out.JSON("skipped queries", analysis.Skipped)
	}
	if len(analysis.NotIndexable) > 0 {
		out.JSON("candidate indexes left out because Postgres cannot build them with btree", analysis.NotIndexable)
	}
	if analysis.TimedOut {
		out.Line("the search stopped at its %s limit; the recommendations below are the ones chosen before that", advisor.SearchLimit)
	}
	if len(analysis.Recommendations) == 0 {
		out.Line("no recommendation: no single index lowers the workload cost by more than 10%% (postgres-mcp threshold)")
	} else {
		recommendations := make([]recommendation, len(analysis.Recommendations))
		for i, r := range analysis.Recommendations {
			recommendations[i] = recommendation{Definition: r.Definition, Table: r.Table, Columns: r.Columns, SizeBytes: r.SizeBytes, CostBefore: r.CostBefore, CostAfter: r.CostAfter, CostAlone: r.CostAlone}
			if r.LongText {
				recommendations[i].LongTextNote = "long_text_column: a text column whose average width is small now; the index grows if longer values arrive"
			}
		}
		out.JSON("recommended indexes, in the order the search chose them (sizes are hypopg estimates, close to a btree without deduplication; on a column with many repeated values the real btree can be several times smaller)", recommendations)
	}
	out.Line("workload cost (average over queries of plan total cost x weight): %.2f before, %.2f after", analysis.BaseCost, analysis.FinalCost)
	out.JSON("plan total cost per query, before and after all recommended indexes", analysis.Queries)
	out.Line("costs are planner estimates; a query with $n placeholders uses a generic plan, whose cost can differ from the plan for real values")
}
