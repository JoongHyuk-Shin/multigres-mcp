package diag

import (
	"context"
	"sort"
	"strings"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/db"
)

type indexInfo struct {
	schema, table, name, using string
	columns                    []string
	unique, primary, valid     bool
	expressions, predicate     *string
}

func indexHealth(ctx context.Context, tx *db.Tx, sink Sink, _ Callbacks) error {
	indexes, err := loadIndexes(ctx, tx)
	if err != nil {
		return err
	}
	invalid := []string{}
	for _, index := range indexes {
		if !index.valid {
			invalid = append(invalid, index.schema+"."+index.name+" on "+index.table)
		}
	}
	sink.JSON("invalid indexes", invalid)
	duplicated := duplicates(indexes)
	sink.JSON("duplicate indexes (the first is covered by the second)", duplicated)
	unused, err := show(ctx, tx, sink, "rarely used indexes (50 scans or fewer, postgres-mcp threshold)", unusedIndexesQuery, maxIndexScans)
	if err != nil {
		return err
	}
	bloated, err := show(ctx, tx, sink, "bloated btree indexes (100MB or more wasted, postgres-mcp threshold)", indexBloatQuery, minIndexBloat)
	if err != nil {
		return err
	}
	if len(invalid) > 0 || len(duplicated) > 0 || len(unused) > 0 || len(bloated) > 0 {
		sink.Line("verdict: warning (postgres-mcp thresholds)")
	} else {
		sink.Line("verdict: healthy (postgres-mcp thresholds)")
	}
	return nil
}

func loadIndexes(ctx context.Context, tx *db.Tx) ([]indexInfo, error) {
	rows, err := tx.Query(ctx, indexesQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var indexes []indexInfo
	for rows.Next() {
		var index indexInfo
		var columns string
		if err := rows.Scan(&index.schema, &index.table, &index.name, &columns, &index.using, &index.unique, &index.primary, &index.valid, &index.expressions, &index.predicate); err != nil {
			return nil, err
		}
		for _, column := range strings.Split(strings.ReplaceAll(columns, ") WHERE (", " WHERE "), ", ") {
			index.columns = append(index.columns, strings.Trim(column, `"`))
		}
		indexes = append(indexes, index)
	}
	return indexes, rows.Err()
}

func duplicates(indexes []indexInfo) [][2]string {
	byTable := map[[2]string][]indexInfo{}
	for _, index := range indexes {
		key := [2]string{index.schema, index.table}
		byTable[key] = append(byTable[key], index)
	}
	type pair struct {
		unneeded, covering indexInfo
	}
	var found []pair
	for _, index := range indexes {
		if !index.valid || index.primary || index.unique {
			continue
		}
		for _, other := range byTable[[2]string{index.schema, index.table}] {
			if coversDuplicate(other, index) {
				found = append(found, pair{index, other})
				break
			}
		}
	}
	sort.SliceStable(found, func(i, j int) bool {
		if found[i].unneeded.table != found[j].unneeded.table {
			return found[i].unneeded.table < found[j].unneeded.table
		}
		return strings.Join(found[i].unneeded.columns, ",") < strings.Join(found[j].unneeded.columns, ",")
	})
	names := make([][2]string, len(found))
	for i, p := range found {
		names[i] = [2]string{p.unneeded.schema + "." + p.unneeded.name, p.covering.schema + "." + p.covering.name}
	}
	return names
}

func coversDuplicate(covering, index indexInfo) bool {
	if !covering.valid || covering.name == index.name || covering.using != index.using ||
		!sameText(covering.expressions, index.expressions) || !sameText(covering.predicate, index.predicate) ||
		!hasPrefix(covering.columns, index.columns) {
		return false
	}
	return !sameColumns(covering.columns, index.columns) || index.name > covering.name || covering.primary || covering.unique
}

func hasPrefix(columns, prefix []string) bool {
	if len(prefix) > len(columns) {
		return false
	}
	return sameColumns(columns[:len(prefix)], prefix)
}

func sameColumns(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameText(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func sequences(ctx context.Context, tx *db.Tx, sink Sink, _ Callbacks) error {
	rows, err := show(ctx, tx, sink, "sequences used by column defaults, most used first (limit is the column type's maximum, as in postgres-mcp)", sequencesQuery)
	if err != nil {
		return err
	}
	danger := 0
	for _, row := range rows {
		if used, ok := row["fraction_used"].(float64); ok && used > sequenceThreshold {
			danger++
		}
	}
	if danger > 0 {
		sink.Line("verdict: warning (%d sequences above 90%% of the column type's maximum, postgres-mcp threshold)", danger)
	} else {
		sink.Line("verdict: healthy (postgres-mcp threshold 90%%)")
	}
	return nil
}
