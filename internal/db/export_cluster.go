//go:build cluster

package db

import "github.com/jackc/pgx/v5"

func ReadConnConfig(url string) (*pgx.ConnConfig, error) {
	return connConfig(url, true, nil)
}

func BreakEndReset() (restore func()) {
	endResetTemplate = "SELECT %[1]s.hypopg_reset(), 1/0"
	return func() { endResetTemplate = hypopgResetTemplate }
}

func BreakIndexSize() (restore func()) {
	sizeTemplate = "SELECT %[1]s.hypopg_relation_size($1) / 0"
	return func() { sizeTemplate = hypopgSizeTemplate }
}

func SlowIndexSize() (restore func()) {
	sizeTemplate = "SELECT %[1]s.hypopg_relation_size($1) FROM pg_catalog.pg_sleep(5)"
	return func() { sizeTemplate = hypopgSizeTemplate }
}
