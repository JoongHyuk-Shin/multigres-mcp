package db

import (
	"crypto/tls"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestURLCannotChangeTheProtocol(t *testing.T) {
	url := "postgres://u:p@gateway:15432/db?default_query_exec_mode=simple_protocol"
	for _, readOnly := range []bool{true, false} {
		cfg, err := connConfig(url, readOnly, nil)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.DefaultQueryExecMode != pgx.QueryExecModeExec {
			t.Errorf("read only %t: the protocol is %v", readOnly, cfg.DefaultQueryExecMode)
		}
	}
}

func TestRetargetMovesEveryAttemptToTheNode(t *testing.T) {
	attempts := map[string]int{"prefer": 2, "allow": 2, "disable": 1, "require": 1, "verify-full": 1}
	for mode, want := range attempts {
		cfg, err := connConfig("postgres://u:p@gateway:15432/db?sslmode="+mode, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		retarget(cfg, "node-a", 25432)
		targets := []struct {
			host string
			port uint16
			tls  *tls.Config
		}{{cfg.Host, cfg.Port, cfg.TLSConfig}}
		for _, fallback := range cfg.Fallbacks {
			targets = append(targets, struct {
				host string
				port uint16
				tls  *tls.Config
			}{fallback.Host, fallback.Port, fallback.TLSConfig})
		}
		if len(targets) != want {
			t.Fatalf("%s: %d attempts, want %d", mode, len(targets), want)
		}
		for _, target := range targets {
			if target.host != "node-a" || target.port != 25432 {
				t.Fatalf("%s: an attempt goes to %s:%d", mode, target.host, target.port)
			}
			if target.tls != nil && target.tls.ServerName != "node-a" {
				t.Fatalf("%s: TLS checks the name %q", mode, target.tls.ServerName)
			}
		}
	}
}
