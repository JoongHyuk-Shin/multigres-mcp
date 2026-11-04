package config

import (
	"testing"
	"time"
)

func envOf(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(nil, envOf(map[string]string{"DATABASE_URI": "postgres://u:p@h:1/d"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DatabaseURL != "postgres://u:p@h:1/d" {
		t.Fatalf("database url %q", cfg.DatabaseURL)
	}
	if cfg.Mode != Restricted {
		t.Fatalf("mode %q", cfg.Mode)
	}
	if cfg.SQLTimeout != 25*time.Second {
		t.Fatalf("timeout %v", cfg.SQLTimeout)
	}
	if cfg.AdminURL != "" || cfg.AdminToken != "" || len(cfg.NodeHostMap) != 0 {
		t.Fatalf("unexpected optional values %+v", cfg)
	}
}

func TestArgumentBeatsEnvironment(t *testing.T) {
	env := envOf(map[string]string{
		"DATABASE_URI":              "postgres://env",
		"MULTIGRES_ADMIN_URL":       "http://env:15000",
		"MULTIGRES_ADMIN_TOKEN":     "env-token",
		"MULTIGRES_MCP_ACCESS_MODE": "restricted",
		"MULTIGRES_MCP_SQL_TIMEOUT": "5s",
		"MULTIGRES_NODE_HOST_MAP":   "envhost=1.1.1.1",
	})
	args := []string{
		"--database-url", "postgres://arg",
		"--admin-url", "http://arg:15000",
		"--admin-token", "arg-token",
		"--access-mode", "unrestricted",
		"--sql-timeout", "7s",
		"--node-host-map", "localhost=172.18.0.2",
	}
	cfg, err := Load(args, env)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.DatabaseURL != "postgres://arg" || cfg.AdminURL != "http://arg:15000" || cfg.AdminToken != "arg-token" {
		t.Fatalf("argument lost %+v", cfg)
	}
	if cfg.Mode != Unrestricted || cfg.SQLTimeout != 7*time.Second {
		t.Fatalf("argument lost %+v", cfg)
	}
	if cfg.NodeHostMap["localhost"] != "172.18.0.2" || len(cfg.NodeHostMap) != 1 {
		t.Fatalf("host map %v", cfg.NodeHostMap)
	}
}

func TestEnvironmentUsedWithoutArgument(t *testing.T) {
	cfg, err := Load(nil, envOf(map[string]string{
		"DATABASE_URI":              "postgres://env",
		"MULTIGRES_MCP_ACCESS_MODE": "unrestricted",
		"MULTIGRES_MCP_SQL_TIMEOUT": "5s",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Mode != Unrestricted || cfg.SQLTimeout != 5*time.Second {
		t.Fatalf("environment lost %+v", cfg)
	}
}

func TestRejectsBadValues(t *testing.T) {
	cases := map[string][]string{
		"missing database url": {},
		"bad mode":             {"--database-url", "x", "--access-mode", "readonly"},
		"zero timeout":         {"--database-url", "x", "--sql-timeout", "0s"},
		"under a millisecond":  {"--database-url", "x", "--sql-timeout", "500us"},
		"bad timeout":          {"--database-url", "x", "--sql-timeout", "soon"},
		"pair without equals":  {"--database-url", "x", "--node-host-map", "localhost"},
		"empty right side":     {"--database-url", "x", "--node-host-map", "localhost="},
		"duplicate left side":  {"--database-url", "x", "--node-host-map", "a=1,a=2"},
	}
	for name, args := range cases {
		if _, err := Load(args, envOf(nil)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestNodeHostMapIsExactMatch(t *testing.T) {
	cfg, err := Load([]string{"--database-url", "x", "--node-host-map", "localhost=172.18.0.2, pooler-1.svc = 10.0.0.5"}, envOf(nil))
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"localhost":       "172.18.0.2",
		"pooler-1.svc":    "10.0.0.5",
		"pooler-2.svc":    "pooler-2.svc",
		"LOCALHOST":       "LOCALHOST",
		"localhost.local": "localhost.local",
		"x.pooler-1.svc":  "x.pooler-1.svc",
	}
	for reported, want := range cases {
		if got := cfg.MapNodeHost(reported); got != want {
			t.Errorf("MapNodeHost(%q) = %q, want %q", reported, got, want)
		}
	}
}
