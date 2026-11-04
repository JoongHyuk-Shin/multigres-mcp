package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"
)

type Mode string

const (
	Restricted   Mode = "restricted"
	Unrestricted Mode = "unrestricted"
)

type Config struct {
	DatabaseURL string
	AdminURL    string
	AdminToken  string
	NodeHostMap map[string]string
	Mode        Mode
	SQLTimeout  time.Duration
}

type option struct {
	flag    string
	env     string
	initial string
	help    string
}

var options = []option{
	{"database-url", "DATABASE_URI", "", "gateway connection string (required)"},
	{"admin-url", "MULTIGRES_ADMIN_URL", "", "multiadmin HTTP address; cluster tools are registered only when set"},
	{"admin-token", "MULTIGRES_ADMIN_TOKEN", "", "bearer token for multiadmin --enable-auth"},
	{"node-host-map", "MULTIGRES_NODE_HOST_MAP", "", "reported=reachable host pairs for direct node connections, comma separated"},
	{"access-mode", "MULTIGRES_MCP_ACCESS_MODE", string(Restricted), "restricted or unrestricted"},
	{"sql-timeout", "MULTIGRES_MCP_SQL_TIMEOUT", "25s", "statement timeout for the SQL every tool sends; keep it under the multipooler --connpool-user-reserved-inactivity-timeout (default 30s), which ends a transaction's connection when one statement runs that long"},
}

func Load(args []string, getenv func(string) string) (Config, error) {
	values, err := resolve(args, getenv)
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		DatabaseURL: values["database-url"],
		AdminURL:    values["admin-url"],
		AdminToken:  values["admin-token"],
		Mode:        Mode(values["access-mode"]),
	}
	if cfg.DatabaseURL == "" {
		return Config{}, errors.New("--database-url or DATABASE_URI is required")
	}
	if cfg.Mode != Restricted && cfg.Mode != Unrestricted {
		return Config{}, fmt.Errorf("--access-mode must be restricted or unrestricted, got %q", cfg.Mode)
	}
	cfg.SQLTimeout, err = time.ParseDuration(values["sql-timeout"])
	if err != nil || cfg.SQLTimeout < time.Millisecond {
		return Config{}, fmt.Errorf("--sql-timeout must be a duration of at least 1ms, got %q", values["sql-timeout"])
	}
	cfg.NodeHostMap, err = parseHostMap(values["node-host-map"])
	if err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) MapNodeHost(reported string) string {
	if reachable, ok := c.NodeHostMap[reported]; ok {
		return reachable
	}
	return reported
}

func Usage() string {
	set, _ := newFlagSet()
	var text strings.Builder
	set.SetOutput(&text)
	text.WriteString("Usage of multigres-mcp:\n")
	set.PrintDefaults()
	return text.String()
}

func newFlagSet() (*flag.FlagSet, map[string]*string) {
	set := flag.NewFlagSet("multigres-mcp", flag.ContinueOnError)
	set.SetOutput(io.Discard)
	pointers := make(map[string]*string, len(options))
	for _, opt := range options {
		pointers[opt.flag] = set.String(opt.flag, "", fmt.Sprintf("%s (env %s, default %q)", opt.help, opt.env, opt.initial))
	}
	return set, pointers
}

func resolve(args []string, getenv func(string) string) (map[string]string, error) {
	set, pointers := newFlagSet()
	if err := set.Parse(args); err != nil {
		return nil, err
	}
	given := map[string]bool{}
	set.Visit(func(f *flag.Flag) { given[f.Name] = true })
	values := make(map[string]string, len(options))
	for _, opt := range options {
		switch {
		case given[opt.flag]:
			values[opt.flag] = *pointers[opt.flag]
		case getenv(opt.env) != "":
			values[opt.flag] = getenv(opt.env)
		default:
			values[opt.flag] = opt.initial
		}
	}
	return values, nil
}

func parseHostMap(text string) (map[string]string, error) {
	hostMap := map[string]string{}
	if strings.TrimSpace(text) == "" {
		return hostMap, nil
	}
	for _, pair := range strings.Split(text, ",") {
		reported, reachable, found := strings.Cut(pair, "=")
		reported, reachable = strings.TrimSpace(reported), strings.TrimSpace(reachable)
		if !found || reported == "" || reachable == "" {
			return nil, fmt.Errorf("--node-host-map pair %q must be reported=reachable", pair)
		}
		if _, duplicate := hostMap[reported]; duplicate {
			return nil, fmt.Errorf("--node-host-map names %q twice", reported)
		}
		hostMap[reported] = reachable
	}
	return hostMap, nil
}
