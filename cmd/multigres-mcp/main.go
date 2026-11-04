package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/admin"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/config"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/db"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/tools"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "multigres-mcp:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.Args[1:], os.Getenv)
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprint(os.Stderr, config.Usage())
		return nil
	}
	if err != nil {
		return err
	}
	logger := log.New(os.Stderr, "multigres-mcp: ", log.LstdFlags)
	database, err := db.Open(db.Options{URL: cfg.DatabaseURL, Writable: cfg.Mode == config.Unrestricted})
	if err != nil {
		return err
	}
	defer database.Close()
	deps := &tools.Deps{Config: cfg, DB: database, Admin: admin.NewReader(cfg.AdminURL, cfg.AdminToken)}
	if cfg.Mode == config.Unrestricted {
		deps.Writer = admin.NewWriter(cfg.AdminURL, cfg.AdminToken)
	}
	logger.Printf("starting in %s mode, cluster tools %t", cfg.Mode, cfg.AdminURL != "")
	if tools.PasswordIsLoginName(cfg.DatabaseURL) {
		logger.Printf("the database password equals the user or database name, which tool results show, so results cannot hide it")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return tools.NewServer(deps, version).Run(ctx, &mcp.StdioTransport{})
}
