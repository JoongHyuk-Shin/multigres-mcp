package tools

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/admin"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/config"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/db"
)

type Deps struct {
	Config config.Config
	DB     *db.DB
	Admin  *admin.Reader
	Writer *admin.Writer

	redact    redactor
	verifying sync.Map
}

func NewServer(deps *Deps, version string) *mcp.Server {
	deps.redact = newRedactor(deps.Config.DatabaseURL, deps.Config.AdminURL, deps.Config.AdminToken)
	server := mcp.NewServer(&mcp.Implementation{Name: "multigres-mcp", Version: version}, nil)
	unrestricted := deps.Config.Mode == config.Unrestricted
	registerPostgres(server, deps)
	if unrestricted {
		registerPostgresWrites(server, deps)
	}
	if deps.Config.AdminURL != "" {
		registerCluster(server, deps)
		if unrestricted {
			registerClusterWrites(server, deps)
		}
	}
	return server
}

type annotation struct {
	readOnly, destructive, idempotent bool
}

var (
	reading    = annotation{readOnly: true, idempotent: true}
	changing   = annotation{destructive: true}
	adding     = annotation{}
	switchable = annotation{destructive: true, idempotent: true}
)

func (a annotation) mcp(title string) *mcp.ToolAnnotations {
	destructive, openWorld := a.destructive, false
	return &mcp.ToolAnnotations{
		Title:           title,
		ReadOnlyHint:    a.readOnly,
		DestructiveHint: &destructive,
		IdempotentHint:  a.idempotent,
		OpenWorldHint:   &openWorld,
	}
}

func add[In any](server *mcp.Server, deps *Deps, name, title, description string, kind annotation, fn func(context.Context, In, *output) error) {
	tool := &mcp.Tool{Name: name, Description: description, Annotations: kind.mcp(title)}
	mcp.AddTool(server, tool, func(ctx context.Context, _ *mcp.CallToolRequest, in In) (*mcp.CallToolResult, any, error) {
		out := output{redact: deps.redact}
		err := fn(ctx, in, &out)
		toolResult := &mcp.CallToolResult{}
		if err != nil {
			out.Tail("error: %s", describe(err))
			toolResult.IsError = true
		}
		toolResult.Content = []mcp.Content{&mcp.TextContent{Text: out.finish()}}
		return toolResult, nil, nil
	})
}

func describe(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		parts := []string{strings.Replace(err.Error(), pgErr.Error(), fmt.Sprintf("%s (SQLSTATE %s)", pgErr.Message, pgErr.Code), 1)}
		if pgErr.Detail != "" {
			parts = append(parts, "detail: "+pgErr.Detail)
		}
		if pgErr.Hint != "" {
			parts = append(parts, "hint: "+pgErr.Hint)
		}
		return strings.Join(parts, "; ")
	}
	var connectErr *connect.Error
	if errors.As(err, &connectErr) {
		return strings.Replace(err.Error(), connectErr.Error(), fmt.Sprintf("admin API error %s: %s", connectErr.Code(), connectErr.Message()), 1)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "the call ran out of time (" + err.Error() + ")"
	}
	return err.Error()
}

func (d *Deps) notes(out *output, report db.Report) {
	for _, note := range report.Notes() {
		out.Tail("note: %s", note)
	}
}
