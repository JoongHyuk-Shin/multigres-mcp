package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/config"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/db"
)

const (
	testPassword = "pa:ss/w@rd"
	testToken    = "tok-123456"
	testAdmin    = "http://admin.internal:15000"
)

var designAnnotations = map[string]string{
	"list_schemas": "r", "list_objects": "r", "get_object_details": "r", "execute_sql": "r", "explain_query": "r",
	"get_top_queries": "r", "analyze_workload_indexes": "r", "analyze_query_indexes": "r", "diagnose": "r",
	"execute_write_sql": "d",
	"cluster_status":    "r", "list_backups": "r", "verify_backups": "r", "get_backup_job_status": "r", "read_node_log": "r",
	"switch_primary": "d", "create_backup": "-", "expire_backups": "d", "set_postgres_restarts": "di",
}

func session(t *testing.T, server *mcp.Server) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	clientSide, serverSide := mcp.NewInMemoryTransports()
	if _, err := server.Connect(ctx, serverSide, nil); err != nil {
		t.Fatal(err)
	}
	client, err := mcp.NewClient(&mcp.Implementation{Name: "test"}, nil).Connect(ctx, clientSide, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func testConfig(mode config.Mode, adminURL string) config.Config {
	return config.Config{
		DatabaseURL: "postgres://postgres:" + strings.ReplaceAll(testPassword, "/", "%2F") + "@localhost:15432/postgres",
		AdminURL:    adminURL,
		AdminToken:  testToken,
		Mode:        mode,
	}
}

func TestToolSetFollowsModeAndAdminURL(t *testing.T) {
	cases := []struct {
		mode     config.Mode
		adminURL string
		count    int
	}{
		{config.Restricted, "", 9},
		{config.Unrestricted, "", 10},
		{config.Restricted, testAdmin, 14},
		{config.Unrestricted, testAdmin, 19},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s admin=%t", tc.mode, tc.adminURL != ""), func(t *testing.T) {
			listed, err := session(t, NewServer(&Deps{Config: testConfig(tc.mode, tc.adminURL)}, "test")).ListTools(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			var names []string
			for _, tool := range listed.Tools {
				names = append(names, tool.Name)
				want, known := designAnnotations[tool.Name]
				if !known {
					t.Errorf("tool %s is not in the design", tool.Name)
					continue
				}
				if got := annotationCode(tool.Annotations); got != want {
					t.Errorf("%s annotations = %s, want %s", tool.Name, got, want)
				}
				if tc.mode == config.Restricted && want != "r" {
					t.Errorf("%s changes things but is registered in restricted mode", tool.Name)
				}
			}
			if len(names) != tc.count {
				t.Fatalf("%d tools %v, want %d", len(names), names, tc.count)
			}
		})
	}
}

func annotationCode(a *mcp.ToolAnnotations) string {
	if a.OpenWorldHint == nil || *a.OpenWorldHint || a.DestructiveHint == nil {
		return "open world or unset"
	}
	switch {
	case a.ReadOnlyHint && !*a.DestructiveHint && a.IdempotentHint:
		return "r"
	case !a.ReadOnlyHint && *a.DestructiveHint && a.IdempotentHint:
		return "di"
	case !a.ReadOnlyHint && *a.DestructiveHint && !a.IdempotentHint:
		return "d"
	case !a.ReadOnlyHint && !*a.DestructiveHint && !a.IdempotentHint:
		return "-"
	}
	return fmt.Sprintf("%+v", *a)
}

type textInput struct {
	Text string `json:"text"`
}

func callText(t *testing.T, write func(*output, string) error, text string) (string, bool) {
	t.Helper()
	deps := &Deps{Config: testConfig(config.Restricted, testAdmin+"/")}
	server := NewServer(deps, "test")
	add(server, deps, "echo", "Echo", "test", reading, func(_ context.Context, in textInput, out *output) error {
		return write(out, in.Text)
	})
	answer, err := session(t, server).CallTool(context.Background(), &mcp.CallToolParams{Name: "echo", Arguments: textInput{Text: text}})
	if err != nil {
		t.Fatal(err)
	}
	return answer.Content[0].(*mcp.TextContent).Text, answer.IsError
}

func TestResultsHideConnectionSecrets(t *testing.T) {
	leak := "url " + testAdmin + "/ host admin.internal:15000 token " + testToken + " password " + testPassword +
		" escaped " + strings.ReplaceAll(testPassword, "/", "%2F")
	for _, path := range []string{"result", "error"} {
		text, isError := callText(t, func(out *output, text string) error {
			if path == "error" {
				return fmt.Errorf("failed: %s", text)
			}
			out.Line("%s", text)
			return nil
		}, leak)
		if isError != (path == "error") {
			t.Errorf("%s path: IsError = %t", path, isError)
		}
		for _, secret := range []string{"admin.internal", testToken, testPassword, "pa:ss%2Fw@rd"} {
			if strings.Contains(text, secret) {
				t.Errorf("%s path leaked %q: %s", path, secret, text)
			}
		}
	}
}

func TestPoolerHostSurvivesAnAdminServerOnTheSameHost(t *testing.T) {
	r := newRedactor("postgres://app:secretpw@localhost:15432/appdb", "http://localhost:15000", "")
	text := "pooler zone1 host localhost port 25432; admin http://localhost:15000 and localhost:15000"
	if got, want := r.apply(text), "pooler zone1 host localhost port 25432; admin *** and ***"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestErrorsKeepTheirCodesAndDetails(t *testing.T) {
	pgErr := &pgconn.PgError{Code: "23505", Message: `duplicate key value violates unique constraint "orders_pkey"`, Detail: "Key (id)=(1) already exists.", Hint: "pick another id"}
	adminErr := connect.NewError(connect.CodeUnauthenticated, errors.New("token expired"))
	cases := map[error][]string{
		fmt.Errorf("insert: %w", pgErr):          {"insert: " + pgErr.Message, "SQLSTATE 23505", "detail: " + pgErr.Detail, "hint: " + pgErr.Hint},
		fmt.Errorf("node zone1/a: %w", adminErr): {"node zone1/a: admin API error unauthenticated: token expired"},
	}
	for err, wants := range cases {
		got := describe(err)
		for _, want := range wants {
			if !strings.Contains(got, want) {
				t.Errorf("describe(%v) = %q, missing %q", err, got, want)
			}
		}
	}
}

func TestPasswordEqualToALoginNameIsNotMasked(t *testing.T) {
	text := "connected as postgres, database postgres, postgres-mcp threshold"
	for _, dsn := range []string{"postgres://postgres:postgres@localhost:15432/postgres", "postgres://app:appdb@localhost:15432/appdb"} {
		if !PasswordIsLoginName(dsn) {
			t.Errorf("%s: the password is a login name but was not reported", dsn)
		}
		if got := newRedactor(dsn, "", "").apply(text); got != text {
			t.Errorf("%s: masked %q", dsn, got)
		}
	}
	dsn := "postgres://app:secretpw@localhost:15432/appdb"
	if PasswordIsLoginName(dsn) {
		t.Errorf("%s: reported a password that differs from the login names", dsn)
	}
	if got := newRedactor(dsn, "", "").apply("app secretpw appdb"); got != "app *** appdb" {
		t.Errorf("%s: %q", dsn, got)
	}
}

func TestPasswordIsMaskedInsideJSON(t *testing.T) {
	password := "p\"w\\d&<x>\x01z"
	t.Setenv("PGPASSWORD", password)
	out := &output{redact: newRedactor("postgres://app@localhost:15432/appdb", "", "")}
	out.JSON("rows", []map[string]any{{"query": "ALTER ROLE app PASSWORD '" + password + "'"}})
	encoded, err := protojson.Marshal(wrapperspb.String(password))
	if err != nil {
		t.Fatal(err)
	}
	out.JSON("status", map[string]any{"value": jsonRaw(encoded)})
	text := out.finish()
	literals := regexp.MustCompile(`"(?:[^"\\]|\\.)*"`).FindAllString(text, -1)
	if len(literals) < 4 {
		t.Fatalf("expected the JSON strings of both values in %q", text)
	}
	for _, literal := range literals {
		var decoded string
		if err := json.Unmarshal([]byte(literal), &decoded); err != nil {
			t.Fatalf("%s: %v", literal, err)
		}
		if strings.Contains(decoded, password) {
			t.Errorf("the password is in a JSON string of the result: %q", literal)
		}
	}
}

func TestPasswordFromTheEnvironmentIsMasked(t *testing.T) {
	t.Setenv("PGPASSWORD", "env-secret-789")
	if got := newRedactor("postgres://app@localhost:15432/appdb", "", "").apply("leak env-secret-789"); got != "leak ***" {
		t.Fatalf("the PGPASSWORD value was not masked: %q", got)
	}
}

func TestResultsStayUnderTheSizeLimit(t *testing.T) {
	text, _ := callText(t, func(out *output, text string) error {
		for i := 0; i < 2000; i++ {
			if !out.Line("%d %s", i, text) {
				break
			}
		}
		return nil
	}, strings.Repeat("가😀", 20))
	if size := utf16Len(text); size > resultLimit {
		t.Fatalf("result is %d UTF-16 units, over %d", size, resultLimit)
	}
	if !strings.Contains(text, "[result truncated") {
		t.Fatal("a cut result does not say it was cut")
	}
	if !utf8.ValidString(text) {
		t.Fatal("the cut produced invalid UTF-8")
	}
}

func TestNotesAndErrorsSurviveATruncatedResult(t *testing.T) {
	text, isError := callText(t, func(out *output, text string) error {
		for out.Line("%s", text) {
		}
		(&Deps{}).notes(out, db.Report{AdvisoryReleased: 2})
		return errors.New("late failure")
	}, strings.Repeat("x", 1000))
	if size := utf16Len(text); size > resultLimit || !isError {
		t.Fatalf("result is %d UTF-16 units, error %t", size, isError)
	}
	marker := strings.Index(text, "[result truncated")
	if marker < 0 || !strings.Contains(text[marker:], "note: released 2 session advisory locks") || !strings.Contains(text[marker:], "error: late failure") {
		t.Fatalf("the note or the error is missing after the truncation mark:\n%s", text[max(0, marker-200):])
	}
}

func TestASecretAcrossTheCutIsMaskedBeforeTheCut(t *testing.T) {
	text, _ := callText(t, func(out *output, text string) error {
		room := resultLimit - tailLimit - utf16Len(truncationMark)
		out.add(strings.Repeat("x", room-len(testPassword)/2))
		out.add(text)
		return nil
	}, testPassword+" after")
	if !strings.Contains(text, "[result truncated") {
		t.Fatal("the result was not cut")
	}
	if half := testPassword[:len(testPassword)/2]; strings.Contains(text, half) {
		t.Fatalf("the first half of the password, %q, is in the cut result", half)
	}
}

func TestALongNoteDoesNotPushOutTheError(t *testing.T) {
	text, isError := callText(t, func(out *output, text string) error {
		out.Tail("note: %s", text)
		return errors.New("late failure")
	}, strings.Repeat("y", 5000))
	if !isError || utf16Len(text) > tailLimit {
		t.Fatalf("error %t, %d UTF-16 units of notes and errors", isError, utf16Len(text))
	}
	mustContain(t, text, "error: late failure", "note: yyy", "[line cut")
}

func mustContain(t *testing.T, text string, parts ...string) {
	t.Helper()
	for _, part := range parts {
		if !strings.Contains(text, part) {
			t.Fatalf("result does not contain %q:\n%s", part, text)
		}
	}
}

func TestRedactionThatGrowsTheTextIsStillCut(t *testing.T) {
	out := output{redact: newRedactor("postgres://postgres:x@localhost:15432/postgres", "", "")}
	out.add(strings.Repeat("x😀", 15000))
	text := out.finish()
	if size := utf16Len(text); size > resultLimit {
		t.Fatalf("a one-letter password grew the result to %d UTF-16 units", size)
	}
	if strings.Contains(text, "x") || !utf8.ValidString(text) {
		t.Fatal("the grown result leaked the password or is invalid UTF-8")
	}
}
