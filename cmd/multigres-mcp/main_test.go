package main

import (
	"context"
	"debug/buildinfo"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestServerReportsTheVersionItWasBuiltAs(t *testing.T) {
	cases := []struct {
		name  string
		flags []string
		want  func(t *testing.T, binary string) string
	}{
		{"go build in a checkout", nil, stampedVersion},
		{"release build", []string{"-ldflags", "-X main.version=9.8.7"}, func(*testing.T, string) string { return "9.8.7" }},
		{"build without version control data", []string{"-buildvcs=false"}, func(*testing.T, string) string { return "dev" }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			binary := filepath.Join(t.TempDir(), "multigres-mcp")
			build := exec.Command("go", append(append([]string{"build"}, c.flags...), "-o", binary, ".")...)
			if out, err := build.CombinedOutput(); err != nil {
				t.Fatalf("build: %v\n%s", err, out)
			}
			want := c.want(t, binary)
			if got := reportedVersion(t, binary); got != want {
				t.Errorf("serverInfo version %q, want %q", got, want)
			}
		})
	}
}

func stampedVersion(t *testing.T, binary string) string {
	t.Helper()
	info, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	if info.Main.Version == "" || info.Main.Version == "(devel)" {
		t.Skipf("go stamped no module version in this checkout: %q", info.Main.Version)
	}
	return info.Main.Version
}

func reportedVersion(t *testing.T, binary string) string {
	t.Helper()
	cmd := exec.Command(binary)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "DATABASE_URI=postgres://nobody@127.0.0.1:1/nothing"}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	session, err := mcp.NewClient(&mcp.Implementation{Name: "multigres-mcp-version"}, nil).Connect(ctx, &mcp.CommandTransport{Command: cmd}, nil)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer session.Close()
	return session.InitializeResult().ServerInfo.Version
}
