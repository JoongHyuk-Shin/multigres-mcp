//go:build cluster

package tools

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"github.com/multigres/multigres/go/pb/multiadmin/multiadminconnect"
	"google.golang.org/protobuf/proto"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/testenv"
)

type proxyCall struct {
	procedure string
	at        time.Time
}

type adminProxy struct {
	t      *testing.T
	server *httptest.Server

	mu           sync.Mutex
	calls        []proxyCall
	rewrite      func([]*clustermetadatapb.Multipooler)
	hold         map[string]time.Duration
	holdFirst    map[string][]time.Duration
	swallow      map[string]bool
	refuseAfter  map[string]refusal
	requireToken string
}

type refusal struct {
	after string
	since int
}

func newAdminProxy(t *testing.T) *adminProxy {
	t.Helper()
	p := &adminProxy{t: t, hold: map[string]time.Duration{}, holdFirst: map[string][]time.Duration{}, swallow: map[string]bool{}, refuseAfter: map[string]refusal{}}
	p.server = httptest.NewServer(p)
	t.Cleanup(p.server.Close)
	return p
}

func (p *adminProxy) URL() string { return p.server.URL }

func (p *adminProxy) set(change func(p *adminProxy)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	change(p)
}

func (p *adminProxy) refuse(procedure, after string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.refuseAfter[procedure] = refusal{after: after, since: len(p.calls)}
}

func (p *adminProxy) recorded() []proxyCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]proxyCall(nil), p.calls...)
}

func (p *adminProxy) callsTo(procedure string) []proxyCall {
	var matched []proxyCall
	for _, call := range p.recorded() {
		if call.procedure == procedure {
			matched = append(matched, call)
		}
	}
	return matched
}

func (p *adminProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	procedure := r.URL.Path
	p.mu.Lock()
	p.calls = append(p.calls, proxyCall{procedure: procedure, at: time.Now()})
	refuse := false
	if rule, ok := p.refuseAfter[procedure]; ok {
		for _, call := range p.calls[rule.since:] {
			if call.procedure == rule.after {
				refuse = true
			}
		}
	}
	unauthenticated := p.requireToken != "" && r.Header.Get("Authorization") != "Bearer "+p.requireToken
	hold, rewrite, swallow := p.hold[procedure], p.rewrite, p.swallow[procedure]
	if queue := p.holdFirst[procedure]; len(queue) > 0 {
		hold = queue[0]
		p.holdFirst[procedure] = queue[1:]
	}
	p.mu.Unlock()
	if unauthenticated {
		connectError(w, http.StatusUnauthorized, "unauthenticated", "missing or wrong bearer token")
		return
	}
	if swallow {
		wait(r.Context(), hold)
		connectError(w, http.StatusServiceUnavailable, "unavailable", "swallowed by the test proxy")
		return
	}
	if refuse {
		connectError(w, http.StatusServiceUnavailable, "unavailable", "refused by the test proxy")
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		connectError(w, http.StatusBadGateway, "unavailable", err.Error())
		return
	}
	upstream, err := http.NewRequestWithContext(context.WithoutCancel(r.Context()), r.Method, testenv.AdminURL()+procedure, bytes.NewReader(body))
	if err != nil {
		connectError(w, http.StatusBadGateway, "unavailable", err.Error())
		return
	}
	for key, values := range r.Header {
		if key != "Accept-Encoding" {
			upstream.Header[key] = values
		}
	}
	resp, err := http.DefaultTransport.RoundTrip(upstream)
	if err != nil {
		connectError(w, http.StatusBadGateway, "unavailable", err.Error())
		return
	}
	defer resp.Body.Close()
	payload, err := io.ReadAll(resp.Body)
	if err != nil {
		connectError(w, http.StatusBadGateway, "unavailable", err.Error())
		return
	}
	if procedure == multiadminconnect.MultiadminServiceGetPoolersProcedure && rewrite != nil && resp.StatusCode == http.StatusOK {
		var message multiadminpb.GetPoolersResponse
		if err := proto.Unmarshal(payload, &message); err != nil {
			p.t.Errorf("proxy could not decode GetPoolers (%s): %v", resp.Header.Get("Content-Type"), err)
		} else {
			rewrite(message.GetPoolers())
			payload, _ = proto.Marshal(&message)
		}
	}
	wait(r.Context(), hold)
	for key, values := range resp.Header {
		if key != "Content-Length" && key != "Content-Encoding" {
			w.Header()[key] = values
		}
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(payload)
}

func wait(ctx context.Context, hold time.Duration) {
	select {
	case <-time.After(hold):
	case <-ctx.Done():
	}
}

func connectError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, `{"code":"`+code+`","message":"`+message+`"}`)
}
