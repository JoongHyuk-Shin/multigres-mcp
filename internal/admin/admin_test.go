package admin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"github.com/multigres/multigres/go/pb/multiadmin/multiadminconnect"
)

func pooler(cell, name, database, tableGroup, shard string, primary bool) *clustermetadatapb.Multipooler {
	role := clustermetadatapb.RoutingRole_ROUTING_ROLE_REPLICA
	if primary {
		role = clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY
	}
	return &clustermetadatapb.Multipooler{
		Id:           &clustermetadatapb.ID{Cell: cell, Name: name},
		ShardRef:     &clustermetadatapb.ShardRef{Database: database, TableGroup: tableGroup, Shard: shard},
		RoutingState: &clustermetadatapb.RoutingState{Role: role},
		PortMap:      map[string]int32{"postgres": 25432},
	}
}

func TestResolveShard(t *testing.T) {
	one := []*clustermetadatapb.Multipooler{
		pooler("zone1", "a", "postgres", "default", "0-inf", false),
		pooler("zone2", "b", "postgres", "default", "0-inf", true),
	}
	got, err := ResolveShard(one, Shard{})
	if err != nil || got != (Shard{"postgres", "default", "0-inf"}) {
		t.Fatalf("the only shard was not chosen: %v, %v", got, err)
	}
	two := append(one, pooler("zone1", "c", "postgres", "orders", "0-inf", true))
	if _, err := ResolveShard(two, Shard{}); err == nil || !strings.Contains(err.Error(), "postgres/orders/0-inf") {
		t.Fatalf("two shards without a choice should fail and list them: %v", err)
	}
	got, err = ResolveShard(two, Shard{TableGroup: "orders"})
	if err != nil || got.TableGroup != "orders" {
		t.Fatalf("table_group did not pick the shard: %v, %v", got, err)
	}
	if _, err := ResolveShard(two, Shard{Database: "nope"}); err == nil || !strings.Contains(err.Error(), "no shard matches") {
		t.Fatalf("an unknown database should fail: %v", err)
	}
}

func TestResolveNode(t *testing.T) {
	poolers := []*clustermetadatapb.Multipooler{
		pooler("zone1", "sp48bdwk", "postgres", "default", "0-inf", false),
		pooler("zone2", "trs9nmqh", "postgres", "default", "0-inf", true),
		pooler("zone3", "sp48bdwk", "postgres", "default", "0-inf", false),
	}
	cases := []struct {
		node, want, fails string
	}{
		{"", "zone2/trs9nmqh", ""},
		{"primary", "zone2/trs9nmqh", ""},
		{"trs9nmqh", "zone2/trs9nmqh", ""},
		{"zone3/sp48bdwk", "zone3/sp48bdwk", ""},
		{"sp48bdwk", "", "more than one node"},
		{"zone9/x", "", "no node matches"},
	}
	for _, tc := range cases {
		got, err := ResolveNode(poolers, tc.node)
		if tc.fails != "" {
			if err == nil || !strings.Contains(err.Error(), tc.fails) {
				t.Errorf("ResolveNode(%q) error = %v, want %q", tc.node, err, tc.fails)
			}
			continue
		}
		if err != nil || NodeName(got) != tc.want {
			t.Errorf("ResolveNode(%q) = %v, %v, want %s", tc.node, got, err, tc.want)
		}
	}
}

type gatewaysOnly struct {
	multiadminconnect.UnimplementedMultiadminServiceHandler
	authorization chan string
}

func (g gatewaysOnly) GetGateways(_ context.Context, req *connect.Request[multiadminpb.GetGatewaysRequest]) (*connect.Response[multiadminpb.GetGatewaysResponse], error) {
	g.authorization <- req.Header().Get("Authorization")
	return connect.NewResponse(&multiadminpb.GetGatewaysResponse{}), nil
}

func TestReaderSendsTheBearerTokenOnlyWhenSet(t *testing.T) {
	seen := make(chan string, 1)
	path, handler := multiadminconnect.NewMultiadminServiceHandler(gatewaysOnly{authorization: seen})
	mux := http.NewServeMux()
	mux.Handle(path, handler)
	server := httptest.NewServer(mux)
	defer server.Close()
	for _, tc := range []struct{ token, want string }{{"secret-token", "Bearer secret-token"}, {"", ""}} {
		if _, err := NewReader(server.URL, tc.token).Gateways(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := <-seen; got != tc.want {
			t.Errorf("token %q sent Authorization %q, want %q", tc.token, got, tc.want)
		}
	}
	if NewReader("", "x") != nil || NewWriter("", "x") != nil {
		t.Error("an empty admin URL must give no client")
	}
}
