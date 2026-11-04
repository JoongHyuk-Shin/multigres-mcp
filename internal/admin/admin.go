package admin

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"connectrpc.com/connect"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"github.com/multigres/multigres/go/pb/multiadmin/multiadminconnect"
)

const (
	RequestTimeout = 30 * time.Second
	VerifyTimeout  = 10 * time.Minute
)

var ErrNotConfigured = errors.New("the Multigres admin server is not configured (set --admin-url or MULTIGRES_ADMIN_URL)")

type Reader struct {
	client multiadminconnect.MultiadminServiceClient
}

type Writer struct {
	client multiadminconnect.MultiadminServiceClient
}

func NewReader(url, token string) *Reader {
	if url == "" {
		return nil
	}
	return &Reader{client: newClient(url, token)}
}

func NewWriter(url, token string) *Writer {
	if url == "" {
		return nil
	}
	return &Writer{client: newClient(url, token)}
}

func newClient(url, token string) multiadminconnect.MultiadminServiceClient {
	httpClient := &http.Client{}
	var opts []connect.ClientOption
	if token != "" {
		opts = append(opts, connect.WithInterceptors(bearer(token)))
	}
	return multiadminconnect.NewMultiadminServiceClient(httpClient, url, opts...)
}

func bearer(token string) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			req.Header().Set("Authorization", "Bearer "+token)
			return next(ctx, req)
		}
	}
}

type Shard struct {
	Database   string
	TableGroup string
	Shard      string
}

func (s Shard) String() string { return s.Database + "/" + s.TableGroup + "/" + s.Shard }

func (s Shard) ref() *clustermetadatapb.ShardRef {
	return &clustermetadatapb.ShardRef{Database: s.Database, TableGroup: s.TableGroup, Shard: s.Shard}
}

func (r *Reader) CellNames(ctx context.Context) ([]string, error) {
	resp, err := r.client.GetCellNames(ctx, connect.NewRequest(&multiadminpb.GetCellNamesRequest{}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetNames(), nil
}

func (r *Reader) Cell(ctx context.Context, name string) (*clustermetadatapb.Cell, error) {
	resp, err := r.client.GetCell(ctx, connect.NewRequest(&multiadminpb.GetCellRequest{Name: name}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetCell(), nil
}

func (r *Reader) DatabaseNames(ctx context.Context) ([]string, error) {
	resp, err := r.client.GetDatabaseNames(ctx, connect.NewRequest(&multiadminpb.GetDatabaseNamesRequest{}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetNames(), nil
}

func (r *Reader) Database(ctx context.Context, name string) (*clustermetadatapb.Database, error) {
	resp, err := r.client.GetDatabase(ctx, connect.NewRequest(&multiadminpb.GetDatabaseRequest{Name: name}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetDatabase(), nil
}

func (r *Reader) Gateways(ctx context.Context) ([]*clustermetadatapb.Multigateway, error) {
	resp, err := r.client.GetGateways(ctx, connect.NewRequest(&multiadminpb.GetGatewaysRequest{}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetGateways(), nil
}

func (r *Reader) Orchs(ctx context.Context) ([]*clustermetadatapb.Multiorch, error) {
	resp, err := r.client.GetOrchs(ctx, connect.NewRequest(&multiadminpb.GetOrchsRequest{}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetOrchs(), nil
}

func (r *Reader) Poolers(ctx context.Context) ([]*clustermetadatapb.Multipooler, error) {
	resp, err := r.client.GetPoolers(ctx, connect.NewRequest(&multiadminpb.GetPoolersRequest{}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetPoolers(), nil
}

func (r *Reader) PoolerStatus(ctx context.Context, id *clustermetadatapb.ID) (*multiadminpb.GetPoolerStatusResponse, error) {
	resp, err := r.client.GetPoolerStatus(ctx, connect.NewRequest(&multiadminpb.GetPoolerStatusRequest{PoolerId: id}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (r *Reader) Backups(ctx context.Context, shard Shard, limit uint32) ([]*multiadminpb.BackupInfo, error) {
	resp, err := r.client.GetBackups(ctx, connect.NewRequest(&multiadminpb.GetBackupsRequest{
		Database: shard.Database, TableGroup: shard.TableGroup, Shard: shard.Shard, Limit: limit,
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetBackups(), nil
}

func (r *Reader) VerifyBackups(ctx context.Context, shard Shard) (*multiadminpb.VerifyBackupsResponse, error) {
	resp, err := r.client.VerifyBackups(ctx, connect.NewRequest(&multiadminpb.VerifyBackupsRequest{
		Database: shard.Database, TableGroup: shard.TableGroup, Shard: shard.Shard,
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (r *Reader) BackupJobStatus(ctx context.Context, jobID string, shard Shard) (*multiadminpb.GetBackupJobStatusResponse, error) {
	resp, err := r.client.GetBackupJobStatus(ctx, connect.NewRequest(&multiadminpb.GetBackupJobStatusRequest{
		JobId: jobID, Database: shard.Database, TableGroup: shard.TableGroup, Shard: shard.Shard,
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (r *Reader) GatewayQueries(ctx context.Context, id *clustermetadatapb.ID) (*multiadminpb.GetGatewayQueriesResponse, error) {
	resp, err := r.client.GetGatewayQueries(ctx, connect.NewRequest(&multiadminpb.GetGatewayQueriesRequest{GatewayId: id, Limit: 0}))
	if err != nil {
		return nil, err
	}
	return resp.Msg, nil
}

func (w *Writer) SwitchPrimary(ctx context.Context, shard Shard, reason string) (*clustermetadatapb.ID, error) {
	resp, err := w.client.SwitchPrimary(ctx, connect.NewRequest(&multiadminpb.SwitchPrimaryRequest{ShardRef: shard.ref(), Reason: reason}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetOldLeaderId(), nil
}

func (w *Writer) Backup(ctx context.Context, shard Shard, backupType string, forcePrimary bool) (string, error) {
	resp, err := w.client.Backup(ctx, connect.NewRequest(&multiadminpb.BackupRequest{
		Database: shard.Database, TableGroup: shard.TableGroup, Shard: shard.Shard, Type: backupType, ForcePrimary: forcePrimary,
	}))
	if err != nil {
		return "", err
	}
	return resp.Msg.GetJobId(), nil
}

func (w *Writer) ExpireBackups(ctx context.Context, shard Shard) ([]string, error) {
	resp, err := w.client.ExpireBackups(ctx, connect.NewRequest(&multiadminpb.ExpireBackupsRequest{
		Database: shard.Database, TableGroup: shard.TableGroup, Shard: shard.Shard,
	}))
	if err != nil {
		return nil, err
	}
	return resp.Msg.GetExpiredBackupIds(), nil
}

func (w *Writer) SetPostgresRestarts(ctx context.Context, id *clustermetadatapb.ID, enabled bool) error {
	_, err := w.client.SetPostgresRestartsEnabled(ctx, connect.NewRequest(&multiadminpb.SetPostgresRestartsEnabledRequest{PoolerId: id, Enabled: enabled}))
	return err
}

func ResolveShard(poolers []*clustermetadatapb.Multipooler, want Shard) (Shard, error) {
	seen := map[Shard]bool{}
	var matches []Shard
	for _, pooler := range poolers {
		ref := pooler.GetShardRef()
		shard := Shard{Database: ref.GetDatabase(), TableGroup: ref.GetTableGroup(), Shard: ref.GetShard()}
		if seen[shard] {
			continue
		}
		seen[shard] = true
		if matchesShard(shard, want) {
			matches = append(matches, shard)
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	all := make([]string, 0, len(seen))
	for shard := range seen {
		all = append(all, shard.String())
	}
	sort.Strings(all)
	if len(matches) == 0 {
		return Shard{}, fmt.Errorf("no shard matches database=%q table_group=%q shard=%q; shards: %s", want.Database, want.TableGroup, want.Shard, strings.Join(all, ", "))
	}
	return Shard{}, fmt.Errorf("more than one shard matches; give database, table_group and shard (database/table_group/shard): %s", strings.Join(all, ", "))
}

func matchesShard(shard, want Shard) bool {
	return (want.Database == "" || want.Database == shard.Database) &&
		(want.TableGroup == "" || want.TableGroup == shard.TableGroup) &&
		(want.Shard == "" || want.Shard == shard.Shard)
}

func ResolveNode(poolers []*clustermetadatapb.Multipooler, node string) (*clustermetadatapb.Multipooler, error) {
	var matches []*clustermetadatapb.Multipooler
	for _, pooler := range poolers {
		if nodeMatches(pooler, node) {
			matches = append(matches, pooler)
		}
	}
	if len(matches) == 1 {
		return matches[0], nil
	}
	names := make([]string, 0, len(poolers))
	for _, pooler := range poolers {
		names = append(names, NodeName(pooler))
	}
	sort.Strings(names)
	if len(matches) == 0 {
		return nil, fmt.Errorf("no node matches %q; nodes: %s, or primary", node, strings.Join(names, ", "))
	}
	return nil, fmt.Errorf("%q matches more than one node; use cell/name: %s", node, strings.Join(names, ", "))
}

func nodeMatches(pooler *clustermetadatapb.Multipooler, node string) bool {
	switch {
	case node == "" || node == "primary":
		return IsPrimary(pooler)
	case strings.Contains(node, "/"):
		return node == NodeName(pooler)
	default:
		return node == pooler.GetId().GetName()
	}
}

func NodeName(pooler *clustermetadatapb.Multipooler) string {
	return pooler.GetId().GetCell() + "/" + pooler.GetId().GetName()
}

func IsPrimary(pooler *clustermetadatapb.Multipooler) bool {
	return pooler.GetRoutingState().GetRole() == clustermetadatapb.RoutingRole_ROUTING_ROLE_PRIMARY
}

func PostgresPort(pooler *clustermetadatapb.Multipooler) (int, error) {
	port, ok := pooler.GetPortMap()["postgres"]
	if !ok {
		return 0, fmt.Errorf("the admin server reports no postgres port for node %s", NodeName(pooler))
	}
	return int(port), nil
}
