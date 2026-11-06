package tools

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	clustermetadatapb "github.com/multigres/multigres/go/pb/clustermetadata"
	multiadminpb "github.com/multigres/multigres/go/pb/multiadmin"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/JoongHyuk-Shin/multigres-mcp/internal/admin"
	"github.com/JoongHyuk-Shin/multigres-mcp/internal/db"
)

const (
	ruleHistoryQuery db.Query = `SELECT coordinator_term, leader_subterm, event_type, leader_id, reason, created_at
FROM multigres.rule_history
ORDER BY coordinator_term DESC, leader_subterm DESC
LIMIT $1`
	superuserQuery db.Query = `SELECT current_user, rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user`
	logFileQuery   db.Query = `SELECT coalesce(pg_catalog.pg_current_logfile(), 'postgresql.log')`
	logSizeQuery   db.Query = `SELECT size FROM pg_catalog.pg_stat_file($1)`
	logReadQuery   db.Query = `SELECT pg_catalog.pg_read_binary_file($1, $2, $3)`

	defaultLogBytes  = 32 * 1024
	maxLogBytes      = 48 * 1024
	defaultHistory   = 5
	defaultWait      = 30
	requeryTimeout   = 10 * time.Second
	answeredTimeout  = 2 * time.Second
	verifyInProgress = "running"
	verifyTimedOut   = "timed out"
	verifyCancelled  = "was cancelled"
)

type shardInput struct {
	Database   string `json:"database,omitempty" jsonschema:"Database of the shard; optional when the cluster has one shard"`
	TableGroup string `json:"table_group,omitempty" jsonschema:"Table group of the shard; optional when the cluster has one shard"`
	Shard      string `json:"shard,omitempty" jsonschema:"Shard name (for example 0-inf); optional when the cluster has one shard"`
}

type clusterStatusInput struct {
	HistoryLimit int `json:"history_limit,omitempty" jsonschema:"How many recent primary changes to show (default 5)"`
}

type listBackupsInput struct {
	shardInput
	Limit int `json:"limit,omitempty" jsonschema:"Maximum number of backups (default all)"`
}

type jobInput struct {
	shardInput
	JobID string `json:"job_id" jsonschema:"Backup job id returned by create_backup"`
}

type nodeLogInput struct {
	Node     string `json:"node,omitempty" jsonschema:"Node as cell/name, name, or primary (default primary)"`
	MaxBytes int    `json:"max_bytes,omitempty" jsonschema:"Bytes to read from the end of the log (default 32768, at most 49152)"`
	Pattern  string `json:"pattern,omitempty" jsonschema:"Optional regular expression; only matching lines are shown"`
}

type switchInput struct {
	shardInput
	Reason      string `json:"reason" jsonschema:"Why the primary is switched; the admin server records it"`
	WaitSeconds int    `json:"wait_seconds,omitempty" jsonschema:"How long to wait for a new primary (default 30)"`
}

type backupInput struct {
	shardInput
	Type         string `json:"type" jsonschema:"full, differential or incremental"`
	ForcePrimary bool   `json:"force_primary,omitempty" jsonschema:"Take the backup on the primary"`
}

type restartsInput struct {
	Node    string `json:"node" jsonschema:"Node as cell/name or name"`
	Enabled bool   `json:"enabled" jsonschema:"true to let Multigres restart Postgres on this node automatically, false to stop it"`
}

func registerCluster(server *mcp.Server, d *Deps) {
	add(server, d, "cluster_status", "Cluster Status", "Show the Multigres cluster: cells, databases, each shard's primary and replicas with LSN and lag, gateways, orchestrators, recent primary changes of the default table group shard, and whether the current user is a superuser.", reading, d.clusterStatus)
	add(server, d, "list_backups", "List Backups", "List backups of a shard.", reading, d.listBackups)
	add(server, d, "verify_backups", "Verify Backups", "Verify the backups of a shard with pgBackRest and show its output.", reading, d.verifyBackups)
	add(server, d, "get_backup_job_status", "Get Backup Job Status", "Show the status of a backup job.", reading, d.backupJobStatus)
	add(server, d, "read_node_log", "Read Node Log", "Read the end of a node's Postgres log through a direct connection to that node.", reading, d.readNodeLog)
}

func registerClusterWrites(server *mcp.Server, d *Deps) {
	add(server, d, "switch_primary", "Switch Primary", "Ask Multigres to switch the primary of a shard, then report the old and new primary and terms. It does not retry by itself.", changing, d.switchPrimary)
	add(server, d, "create_backup", "Create Backup", "Start a backup of a shard and return its job id. Success means the job was accepted, not finished.", adding, d.createBackup)
	add(server, d, "expire_backups", "Expire Backups", "Expire backups of a shard with the retention rules configured in the cluster.", changing, d.expireBackups)
	add(server, d, "set_postgres_restarts", "Set Postgres Restarts", "Turn automatic Postgres restarts on a node on or off.", switchable, d.setPostgresRestarts)
}

func adminCall(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, admin.RequestTimeout)
}

func (d *Deps) poolers(ctx context.Context) ([]*clustermetadatapb.Multipooler, error) {
	if d.Admin == nil {
		return nil, admin.ErrNotConfigured
	}
	callCtx, cancel := adminCall(ctx)
	defer cancel()
	return d.Admin.Poolers(callCtx)
}

func (d *Deps) shard(ctx context.Context, in shardInput) (admin.Shard, error) {
	poolers, err := d.poolers(ctx)
	if err != nil {
		return admin.Shard{}, err
	}
	return admin.ResolveShard(poolers, admin.Shard{Database: in.Database, TableGroup: in.TableGroup, Shard: in.Shard})
}

func (d *Deps) clusterStatus(ctx context.Context, in clusterStatusInput, out *output) error {
	var cells, databases, shards, gateways, orchestrators any
	var wg sync.WaitGroup
	wg.Go(func() {
		names, err := adminValue(ctx, d.Admin.CellNames)
		cells = names
		if err != nil {
			cells = "error: " + describe(err)
		}
	})
	wg.Go(func() { databases = d.databases(ctx) })
	wg.Go(func() { shards = d.shardStatus(ctx) })
	wg.Go(func() {
		list, err := adminValue(ctx, d.Admin.Gateways)
		gateways = componentList(list, err)
	})
	wg.Go(func() {
		list, err := adminValue(ctx, d.Admin.Orchs)
		orchestrators = componentList(list, err)
	})
	wg.Wait()
	out.JSON("", map[string]any{"cells": cells, "databases": databases, "shards": shards, "gateways": gateways, "orchestrators": orchestrators})
	limit := in.HistoryLimit
	if limit <= 0 {
		limit = defaultHistory
	}
	report, err := d.DB.Read(ctx, d.readPlan(), func(ctx context.Context, tx *db.Tx) error {
		out.Line("recent primary changes of the default table group shard (multigres.rule_history):")
		if err := readRows(ctx, out, func(ctx context.Context) (pgx.Rows, error) {
			return tx.Query(ctx, ruleHistoryQuery, limit)
		}); err != nil || out.truncated {
			return err
		}
		var user string
		var superuser bool
		if err := tx.QueryRow(ctx, superuserQuery).Scan(&user, &superuser); err != nil {
			return err
		}
		out.Line("connected as %s, superuser: %t", user, superuser)
		return nil
	})
	d.notes(out, report)
	if err != nil {
		out.Line("primary history and user through the gateway: %s", describe(err))
	}
	return nil
}

func adminValue[T any](ctx context.Context, call func(context.Context) (T, error)) (T, error) {
	callCtx, cancel := adminCall(ctx)
	defer cancel()
	return call(callCtx)
}

func (d *Deps) databases(ctx context.Context) any {
	names, err := adminValue(ctx, d.Admin.DatabaseNames)
	if err != nil {
		return "error: " + describe(err)
	}
	values := make([]any, len(names))
	var wg sync.WaitGroup
	for i, name := range names {
		wg.Go(func() {
			database, err := adminValue(ctx, func(ctx context.Context) (*clustermetadatapb.Database, error) { return d.Admin.Database(ctx, name) })
			if err != nil {
				values[i] = "error: " + describe(err)
				return
			}
			values[i] = protoValue(database)
		})
	}
	wg.Wait()
	databases := map[string]any{}
	for i, name := range names {
		databases[name] = values[i]
	}
	return databases
}

func protoValue(message proto.Message) any {
	encoded, err := protojson.Marshal(message)
	if err != nil {
		return "could not encode: " + err.Error()
	}
	return jsonRaw(encoded)
}

type componentID interface {
	GetId() *clustermetadatapb.ID
	GetHostname() string
	GetPortMap() map[string]int32
}

func componentList[T componentID](components []T, err error) any {
	if err != nil {
		return "error: " + describe(err)
	}
	list := make([]map[string]any, 0, len(components))
	for _, component := range components {
		list = append(list, map[string]any{
			"name":     component.GetId().GetCell() + "/" + component.GetId().GetName(),
			"hostname": component.GetHostname(),
			"ports":    component.GetPortMap(),
		})
	}
	return list
}

func (d *Deps) shardStatus(ctx context.Context) any {
	poolers, err := adminValue(ctx, d.Admin.Poolers)
	if err != nil {
		return "error: " + describe(err)
	}
	return d.shards(ctx, poolers)
}

type nodeState struct {
	shard      string
	entry      map[string]any
	lsn        uint64
	primaryLSN uint64
}

func (d *Deps) shards(ctx context.Context, poolers []*clustermetadatapb.Multipooler) map[string][]map[string]any {
	states := make([]nodeState, len(poolers))
	var wg sync.WaitGroup
	for i, pooler := range poolers {
		wg.Go(func() { states[i] = d.nodeState(ctx, pooler) })
	}
	wg.Wait()
	primaryLSN := map[string]uint64{}
	for _, state := range states {
		if state.primaryLSN > 0 {
			primaryLSN[state.shard] = state.primaryLSN
		}
	}
	entries := map[string][]map[string]any{}
	for _, state := range states {
		if primary, ok := primaryLSN[state.shard]; ok && state.lsn > 0 {
			if primary >= state.lsn {
				state.entry["replay_lag_bytes"] = primary - state.lsn
			} else {
				state.entry["replay_lag_bytes"] = 0
			}
		}
		entries[state.shard] = append(entries[state.shard], state.entry)
	}
	return entries
}

func (d *Deps) nodeState(ctx context.Context, pooler *clustermetadatapb.Multipooler) nodeState {
	ref := pooler.GetShardRef()
	state := nodeState{shard: ref.GetDatabase() + "/" + ref.GetTableGroup() + "/" + ref.GetShard(), entry: map[string]any{
		"node":     admin.NodeName(pooler),
		"role":     strings.TrimPrefix(pooler.GetRoutingState().GetRole().String(), "ROUTING_ROLE_"),
		"hostname": pooler.GetHostname(),
		"ports":    pooler.GetPortMap(),
	}}
	status, err := adminValue(ctx, func(ctx context.Context) (*multiadminpb.GetPoolerStatusResponse, error) {
		return d.Admin.PoolerStatus(ctx, pooler.GetId())
	})
	if err != nil {
		state.entry["status"] = "no response: " + describe(err)
		return state
	}
	s := status.GetStatus()
	state.entry["postgres_running"] = s.GetPostgresRunning()
	state.entry["postgres_ready"] = s.GetPostgresReady()
	state.entry["wal_position"] = s.GetWalPosition()
	if primary := s.GetPrimaryStatus(); primary != nil && primary.GetLsn() != "" {
		state.entry["lsn"] = primary.GetLsn()
		if lsn, ok := parseLSN(primary.GetLsn()); ok {
			state.primaryLSN = lsn
		}
	}
	if replica := s.GetReplicationStatus(); replica != nil && replica.GetLastReplayLsn() != "" {
		state.entry["last_replay_lsn"] = replica.GetLastReplayLsn()
		state.entry["last_receive_lsn"] = replica.GetLastReceiveLsn()
		state.entry["wal_replay_paused"] = replica.GetIsWalReplayPaused()
		if lsn, ok := parseLSN(replica.GetLastReplayLsn()); ok {
			state.lsn = lsn
		}
	}
	return state
}

func parseLSN(text string) (uint64, bool) {
	high, low, ok := strings.Cut(text, "/")
	if !ok {
		return 0, false
	}
	h, err := strconv.ParseUint(high, 16, 32)
	if err != nil {
		return 0, false
	}
	l, err := strconv.ParseUint(low, 16, 32)
	if err != nil {
		return 0, false
	}
	return h<<32 | l, true
}

func (d *Deps) listBackups(ctx context.Context, in listBackupsInput, out *output) error {
	shard, err := d.shard(ctx, in.shardInput)
	if err != nil {
		return err
	}
	callCtx, cancel := adminCall(ctx)
	defer cancel()
	backups, err := d.Admin.Backups(callCtx, shard, uint32(max(in.Limit, 0)))
	if err != nil {
		return err
	}
	out.Line("shard: %s, backups: %d", shard, len(backups))
	for _, backup := range backups {
		if !out.JSON("", protoValue(backup)) {
			break
		}
	}
	return nil
}

func (d *Deps) verifyBackups(ctx context.Context, in shardInput, out *output) error {
	shard, err := d.shard(ctx, in)
	if err != nil {
		return err
	}
	key := shard.String()
	if previous, busy := d.verifying.LoadOrStore(key, verifyInProgress); busy {
		if previous != verifyInProgress {
			return fmt.Errorf("an earlier verification of %s %s; the admin API asks to check the pooler's running state before starting another one, so this server will not start one again until it restarts", key, previous)
		}
		return fmt.Errorf("a verification of %s is already running in this server; the admin API asks not to run verifications concurrently", key)
	}
	out.Line("note: this server only knows about verifications it started; another process or person may be verifying the same shard")
	callCtx, cancel := context.WithTimeout(ctx, admin.VerifyTimeout)
	defer cancel()
	verified, err := d.Admin.VerifyBackups(callCtx, shard)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || callCtx.Err() != nil {
			ending := verifyCancelled
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(callCtx.Err(), context.DeadlineExceeded) {
				ending = verifyTimedOut
			}
			d.verifying.Store(key, ending)
			out.Line("the verification %s before it answered; check the pooler's running state before starting another verification (admin API definition)", ending)
			return err
		}
		d.verifying.Delete(key)
		return err
	}
	d.verifying.Delete(key)
	out.Line("shard: %s, duration: %s", shard, verified.GetDuration().AsDuration())
	out.add(verified.GetRawOutput() + "\n")
	return nil
}

func (d *Deps) backupJobStatus(ctx context.Context, in jobInput, out *output) error {
	if in.JobID == "" {
		return errors.New("job_id is required")
	}
	shard, err := d.shard(ctx, in.shardInput)
	if err != nil {
		return err
	}
	callCtx, cancel := adminCall(ctx)
	defer cancel()
	status, err := d.Admin.BackupJobStatus(callCtx, in.JobID, shard)
	if err != nil {
		return err
	}
	out.JSON("", protoValue(status))
	return nil
}

func (d *Deps) readNodeLog(ctx context.Context, in nodeLogInput, out *output) error {
	limit := in.MaxBytes
	if limit <= 0 {
		limit = defaultLogBytes
	}
	if limit > maxLogBytes {
		return fmt.Errorf("max_bytes is at most %d", maxLogBytes)
	}
	var filter *regexp.Regexp
	if in.Pattern != "" {
		var err error
		if filter, err = regexp.Compile(in.Pattern); err != nil {
			return fmt.Errorf("pattern: %w", err)
		}
	}
	text, node, err := d.nodeLog(ctx, in.Node, limit)
	if err != nil {
		return err
	}
	out.Line("node: %s", node)
	if filter == nil {
		out.add(text)
		return nil
	}
	matched := 0
	for _, line := range strings.Split(text, "\n") {
		if filter.MatchString(line) {
			matched++
			if !out.add(line + "\n") {
				break
			}
		}
	}
	out.Line("matching lines: %d", matched)
	return nil
}

func (d *Deps) readNode(ctx context.Context, node string, plan db.Plan, fn func(context.Context, *db.Tx, string) error) (string, error) {
	poolers, err := d.poolers(ctx)
	if err != nil {
		return "", err
	}
	pooler, err := admin.ResolveNode(poolers, node)
	if err != nil {
		return "", err
	}
	port, err := admin.PostgresPort(pooler)
	if err != nil {
		return "", err
	}
	name := admin.NodeName(pooler)
	host := d.Config.MapNodeHost(pooler.GetHostname())
	err = db.ReadNode(ctx, d.Config.DatabaseURL, host, port, plan, func(ctx context.Context, tx *db.Tx) error {
		return fn(ctx, tx, name)
	})
	if err != nil {
		return name, fmt.Errorf("node %s (direct connection): %w", name, err)
	}
	return name, nil
}

func (d *Deps) nodeLog(ctx context.Context, node string, limit int) (string, string, error) {
	var tail []byte
	name, err := d.readNode(ctx, node, d.readPlan(), func(ctx context.Context, tx *db.Tx, _ string) error {
		var file string
		if err := tx.QueryRow(ctx, logFileQuery).Scan(&file); err != nil {
			return err
		}
		var size int64
		if err := tx.QueryRow(ctx, logSizeQuery, file).Scan(&size); err != nil {
			return err
		}
		offset := max(size-int64(limit), 0)
		if err := tx.QueryRow(ctx, logReadQuery, file, offset, int64(limit)).Scan(&tail); err != nil {
			return err
		}
		if offset > 0 {
			if _, rest, ok := bytes.Cut(tail, []byte("\n")); ok {
				tail = rest
			}
		}
		return nil
	})
	return string(tail), name, err
}

type primaryState struct {
	primary string
	term    int64
}

func (d *Deps) primaryState(ctx context.Context, shard admin.Shard) (primaryState, error) {
	poolers, err := adminValue(ctx, d.Admin.Poolers)
	if err != nil {
		return primaryState{}, err
	}
	var state primaryState
	var members []*clustermetadatapb.Multipooler
	for _, pooler := range poolers {
		ref := pooler.GetShardRef()
		if ref.GetDatabase() != shard.Database || ref.GetTableGroup() != shard.TableGroup || ref.GetShard() != shard.Shard {
			continue
		}
		members = append(members, pooler)
		if admin.IsPrimary(pooler) {
			state.primary = admin.NodeName(pooler)
		}
	}
	if len(members) == 0 {
		return state, fmt.Errorf("the pooler list has no pooler of %s", shard)
	}
	terms := make([]int64, len(members))
	errs := make([]error, len(members))
	var wg sync.WaitGroup
	for i, pooler := range members {
		wg.Go(func() {
			status, err := adminValue(ctx, func(ctx context.Context) (*multiadminpb.GetPoolerStatusResponse, error) {
				return d.Admin.PoolerStatus(ctx, pooler.GetId())
			})
			if err != nil {
				errs[i] = fmt.Errorf("%s: %w", admin.NodeName(pooler), err)
				return
			}
			terms[i] = status.GetConsensusStatus().GetCurrentPosition().GetPosition().GetDecision().GetRuleNumber().GetCoordinatorTerm()
		})
	}
	wg.Wait()
	answered := false
	for i := range members {
		if errs[i] == nil {
			answered = true
			state.term = max(state.term, terms[i])
		}
	}
	if !answered {
		return state, fmt.Errorf("no pooler of %s answered its status: %w", shard, errors.Join(errs...))
	}
	return state, nil
}

func (d *Deps) switchPrimary(ctx context.Context, in switchInput, out *output) error {
	if strings.TrimSpace(in.Reason) == "" {
		return errors.New("reason is required; the admin server records it")
	}
	shard, err := d.shard(ctx, in.shardInput)
	if err != nil {
		return err
	}
	before, err := d.primaryState(ctx, shard)
	if err != nil {
		return fmt.Errorf("could not read the current primary before switching: %w", err)
	}
	out.Line("before: primary %s, term %d", before.primary, before.term)
	wait := time.Duration(in.WaitSeconds) * time.Second
	if in.WaitSeconds <= 0 {
		wait = defaultWait * time.Second
	}
	callCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	_, switchErr := d.Writer.SwitchPrimary(callCtx, shard, in.Reason)
	if switchErr != nil {
		out.Line("switch request: %s", describe(switchErr))
	}
	after, err := d.waitForPrimary(callCtx, shard, before, switchErr == nil)
	if err != nil {
		requeryCtx, cancelRequery := context.WithTimeout(context.WithoutCancel(ctx), requeryTimeout)
		defer cancelRequery()
		after, err = d.waitForPrimary(requeryCtx, shard, before, true)
		if err != nil {
			out.Line("result unknown: the switch request and the re-read both failed (%s)", describe(err))
			return errors.Join(switchErr, err)
		}
	}
	out.Line("after: primary %s, term %d", after.primary, after.term)
	for _, line := range switchVerdict(before, after, wait) {
		out.Line("%s", line)
	}
	out.Line("this tool does not retry; the admin API asks callers to check the cluster before calling again")
	return switchErr
}

func switchVerdict(before, after primaryState, wait time.Duration) []string {
	switch {
	case after.primary == "":
		return []string{fmt.Sprintf("the pooler list shows no primary after %s; whether the switch finished is unknown", wait)}
	case after.primary != before.primary && after.term <= before.term:
		return []string{
			fmt.Sprintf("the primary changed from %s to %s", before.primary, after.primary),
			fmt.Sprintf("term %d has not risen yet in the poolers' status", before.term),
		}
	case after.primary != before.primary:
		return []string{fmt.Sprintf("the primary changed from %s to %s", before.primary, after.primary)}
	case after.term > before.term:
		return []string{"a new term started but the same node was elected again; the primary did not change"}
	default:
		return []string{fmt.Sprintf("no new term yet; the primary did not change within %s", wait)}
	}
}

func (d *Deps) waitForPrimary(ctx context.Context, shard admin.Shard, before primaryState, accepted bool) (primaryState, error) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var last primaryState
	var readErr error
	for {
		state, err := d.primaryState(ctx, shard)
		if err == nil {
			last = state
			if state.term > before.term && state.primary != "" {
				return state, nil
			}
			if !accepted {
				return state, nil
			}
		} else if ctx.Err() == nil {
			readErr = err
		}
		select {
		case <-ctx.Done():
			if last.term == 0 && last.primary == "" {
				return last, errors.Join(readErr, ctx.Err())
			}
			return last, nil
		case <-ticker.C:
		}
	}
}

func (d *Deps) createBackup(ctx context.Context, in backupInput, out *output) error {
	switch in.Type {
	case "full", "differential", "incremental":
	default:
		return fmt.Errorf("type must be full, differential or incremental, not %q", in.Type)
	}
	shard, err := d.shard(ctx, in.shardInput)
	if err != nil {
		return err
	}
	callCtx, cancel := adminCall(ctx)
	defer cancel()
	jobID, err := d.Writer.Backup(callCtx, shard, in.Type, in.ForcePrimary)
	if err != nil {
		return err
	}
	out.Line("shard: %s, job_id: %s", shard, jobID)
	out.Line("the backup was accepted, not finished; follow it with get_backup_job_status")
	return nil
}

func (d *Deps) expireBackups(ctx context.Context, in shardInput, out *output) error {
	shard, err := d.shard(ctx, in)
	if err != nil {
		return err
	}
	callCtx, cancel := adminCall(ctx)
	defer cancel()
	expired, err := d.Writer.ExpireBackups(callCtx, shard)
	if err == nil {
		out.Line("shard: %s, expired: %s", shard, strings.Join(expired, ", "))
		return nil
	}
	out.Line("expire failed: %s", describe(err))
	out.Line("backups may already have been removed before the error; the admin API asks to read the list again before calling again. Current list:")
	requeryCtx, cancelRequery := context.WithTimeout(context.WithoutCancel(ctx), requeryTimeout)
	defer cancelRequery()
	backups, listErr := d.Admin.Backups(requeryCtx, shard, 0)
	if listErr != nil {
		out.Line("could not read the list: %s", describe(listErr))
		return errors.Join(err, listErr)
	}
	for _, backup := range backups {
		if !out.JSON("", protoValue(backup)) {
			break
		}
	}
	return err
}

func (d *Deps) setPostgresRestarts(ctx context.Context, in restartsInput, out *output) error {
	if in.Node == "" || in.Node == "primary" {
		return errors.New("node is required as cell/name or name")
	}
	poolers, err := d.poolers(ctx)
	if err != nil {
		return err
	}
	pooler, err := admin.ResolveNode(poolers, in.Node)
	if err != nil {
		return err
	}
	callCtx, cancel := adminCall(ctx)
	defer cancel()
	if err := d.Writer.SetPostgresRestarts(callCtx, pooler.GetId(), in.Enabled); err != nil {
		out.Line("result unknown: an error does not show that nothing changed (admin API definition)")
		return err
	}
	out.Line("accepted: automatic Postgres restarts on %s set to %t; the current value cannot be read back", admin.NodeName(pooler), in.Enabled)
	return nil
}
