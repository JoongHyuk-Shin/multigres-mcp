package db

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	ApplicationName = "multigres-mcp"
	cleanupTimeout  = 5 * time.Second
	openTimeout     = 10 * time.Second
	clientMargin    = time.Second
	readOnlyOption  = "-c default_transaction_read_only=on"
	readPoolSize    = 4
	writePoolSize   = 2
)

var ErrWritesDisabled = errors.New("writes are available only in unrestricted mode")

type Options struct {
	URL      string
	Writable bool
}

type Plan struct {
	Timeout     time.Duration
	CallTimeout time.Duration
	Hypopg      bool
}

type DB struct {
	read     *pgxpool.Pool
	write    *pgxpool.Pool
	canceler *canceler
}

func Open(opts Options) (*DB, error) {
	canceler, err := newCanceler(opts.URL)
	if err != nil {
		return nil, err
	}
	db := &DB{canceler: canceler}
	db.read, err = newPool(opts.URL, true, readPoolSize, canceler.build)
	if err != nil {
		db.Close()
		return nil, err
	}
	if opts.Writable {
		db.write, err = newPool(opts.URL, false, writePoolSize, canceler.build)
		if err != nil {
			db.Close()
			return nil, err
		}
	}
	return db, nil
}

func (d *DB) Close() {
	for _, pool := range []*pgxpool.Pool{d.read, d.write} {
		if pool != nil {
			pool.Close()
		}
	}
	if d.canceler != nil {
		d.canceler.close()
	}
}

func (d *DB) Read(ctx context.Context, plan Plan, fn func(context.Context, *Tx) error) (Report, error) {
	return d.run(ctx, d.read, beginReadOnly, rollback, plan, fn)
}

func (d *DB) Write(ctx context.Context, plan Plan, fn func(context.Context, *Tx) error) (Report, error) {
	if d.write == nil {
		return Report{}, ErrWritesDisabled
	}
	return d.run(ctx, d.write, beginWrite, commit, plan, fn)
}

func ReadNode(ctx context.Context, url string, host string, port int, plan Plan, fn func(context.Context, *Tx) error) error {
	cfg, err := connConfig(url, true, protocolCancel)
	if err != nil {
		return err
	}
	retarget(cfg, host, uint16(port))
	openCtx, cancelOpen := context.WithTimeout(ctx, openTimeout)
	defer cancelOpen()
	conn, err := pgx.ConnectConfig(openCtx, cfg)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()
	tx, err := open(openCtx, conn, beginReadOnly, plan)
	if err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, plan.callTimeout())
	defer cancel()
	return fn(callCtx, tx)
}

func retarget(cfg *pgx.ConnConfig, host string, port uint16) {
	cfg.Host, cfg.Port = host, port
	cfg.TLSConfig = tlsForHost(cfg.TLSConfig, host)
	plain, encrypted := cfg.TLSConfig == nil, cfg.TLSConfig != nil
	var fallbacks []*pgconn.FallbackConfig
	for _, fallback := range cfg.Fallbacks {
		if fallback.TLSConfig == nil && !plain {
			plain = true
			fallbacks = append(fallbacks, &pgconn.FallbackConfig{Host: host, Port: port})
		}
		if fallback.TLSConfig != nil && !encrypted {
			encrypted = true
			fallbacks = append(fallbacks, &pgconn.FallbackConfig{Host: host, Port: port, TLSConfig: tlsForHost(fallback.TLSConfig, host)})
		}
	}
	cfg.Fallbacks = fallbacks
}

func tlsForHost(config *tls.Config, host string) *tls.Config {
	if config == nil || config.ServerName == "" {
		return config
	}
	retargeted := config.Clone()
	retargeted.ServerName = host
	return retargeted
}

type handlerBuilder func(*pgconn.PgConn) ctxwatch.Handler

func protocolCancel(conn *pgconn.PgConn) ctxwatch.Handler {
	return &pgconn.CancelRequestContextWatcherHandler{Conn: conn, DeadlineDelay: cancelGrace}
}

func newPool(url string, readOnly bool, size int32, handler handlerBuilder) (*pgxpool.Pool, error) {
	connCfg, err := connConfig(url, readOnly, handler)
	if err != nil {
		return nil, err
	}
	poolCfg, err := pgxpool.ParseConfig("")
	if err != nil {
		return nil, err
	}
	poolCfg.ConnConfig = connCfg
	poolCfg.MaxConns = size
	return pgxpool.NewWithConfig(context.Background(), poolCfg)
}

func connConfig(url string, readOnly bool, handler handlerBuilder) (*pgx.ConnConfig, error) {
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("the database URL does not parse: %w", redactURLError(err))
	}
	if cfg.ConnectTimeout == 0 {
		cfg.ConnectTimeout = openTimeout
	}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeExec
	cfg.RuntimeParams["application_name"] = ApplicationName
	if readOnly {
		cfg.RuntimeParams["options"] = strings.TrimSpace(cfg.RuntimeParams["options"] + " " + readOnlyOption)
	}
	if handler != nil {
		cfg.BuildContextWatcherHandler = handler
	}
	return cfg, nil
}

func redactURLError(err error) error {
	var parseErr *pgconn.ParseConfigError
	if !errors.As(err, &parseErr) {
		return err
	}
	withoutURL := *parseErr
	withoutURL.ConnString = ""
	return errors.New(strings.TrimPrefix(withoutURL.Error(), "cannot parse ``: "))
}
