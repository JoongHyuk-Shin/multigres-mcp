package db

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgconn/ctxwatch"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	cancelGrace       = 5 * time.Second
	cancelSettleDelay = 100 * time.Millisecond
	cancelResendEvery = 200 * time.Millisecond
	cancelPoolSize    = 1
	cancelKey         = "multigres-mcp.cancel"
)

const cancelSessionQuery Query = `SELECT pg_catalog.pg_cancel_backend(a.pid)
FROM pg_catalog.pg_stat_activity a
WHERE a.pid = $1 AND a.backend_start = $2`

type session struct {
	pid   int32
	start time.Time
}

type canceler struct {
	pool *pgxpool.Pool
}

func newCanceler(url string) (*canceler, error) {
	pool, err := newPool(url, true, cancelPoolSize, nil)
	if err != nil {
		return nil, err
	}
	return &canceler{pool: pool}, nil
}

func (c *canceler) close() { c.pool.Close() }

func (c *canceler) build(pg *pgconn.PgConn) ctxwatch.Handler {
	handler := &sessionCancel{pg: pg, owner: c}
	pg.CustomData()[cancelKey] = handler
	return handler
}

func (c *canceler) cancelSession(ctx context.Context, target session) error {
	return c.readOnly(ctx, func(conn *pgx.Conn) error {
		rows, err := conn.Query(ctx, string(cancelSessionQuery), target.pid, target.start)
		if err != nil {
			return err
		}
		rows.Close()
		return rows.Err()
	})
}

func (c *canceler) terminateSession(ctx context.Context, target session) error {
	return c.readOnly(ctx, func(conn *pgx.Conn) error {
		var ended bool
		return endedOrError(conn.QueryRow(ctx, string(endSessionQuery), target.pid, target.start).Scan(&ended), ended)
	})
}

func (c *canceler) readOnly(ctx context.Context, fn func(*pgx.Conn) error) error {
	pooled, err := c.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer pooled.Release()
	conn := pooled.Conn()
	if _, err := conn.Exec(ctx, string(beginReadOnly)); err != nil {
		return err
	}
	err = fn(conn)
	if _, rollbackErr := conn.Exec(ctx, string(rollback)); err == nil {
		err = rollbackErr
	}
	return err
}

func watchSession(conn *pgx.Conn, target session) {
	if handler := handlerOf(conn); handler != nil {
		handler.target.Store(&target)
	}
}

func unwatchSession(conn *pgx.Conn) {
	if handler := handlerOf(conn); handler != nil {
		handler.target.Store(nil)
	}
}

func handlerOf(conn *pgx.Conn) *sessionCancel {
	handler, _ := conn.PgConn().CustomData()[cancelKey].(*sessionCancel)
	return handler
}

type sessionCancel struct {
	pg       *pgconn.PgConn
	owner    *canceler
	target   atomic.Pointer[session]
	returned chan struct{}
	finished chan struct{}
}

func (h *sessionCancel) HandleCancel(context.Context) {
	h.returned = make(chan struct{})
	h.finished = make(chan struct{})
	deadline := time.Now().Add(cancelGrace)
	_ = h.pg.Conn().SetDeadline(deadline)
	send := h.pg.CancelRequest
	if target := h.target.Load(); target != nil {
		send = func(ctx context.Context) error { return h.owner.cancelSession(ctx, *target) }
	}
	go func() {
		defer close(h.finished)
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()
		for {
			_ = send(ctx)
			select {
			case <-h.returned:
				time.Sleep(cancelSettleDelay)
				return
			case <-ctx.Done():
				return
			case <-time.After(cancelResendEvery):
			}
		}
	}()
}

func (h *sessionCancel) HandleUnwatchAfterCancel() {
	close(h.returned)
	<-h.finished
	_ = h.pg.Conn().SetDeadline(time.Time{})
}
