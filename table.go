package hltscl

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultTimeout    = 10 * time.Second
	defaultMinTimeout = 1 * time.Second
	defaultBufferSize = 100
)

type WriteError struct {
	Entry Entry
	Err   error
}

type TableWriter struct {
	conn        *pgxpool.Pool
	name        string
	tsColName   string
	loc         *time.Location
	ch          chan Entry
	chErr       chan WriteError
	timeoutCtrl *timeoutController
}

type optItems struct {
	timeout    time.Duration
	minTimeout time.Duration
	bufferSize int
}

type OptsFn func(*optItems)

func WithTimeout(tm time.Duration) OptsFn {
	return func(o *optItems) {
		o.timeout = tm
	}
}

func WithMinTimeout(tm time.Duration) OptsFn {
	return func(o *optItems) {
		o.minTimeout = tm
	}
}

func WithBufferSize(size int) OptsFn {
	return func(o *optItems) {
		o.bufferSize = size
	}
}

func (table *TableWriter) CurrentQueryTimeout() time.Duration {
	return table.timeoutCtrl.currentTimeout
}

// Activate starts TimescaleDB writer and provides a writing channel to the client along with
// a reading channel for error reporting. Writing uses an adaptive timeout to prevent flooding
// of the incoming channel with requests in case the target database cannot handle queries.
//
// defaults:
//
//	defaultTimeout = 10 * time.Second
//	defaultMinTimeout = 1 * time.Second
//	defaultBufferSize = 100
func (table *TableWriter) Activate(ctx context.Context, opts ...OptsFn) (chan<- Entry, <-chan WriteError) {
	o := optItems{
		timeout:    defaultTimeout,
		bufferSize: defaultBufferSize,
		minTimeout: defaultMinTimeout,
	}
	for _, oItem := range opts {
		oItem(&o)
	}
	table.timeoutCtrl = newTimeoutController(ctx, o.timeout, o.minTimeout)
	table.ch = make(chan Entry, o.bufferSize)
	table.chErr = make(chan WriteError, o.bufferSize)
	go func() {
		defer close(table.chErr)
		for {
			select {
			case entry := <-table.ch:
				sql, args := entry.ExportForSQL(table.name, table.tsColName)
				ctx2, cancel := context.WithTimeout(ctx, o.timeout)
				_, err := table.conn.Exec(ctx2, sql, args...)
				if err == nil {
					table.timeoutCtrl.reportSuccess()

				} else {
					table.chErr <- WriteError{
						Entry: entry,
						Err:   fmt.Errorf("failed to write TimescaleDB entry: %w", err),
					}
					table.timeoutCtrl.ReportFailure()
				}
				cancel()

			case <-ctx.Done():
				if len(table.ch) > 0 {
					table.chErr <- WriteError{
						Err: fmt.Errorf("writer timed out with %d entries remaining", len(table.ch)),
					}
				}
				return
			}
		}
	}()
	return table.ch, table.chErr
}

func (table *TableWriter) NewEntry(ts time.Time) *Entry {
	return &Entry{
		ts:   ts.In(table.loc),
		loc:  table.loc,
		data: make(map[string]any),
	}
}

func NewTableWriter(conn *pgxpool.Pool, name, tsColName string, loc *time.Location) *TableWriter {
	return &TableWriter{
		conn:      conn,
		name:      name,
		tsColName: tsColName,
		loc:       loc,
	}
}
