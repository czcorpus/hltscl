package hltscl

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const defaultMaxNumErrors = 50
const defaultTimeout = 10 * time.Second
const defaultBufferSize = 100

type WriteError struct {
	Entry Entry
	Err   error
}

type TableWriter struct {
	conn      *pgxpool.Pool
	name      string
	tsColName string
	loc       *time.Location
	ch        chan Entry
	chErr     chan WriteError
}

type optItems struct {
	timeout      time.Duration
	bufferSize   int
	maxNumErrors int
}

type OptsFn func(*optItems)

func WithTimeout(tm time.Duration) OptsFn {
	return func(o *optItems) {
		o.timeout = tm
	}
}

func WithBufferSize(size int) OptsFn {
	return func(o *optItems) {
		o.bufferSize = size
	}
}

func WithMaxNumErrors(num int) OptsFn {
	return func(o *optItems) {
		o.maxNumErrors = num
	}
}

// Activate
// defaults:
//
//	defaultMaxNumErrors = 50
//	defaultTimeout = 10 * time.Second
//	defaultBufferSize = 100
func (table *TableWriter) Activate(ctx context.Context, opts ...OptsFn) (chan<- Entry, <-chan WriteError) {
	o := optItems{
		maxNumErrors: defaultMaxNumErrors,
		timeout:      defaultTimeout,
		bufferSize:   defaultBufferSize,
	}
	for _, oItem := range opts {
		oItem(&o)
	}

	table.ch = make(chan Entry, o.bufferSize)
	table.chErr = make(chan WriteError, o.bufferSize)
	go func() {
		defer close(table.chErr)
		var numErrors int
		for {
			select {
			case entry := <-table.ch:
				if numErrors <= o.maxNumErrors {
					sql, args := entry.ExportForSQL(table.name, table.tsColName)
					ctx2, cancel := context.WithTimeout(ctx, o.timeout)
					_, err := table.conn.Exec(ctx2, sql, args...)
					if err == nil {
						numErrors = 0

					} else {
						table.chErr <- WriteError{
							Entry: entry,
							Err:   fmt.Errorf("failed to write TimescaleDB entry: %w", err),
						}
						numErrors++
					}
					cancel()

				} else {
					table.chErr <- WriteError{
						Entry: entry,
						Err:   fmt.Errorf("failed to write TimescaleDB entry: max num errors reached, restart required"),
					}
				}
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
