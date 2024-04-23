package hltscl

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
	ctx       context.Context
}

func (table *TableWriter) Activate() (chan<- Entry, <-chan WriteError) {
	table.ch = make(chan Entry, 100)
	table.chErr = make(chan WriteError, 100)
	table.ctx = context.Background()
	go func() {
		for entry := range table.ch {
			sql, args := entry.ExportForSQL(table.name, table.tsColName)
			_, err := table.conn.Exec(table.ctx, sql, args...)
			if err != nil {
				table.chErr <- WriteError{Entry: entry, Err: err}
			}
		}
	}()
	return table.ch, table.chErr
}

func (table *TableWriter) NewEntry(ts time.Time) *Entry {
	return &Entry{
		ts:   ts,
		loc:  table.loc,
		data: make(map[string]any),
	}
}

func NewTableWriter(conn *pgxpool.Pool, name, tsColName string) *TableWriter {
	return &TableWriter{
		conn:      conn,
		name:      name,
		tsColName: tsColName,
	}
}
