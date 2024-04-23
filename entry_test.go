package hltscl

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMakeEntry(t *testing.T) {
	table := NewTableWriter(nil, "foo", "ts")
	loc, err := time.LoadLocation("Europe/Prague")
	if err != nil {
		panic(err)
	}
	ts := time.Date(2024, 4, 23, 14, 21, 45, 0, loc)
	p := table.NewEntry(ts).
		Str("service", "Kontext").
		Str("instance", "production").
		Float("cpu_load", 0.8)
	sql, args := p.ExportForSQL(table.name, table.tsColName)
	assert.Equal(
		t,
		"INSERT INTO foo (ts, cpu_load, instance, service) VALUES ($1, $2, $3, $4)",
		sql,
	)
	assert.Equal(t, ts, args[0])
	assert.InDelta(t, 0.8, args[1], 0.0001)
	assert.Equal(t, "production", args[2])
	assert.Equal(t, "Kontext", args[3])
}

func TestString(t *testing.T) {
	table := NewTableWriter(nil, "foo", "ts")
	loc, err := time.LoadLocation("Europe/Prague")
	if err != nil {
		panic(err)
	}
	ts := time.Date(2024, 4, 23, 14, 21, 45, 0, loc)
	p := table.NewEntry(ts).
		Str("service", "Kontext").
		Str("instance", "production").
		Float("cpu_load", 0.8).
		Bool("has_error", false)
	assert.Equal(
		t,
		"Entry{ts: 2024-04-23 14:21:45 +0200 CEST, cpu_load: 0.800, "+
			"has_error: false, instance: production, service: Kontext}",
		p.String())
}
