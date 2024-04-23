package hltscl

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

type Entry struct {
	ts   time.Time
	loc  *time.Location
	data map[string]any
}

func (e *Entry) Str(key, value string) *Entry {
	e.data[key] = value
	return e
}

func (e *Entry) Int(key string, value int) *Entry {
	e.data[key] = value
	return e
}

func (e *Entry) Float(key string, value float64) *Entry {
	e.data[key] = value
	return e
}

func (e *Entry) ExportForSQL(table, tsCol string) (string, []any) {
	keys := make([]string, 0, len(e.data)+1) // +1 <= timestamp
	for k := range e.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sqlEntries strings.Builder
	args := make([]any, len(keys)+1)

	sqlEntries.WriteString(fmt.Sprintf("INSERT INTO %s (%s", table, tsCol))
	for _, k := range keys {
		sqlEntries.WriteString(", " + k)
	}
	sqlEntries.WriteString(") VALUES ($1")
	args[0] = e.ts
	for i, k := range keys {
		sqlEntries.WriteString(fmt.Sprintf(", $%d", i+2))
		args[i+1] = e.data[k]
	}
	sqlEntries.WriteString(")")
	return sqlEntries.String(), args
}
