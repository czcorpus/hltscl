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

func (e *Entry) String() string {
	keys := e.orderedDataKeys()
	var ans strings.Builder
	ans.WriteString(fmt.Sprintf("Entry{ts: %s", e.ts))
	for _, k := range keys {
		v := e.data[k]
		switch tv := v.(type) {
		case float64, float32:
			ans.WriteString(fmt.Sprintf(", %s: %.3f", k, tv))
		case int, int64:
			ans.WriteString(fmt.Sprintf(", %s: %d", k, tv))
		case bool:
			ans.WriteString(fmt.Sprintf(", %s: %t", k, tv))
		default:
			ans.WriteString(fmt.Sprintf(", %s: %s", k, tv))
		}

	}
	ans.WriteString("}")
	return ans.String()
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

func (e *Entry) Bool(key string, value bool) *Entry {
	e.data[key] = value
	return e
}

func (e *Entry) orderedDataKeys() []string {
	keys := make([]string, 0, len(e.data)+1) // +1 <= timestamp
	for k := range e.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (e *Entry) ExportForSQL(table, tsCol string) (string, []any) {
	keys := e.orderedDataKeys()
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
