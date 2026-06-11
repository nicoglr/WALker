package decode

import (
	"bytes"
	"encoding/json"

	"4gclinical.com/walker/internal/config"
)

// Change is one decoded CDC event.
type Change struct {
	Op     string     // "insert" | "update" | "delete"
	Schema string
	Table  string
	// LSN is the WAL position immediately *after* this record (WALStart + len(WALData)).
	// Consumers deduplicating on LSN must use strict greater-than (>) when comparing
	// against a last-seen LSN, since this value points past the record — not to its
	// start. Example: resume position is the LSN from the last-processed event;
	// re-deliver condition is event.LSN > lastSeen (not >=).
	LSN  string
	Data config.Row // new row (INSERT/UPDATE) or PK only (DELETE)
	Old  config.Row // PK of prior row (UPDATE/DELETE); nil for INSERT
}

// wal2json v2 on-wire types (unexported)
type w2Column struct {
	Name  string `json:"name"`
	Value any    `json:"value"`
}

type w2Change struct {
	Action   string     `json:"action"`
	Schema   string     `json:"schema"`
	Table    string     `json:"table"`
	Columns  []w2Column `json:"columns"`
	Identity []w2Column `json:"identity"`
}

var actionMap = map[string]string{
	"I": "insert",
	"U": "update",
	"D": "delete",
}

// Parse decodes a wal2json v2 single-change message.
// Returns zero changes for TRUNCATE (action="T").
// Returns an error if JSON is malformed.
func Parse(raw []byte) ([]Change, error) {
	var w w2Change
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber() // keep bigint/numeric as exact text, not float64
	if err := dec.Decode(&w); err != nil {
		return nil, err
	}
	op, ok := actionMap[w.Action]
	if !ok {
		// TRUNCATE or unknown — ignore
		return nil, nil
	}

	c := Change{
		Op:     op,
		Schema: w.Schema,
		Table:  w.Table,
	}

	// Data = columns (new row for I/U) or identity (D)
	switch w.Action {
	case "I", "U":
		c.Data = columnsToRow(w.Columns)
	case "D":
		c.Data = columnsToRow(w.Identity)
	}

	// Old = identity for U/D
	if (w.Action == "U" || w.Action == "D") && len(w.Identity) > 0 {
		c.Old = columnsToRow(w.Identity)
	}

	return []Change{c}, nil
}

func columnsToRow(cols []w2Column) config.Row {
	m := make(config.Row, len(cols))
	for _, col := range cols {
		m[col.Name] = col.Value
	}
	return m
}
