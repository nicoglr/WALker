package decode_test

import (
	"encoding/json"
	"os"
	"testing"

	"4gclinical.com/walker/internal/decode"
)

func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	return data
}

func TestParseInsert(t *testing.T) {
	raw := readFixture(t, "insert.json")
	changes, err := decode.Parse(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}
	c := changes[0]
	if c.Op != "insert"    { t.Errorf("op: got %q", c.Op) }
	if c.Schema != "public" { t.Errorf("schema: got %q", c.Schema) }
	if c.Table != "orders"  { t.Errorf("table: got %q", c.Table) }
	if c.Data["id"] != json.Number("42")   { t.Errorf("data.id: got %v", c.Data["id"]) }
	if c.Data["status"] != "pending"       { t.Errorf("data.status: got %v", c.Data["status"]) }
	if c.Old != nil                         { t.Errorf("old should be nil for insert") }
}

func TestParseUpdate(t *testing.T) {
	raw := readFixture(t, "update.json")
	changes, err := decode.Parse(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := changes[0]
	if c.Op != "update"            { t.Errorf("op: got %q", c.Op) }
	if c.Data["status"] != "shipped" { t.Errorf("data.status: got %v", c.Data["status"]) }
	if c.Old == nil                 { t.Error("old should be set for update") }
	if c.Old["id"] != json.Number("42") { t.Errorf("old.id: got %v", c.Old["id"]) }
}

func TestParseDelete(t *testing.T) {
	raw := readFixture(t, "delete.json")
	changes, err := decode.Parse(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := changes[0]
	if c.Op != "delete"            { t.Errorf("op: got %q", c.Op) }
	if c.Data["id"] != json.Number("42") { t.Errorf("data.id: got %v", c.Data["id"]) }
}

func TestParseTruncateIgnored(t *testing.T) {
	raw := []byte(`{"action":"T","schema":"public","table":"orders"}`)
	changes, err := decode.Parse(raw)
	if err != nil { t.Fatalf("unexpected error: %v", err) }
	if len(changes) != 0 { t.Errorf("expected 0 changes for TRUNCATE, got %d", len(changes)) }
}
