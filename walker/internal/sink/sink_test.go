package sink_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"4gclinical.com/walker/internal/decode"
	"4gclinical.com/walker/internal/sink"
)

func newTestSink(t *testing.T) (*sink.Sink, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := sink.New(rdb, "cdc", "test-instance")
	return s, rdb
}

func TestWriteInsert(t *testing.T) {
	s, rdb := newTestSink(t)
	ctx := context.Background()

	c := decode.Change{
		Op: "insert", Schema: "public", Table: "orders", LSN: "0/1234",
		Data: map[string]interface{}{"id": json.Number("1"), "status": "pending"},
	}
	if err := s.Write(ctx, c); err != nil {
		t.Fatalf("Write: %v", err)
	}

	entries, err := rdb.XRange(ctx, "test-instance.cdc.orders", "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	fields := entries[0].Values
	if fields["op"] != "insert"    { t.Errorf("op: %q", fields["op"]) }
	if fields["table"] != "orders"  { t.Errorf("table: %q", fields["table"]) }
	if fields["lsn"] != "0/1234"   { t.Errorf("lsn: %q", fields["lsn"]) }
	if fields["schema"] != "public" { t.Errorf("schema: %q", fields["schema"]) }
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(fields["data"].(string)), &data); err != nil {
		t.Errorf("data not valid JSON: %v", err)
	}
	if _, ok := fields["streamed_at"]; !ok {
		t.Error("streamed_at field missing")
	}
}

func TestWriteDelete(t *testing.T) {
	s, rdb := newTestSink(t)
	ctx := context.Background()

	c := decode.Change{
		Op: "delete", Schema: "public", Table: "products", LSN: "0/5678",
		Data: map[string]interface{}{"id": json.Number("99")},
		Old:  map[string]interface{}{"id": json.Number("99")},
	}
	if err := s.Write(ctx, c); err != nil {
		t.Fatalf("Write: %v", err)
	}

	entries, err := rdb.XRange(ctx, "test-instance.cdc.products", "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	fields := entries[0].Values
	if fields["op"] != "delete"  { t.Errorf("op: %q", fields["op"]) }
	if fields["old"] == ""       { t.Error("old field should be set for delete") }
}
