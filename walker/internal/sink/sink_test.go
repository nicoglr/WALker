package sink_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, s.Write(ctx, c))

	entries, err := rdb.XRange(ctx, "test-instance.cdc.orders", "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 1)

	fields := entries[0].Values
	assert.Equal(t, "insert", fields["op"])
	assert.Equal(t, "orders", fields["table"])
	assert.Equal(t, "0/1234", fields["lsn"])
	assert.Equal(t, "public", fields["schema"])
	assert.Contains(t, fields, "streamed_at")

	var data map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(fields["data"].(string)), &data), "data must be valid JSON")
}

func TestWriteDelete(t *testing.T) {
	s, rdb := newTestSink(t)
	ctx := context.Background()

	c := decode.Change{
		Op: "delete", Schema: "public", Table: "products", LSN: "0/5678",
		Data: map[string]interface{}{"id": json.Number("99")},
		Old:  map[string]interface{}{"id": json.Number("99")},
	}
	require.NoError(t, s.Write(ctx, c))

	entries, err := rdb.XRange(ctx, "test-instance.cdc.products", "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, entries, 1)

	fields := entries[0].Values
	assert.Equal(t, "delete", fields["op"])
	assert.NotEmpty(t, fields["old"], "old field should be set for delete")
}
