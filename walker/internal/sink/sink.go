package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"4gclinical.com/walker/internal/decode"
)

// Sink writes decoded changes to Redis Streams.
type Sink struct {
	rdb    *redis.Client
	prefix string
	db     string
}

// New creates a Sink.
// Stream key = prefix + db + "." + table  (e.g. "cdc.mydb.orders").
func New(rdb *redis.Client, prefix, db string) *Sink {
	return &Sink{rdb: rdb, prefix: prefix, db: db}
}

// Write sends one change to the appropriate Redis stream.
// Returns nil only when the XADD was acknowledged by Redis.
func (s *Sink) Write(ctx context.Context, c decode.Change) error {
	dataJSON, err := json.Marshal(c.Data)
	if err != nil {
		return fmt.Errorf("marshal data: %w", err)
	}

	values := []interface{}{
		"op", c.Op,
		"table", c.Table,
		"schema", c.Schema,
		"lsn", c.LSN,
		"streamed_at", time.Now().UTC().Format(time.RFC3339),
		"data", string(dataJSON),
	}

	if c.Old != nil {
		oldJSON, err := json.Marshal(c.Old)
		if err != nil {
			return fmt.Errorf("marshal old: %w", err)
		}
		values = append(values, "old", string(oldJSON))
	}

	args := &redis.XAddArgs{
		Stream: s.prefix + s.db + "." + c.Table,
		ID:     "*",
		Values: values,
	}

	return s.rdb.XAdd(ctx, args).Err()
}
