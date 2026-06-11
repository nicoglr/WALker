package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
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
// It retries with exponential backoff on transient Redis errors, blocking the
// replication loop (and therefore WAL advancement) until the write succeeds or
// ctx is cancelled. This implements the at-least-once back-pressure contract:
// the LSN is never advanced past a change that has not been durably written.
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

	backoff := 100 * time.Millisecond
	for {
		err := s.rdb.XAdd(ctx, args).Err()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		slog.Warn("XADD failed, retrying", "stream", args.Stream, "err", err, "backoff", backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}
