# WALker Option A Implementation Plan

> **REQUIRED SUB-SKILL:** Use the executing-plans skill to implement this plan task-by-task.

**Goal:** Build a Go service that reads committed Postgres changes via logical decoding (wal2json) and writes one event per change to Redis Streams, with at-least-once delivery.

**Architecture:** Single Go process using `pglogrepl` + `pgconn` to speak the replication protocol directly. Receives `XLogData` frames, decodes wal2json v2 JSON, and writes `XADD` to `cdc.<db>.<table>` Redis streams (e.g. `cdc.mydb.orders`). Confirmed LSN is advanced **and immediately reported to Postgres** after each successful Redis flush (ack-after-flush); the status timer is an idle-only keepalive.

**Tech Stack:** Go 1.22+, `github.com/jackc/pglogrepl`, `github.com/jackc/pgx/v5/pgconn`, `github.com/redis/go-redis/v9`, `github.com/alicebob/miniredis/v2` (tests), Docker Compose (Postgres + Redis).

**Design Spec:** `docs/2026-06-11-walker-option-a-design.md`

---

## Task 1: Custom Postgres Image with wal2json

**Files:**
- Create: `postgres/Dockerfile`
- Create: `postgres/init.sql`
- Modify: `docker-compose.yml`

**Step 1: Create the Dockerfile**

```dockerfile
# postgres/Dockerfile
FROM postgres:16
RUN apt-get update \
 && apt-get install -y postgresql-16-wal2json \
 && rm -rf /var/lib/apt/lists/*
```

**Step 1b: Create `postgres/init.sql` (schema + seed)**

The compose file mounts this as `/docker-entrypoint-initdb.d/01_init.sql`, so it
runs on first DB init and creates the tables WALker captures. No publication is
needed (wal2json filters via `add-tables`); tables keep default `REPLICA IDENTITY`
(PK-only identity, per the design spec).

```sql
CREATE TABLE orders (
  id            SERIAL PRIMARY KEY,
  customer_name TEXT        NOT NULL,
  item          TEXT        NOT NULL,
  quantity      INT         NOT NULL DEFAULT 1,
  status        TEXT        NOT NULL DEFAULT 'pending',
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
  id          SERIAL PRIMARY KEY,
  name        TEXT        NOT NULL,
  price_cents INT         NOT NULL,
  stock       INT         NOT NULL DEFAULT 0,
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO products (name, price_cents, stock) VALUES
  ('Widget A', 999,  100),
  ('Widget B', 1499, 50),
  ('Gadget X', 2999, 25);

INSERT INTO orders (customer_name, item, quantity, status) VALUES
  ('Alice', 'Widget A', 2, 'pending'),
  ('Bob',   'Gadget X', 1, 'shipped');
```

> Note: these inserts happen during DB init, before any slot exists, so they are
> NOT captured by WALker (the slot only sees changes after its creation). They
> exist so the tables are present and queryable; the smoke test generates the
> actual captured events.

**Step 2: Update docker-compose.yml**

Replace the `postgres` service's `image:` line with a `build:` directive. Also add the WALker service stub and remove the `debezium` service:

```yaml
  postgres:
    build: ./postgres          # was: image: postgres:16
    container_name: rp_postgres
    # ... rest unchanged ...

  walker:
    build: ./walker
    container_name: rp_walker
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    environment:
      WALKER_PG_DSN: "postgres://postgres:postgres@postgres:5432/mydb?replication=database"
      WALKER_SLOT: walker_slot
      WALKER_TABLES: "public.orders,public.products"
      WALKER_REDIS_ADDR: "redis:6379"
      WALKER_DB: "mydb"
      WALKER_STREAM_PREFIX: "cdc."
      WALKER_STATUS_INTERVAL: "10s"
    restart: on-failure
```

**Step 3: Build and verify wal2json is present**

wal2json is a logical-decoding **output plugin**, not a SQL extension, so it does
NOT appear in `pg_available_extensions`. Verify by creating (then dropping) a slot
that uses it:

```bash
docker compose build postgres
docker compose up -d postgres
docker exec rp_postgres psql -U postgres -d mydb \
  -c "SELECT pg_create_logical_replication_slot('t_wal2json','wal2json');" \
  -c "SELECT pg_drop_replication_slot('t_wal2json');"
```

Expected: the first statement returns a `(slot_name, lsn)` row; the second
succeeds. If wal2json is missing you get `ERROR: could not access file "wal2json"`.

**Step 4: Commit**

```bash
git add postgres/Dockerfile postgres/init.sql docker-compose.yml
git commit -m "feat: custom postgres image with wal2json"
```

---

## Task 2: Go Module Scaffold

**Files:**
- Create: `walker/go.mod`
- Create: `walker/go.sum` (generated)
- Create: `walker/cmd/walker/main.go` (stub)
- Create: `walker/internal/config/config.go` (stub)
- Create: `walker/internal/decode/decode.go` (stub)
- Create: `walker/internal/sink/sink.go` (stub)
- Create: `walker/internal/replication/replication.go` (stub)
- Create: `walker/Dockerfile`
- Create: `walker/Makefile`

**Step 1: Initialize the Go module**

```bash
mkdir -p walker
cd walker
go mod init 4gclinical.com/walker
go get github.com/jackc/pglogrepl@latest
go get github.com/jackc/pgx/v5@latest
go get github.com/redis/go-redis/v9@latest
go get github.com/alicebob/miniredis/v2@latest   # test dep
```

**Step 2: Create package stubs**

```bash
mkdir -p cmd/walker internal/config internal/decode internal/sink internal/replication
```

Each stub is just `package <name>` with no exported symbols yet, so the module compiles.

`cmd/walker/main.go`:
```go
package main

func main() {}
```

`internal/config/config.go`:
```go
package config
```

`internal/decode/decode.go`:
```go
package decode
```

`internal/sink/sink.go`:
```go
package sink
```

`internal/replication/replication.go`:
```go
package replication
```

**Step 3: Create the walker Dockerfile (multi-stage)**

```dockerfile
# walker/Dockerfile
FROM golang:1.22-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /walker ./cmd/walker

FROM gcr.io/distroless/static-debian12
COPY --from=build /walker /walker
ENTRYPOINT ["/walker"]
```

**Step 4: Create Makefile**

```makefile
# walker/Makefile
.PHONY: build test lint up down

build:
	go build ./...

test:
	go test ./... -v -count=1

lint:
	golangci-lint run ./...

up:
	docker compose -f ../docker-compose.yml up -d

down:
	docker compose -f ../docker-compose.yml down
```

**Step 5: Verify the module compiles**

```bash
cd walker && go build ./...
```

Expected: no errors, no output.

**Step 6: Commit**

```bash
git add walker/
git commit -m "feat: go module scaffold with stub packages"
```

---

## Task 3: `internal/config` Package

**Files:**
- Modify: `walker/internal/config/config.go`
- Create: `walker/internal/config/config_test.go`

**Step 1: Write the failing test**

`walker/internal/config/config_test.go`:
```go
package config_test

import (
	"testing"
	"time"

	"4gclinical.com/walker/internal/config"
)

func TestDefaults(t *testing.T) {
	t.Setenv("WALKER_PG_DSN", "")
	t.Setenv("WALKER_SLOT", "")
	t.Setenv("WALKER_TABLES", "")
	t.Setenv("WALKER_DB", "")
	t.Setenv("WALKER_REDIS_ADDR", "")
	t.Setenv("WALKER_STREAM_PREFIX", "")
	t.Setenv("WALKER_STATUS_INTERVAL", "")

	cfg := config.Load()

	if cfg.PGDSN != "postgres://postgres:postgres@localhost:5432/mydb" {
		t.Errorf("unexpected PGDSN: %s", cfg.PGDSN)
	}
	if cfg.Slot != "walker_slot" {
		t.Errorf("unexpected Slot: %s", cfg.Slot)
	}
	if len(cfg.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(cfg.Tables))
	}
	if cfg.DB != "mydb" {
		t.Errorf("unexpected DB: %s", cfg.DB)
	}
	if cfg.RedisAddr != "localhost:6380" {
		t.Errorf("unexpected RedisAddr: %s", cfg.RedisAddr)
	}
	if cfg.StreamPrefix != "cdc." {
		t.Errorf("unexpected StreamPrefix: %s", cfg.StreamPrefix)
	}
	if cfg.StatusInterval != 10*time.Second {
		t.Errorf("unexpected StatusInterval: %v", cfg.StatusInterval)
	}
}

func TestEnvOverride(t *testing.T) {
	t.Setenv("WALKER_SLOT", "my_slot")
	t.Setenv("WALKER_TABLES", "public.foo,public.bar,public.baz")
	t.Setenv("WALKER_STATUS_INTERVAL", "30s")

	cfg := config.Load()

	if cfg.Slot != "my_slot" {
		t.Errorf("expected my_slot, got %s", cfg.Slot)
	}
	if len(cfg.Tables) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(cfg.Tables))
	}
	if cfg.StatusInterval != 30*time.Second {
		t.Errorf("expected 30s, got %v", cfg.StatusInterval)
	}
}
```

**Step 2: Run test to verify it fails**

```bash
cd walker && go test ./internal/config/... -v
```

Expected: compile error — `config.Load` undefined.

**Step 3: Implement `config.go`**

```go
package config

import (
	"os"
	"strings"
	"time"
)

type Config struct {
	PGDSN          string
	Slot           string
	Tables         []string
	DB             string
	RedisAddr      string
	StreamPrefix   string
	StatusInterval time.Duration
}

func Load() Config {
	interval, err := time.ParseDuration(getenv("WALKER_STATUS_INTERVAL", "10s"))
	if err != nil {
		interval = 10 * time.Second
	}
	rawTables := getenv("WALKER_TABLES", "public.orders,public.products")
	tables := strings.Split(rawTables, ",")
	for i := range tables {
		tables[i] = strings.TrimSpace(tables[i])
	}
	return Config{
		PGDSN:          getenv("WALKER_PG_DSN", "postgres://postgres:postgres@localhost:5432/mydb"),
		Slot:           getenv("WALKER_SLOT", "walker_slot"),
		Tables:         tables,
		DB:             getenv("WALKER_DB", "mydb"),
		RedisAddr:      getenv("WALKER_REDIS_ADDR", "localhost:6380"),
		StreamPrefix:   getenv("WALKER_STREAM_PREFIX", "cdc."),
		StatusInterval: interval,
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
```

**Step 4: Run tests to verify they pass**

```bash
cd walker && go test ./internal/config/... -v
```

Expected: PASS.

**Step 5: Commit**

```bash
git add walker/internal/config/
git commit -m "feat: config package with env-based loading"
```

---

## Task 4: `internal/decode` Package

**Files:**
- Modify: `walker/internal/decode/decode.go`
- Create: `walker/internal/decode/decode_test.go`
- Create: `walker/internal/decode/testdata/insert.json`
- Create: `walker/internal/decode/testdata/update.json`
- Create: `walker/internal/decode/testdata/delete.json`

**Step 1: Create test fixture files**

`walker/internal/decode/testdata/insert.json`:
```json
{
  "action": "I",
  "schema": "public",
  "table": "orders",
  "columns": [
    {"name": "id",     "type": "integer", "value": 42},
    {"name": "status", "type": "text",    "value": "pending"},
    {"name": "amount", "type": "numeric", "value": "9.99"}
  ]
}
```

`walker/internal/decode/testdata/update.json`:
```json
{
  "action": "U",
  "schema": "public",
  "table": "orders",
  "columns": [
    {"name": "id",     "type": "integer", "value": 42},
    {"name": "status", "type": "text",    "value": "shipped"}
  ],
  "identity": [
    {"name": "id", "type": "integer", "value": 42}
  ]
}
```

`walker/internal/decode/testdata/delete.json`:
```json
{
  "action": "D",
  "schema": "public",
  "table": "orders",
  "identity": [
    {"name": "id", "type": "integer", "value": 42}
  ]
}
```

**Step 2: Write the failing test**

`walker/internal/decode/decode_test.go`:
```go
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
	if c.Op != "insert"   { t.Errorf("op: got %q", c.Op) }
	if c.Schema != "public" { t.Errorf("schema: got %q", c.Schema) }
	if c.Table != "orders"  { t.Errorf("table: got %q", c.Table) }
	if c.Data["id"] != json.Number("42")      { t.Errorf("data.id: got %v", c.Data["id"]) }
	if c.Data["status"] != "pending"          { t.Errorf("data.status: got %v", c.Data["status"]) }
	if c.Old != nil                            { t.Errorf("old should be nil for insert") }
}

func TestParseUpdate(t *testing.T) {
	raw := readFixture(t, "update.json")
	changes, err := decode.Parse(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := changes[0]
	if c.Op != "update"       { t.Errorf("op: got %q", c.Op) }
	if c.Data["status"] != "shipped" { t.Errorf("data.status: got %v", c.Data["status"]) }
	if c.Old == nil            { t.Error("old should be set for update") }
	if c.Old["id"] != json.Number("42") { t.Errorf("old.id: got %v", c.Old["id"]) }
}

func TestParseDelete(t *testing.T) {
	raw := readFixture(t, "delete.json")
	changes, err := decode.Parse(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := changes[0]
	if c.Op != "delete"       { t.Errorf("op: got %q", c.Op) }
	if c.Data["id"] != json.Number("42") { t.Errorf("data.id: got %v", c.Data["id"]) }
}

func TestParseTruncateIgnored(t *testing.T) {
	raw := []byte(`{"action":"T","schema":"public","table":"orders"}`)
	changes, err := decode.Parse(raw)
	if err != nil { t.Fatalf("unexpected error: %v", err) }
	if len(changes) != 0 { t.Errorf("expected 0 changes for TRUNCATE, got %d", len(changes)) }
}
```

**Step 3: Run test to verify it fails**

```bash
cd walker && go test ./internal/decode/... -v
```

Expected: compile error — `decode.Parse` undefined.

**Step 4: Implement `decode.go`**

```go
package decode

import "encoding/json"

// Change is one decoded CDC event.
type Change struct {
	Op     string                 // "insert" | "update" | "delete"
	Schema string
	Table  string
	LSN    string                 // set by replication layer, not decode
	Data   map[string]interface{} // new row / identity for delete
	Old    map[string]interface{} // identity columns for update/delete (nil for insert)
}

// wal2json v2 on-wire types (unexported)
type w2Column struct {
	Name  string      `json:"name"`
	Value interface{} `json:"value"`
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
	dec := json.NewDecoder(bytesReader(raw))
	dec.UseNumber() // load-bearing: keep bigint/numeric as exact text, not float64 (which would silently round)
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
		c.Data = columnsToMap(w.Columns)
	case "D":
		c.Data = columnsToMap(w.Identity)
	}

	// Old = identity for U/D
	if w.Action == "U" || w.Action == "D" {
		if len(w.Identity) > 0 {
			c.Old = columnsToMap(w.Identity)
		}
	}

	return []Change{c}, nil
}

func columnsToMap(cols []w2Column) map[string]interface{} {
	m := make(map[string]interface{}, len(cols))
	for _, col := range cols {
		m[col.Name] = col.Value
	}
	return m
}

// bytesReader wraps []byte in a minimal io.Reader for json.NewDecoder.
import "bytes"
func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }
```

> Note: The `import "bytes"` inside a function body is invalid Go; fix by moving imports to the top of the file. The plan shows the intent; clean it up when writing the actual file.

Correct `decode.go` imports block:
```go
import (
    "bytes"
    "encoding/json"
)
```

And remove the inline `import "bytes"` line.

**Step 5: Run tests to verify they pass**

```bash
cd walker && go test ./internal/decode/... -v
```

Expected: all 4 tests PASS.

**Step 6: Commit**

```bash
git add walker/internal/decode/
git commit -m "feat: wal2json v2 decoder with table-driven tests"
```

---

## Task 5: `internal/sink` Package

**Files:**
- Modify: `walker/internal/sink/sink.go`
- Create: `walker/internal/sink/sink_test.go`

**Step 1: Write the failing test**

`walker/internal/sink/sink_test.go`:
```go
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

func newTestSink(t *testing.T) (*sink.Sink, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := sink.New(rdb, "cdc.", "testdb")
	return s, mr
}

func TestWriteInsert(t *testing.T) {
	s, mr := newTestSink(t)
	ctx := context.Background()

	c := decode.Change{
		Op: "insert", Schema: "public", Table: "orders", LSN: "0/1234",
		Data: map[string]interface{}{"id": json.Number("1"), "status": "pending"},
	}
	if err := s.Write(ctx, c); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Verify entry exists on cdc.testdb.orders
	entries := mr.XRange("cdc.testdb.orders", "-", "+")
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	fields := entries[0].Fields
	if fields["op"] != "insert"   { t.Errorf("op: %q", fields["op"]) }
	if fields["table"] != "orders" { t.Errorf("table: %q", fields["table"]) }
	if fields["lsn"] != "0/1234"  { t.Errorf("lsn: %q", fields["lsn"]) }
	if fields["schema"] != "public" { t.Errorf("schema: %q", fields["schema"]) }
	// data must be valid JSON
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(fields["data"]), &data); err != nil {
		t.Errorf("data not valid JSON: %v", err)
	}
	if _, ok := fields["streamed_at"]; !ok {
		t.Error("streamed_at field missing")
	}
}

func TestWriteDelete(t *testing.T) {
	s, mr := newTestSink(t)
	ctx := context.Background()

	c := decode.Change{
		Op: "delete", Schema: "public", Table: "products", LSN: "0/5678",
		Data: map[string]interface{}{"id": json.Number("99")},
		Old:  map[string]interface{}{"id": json.Number("99")},
	}
	if err := s.Write(ctx, c); err != nil {
		t.Fatalf("Write: %v", err)
	}

	entries := mr.XRange("cdc.testdb.products", "-", "+")
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	fields := entries[0].Fields
	if fields["op"] != "delete" { t.Errorf("op: %q", fields["op"]) }
	if fields["old"] == "" { t.Error("old field should be set for delete") }
}
```

**Step 2: Run test to verify it fails**

```bash
cd walker && go test ./internal/sink/... -v
```

Expected: compile error — `sink.New` undefined.

**Step 3: Implement `sink.go`**

```go
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

	args := &redis.XAddArgs{
		Stream: s.prefix + s.db + "." + c.Table,
		ID:     "*",
		Values: map[string]interface{}{
			"op":          c.Op,
			"table":       c.Table,
			"schema":      c.Schema,
			"lsn":         c.LSN,
			"streamed_at": time.Now().UTC().Format(time.RFC3339),
			"data":        string(dataJSON),
		},
	}

	if c.Old != nil {
		oldJSON, err := json.Marshal(c.Old)
		if err != nil {
			return fmt.Errorf("marshal old: %w", err)
		}
		args.Values["old"] = string(oldJSON)
	}

	return s.rdb.XAdd(ctx, args).Err()
}
```

**Step 4: Run tests to verify they pass**

```bash
cd walker && go test ./internal/sink/... -v
```

Expected: all tests PASS.

**Step 5: Commit**

```bash
git add walker/internal/sink/
git commit -m "feat: redis sink with XADD and miniredis tests"
```

---

## Task 6: `internal/replication` Package

**Files:**
- Modify: `walker/internal/replication/replication.go`

> This package speaks the Postgres logical-replication protocol. Unit testing requires a live Postgres; integration-level coverage is provided by the Task 8 smoke test. Keep the code small and rely on the smoke test for validation.

**Step 1: Implement `replication.go`**

```go
package replication

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"4gclinical.com/walker/internal/decode"
	"4gclinical.com/walker/internal/sink"
)

// Runner owns the replication connection and receive loop.
type Runner struct {
	dsn            string
	slot           string
	tables         []string
	sink           *sink.Sink
	statusInterval time.Duration
}

func New(dsn, slot string, tables []string, s *sink.Sink, statusInterval time.Duration) *Runner {
	return &Runner{dsn: dsn, slot: slot, tables: tables, sink: s, statusInterval: statusInterval}
}

// Run starts the replication loop. It blocks until ctx is cancelled or a fatal
// error occurs (connection drop, decode error). The caller should restart on error.
func (r *Runner) Run(ctx context.Context) error {
	conn, err := pgconn.Connect(ctx, r.dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)

	if err := r.ensureSlot(ctx, conn); err != nil {
		return err
	}

	// Always start from 0. For a logical slot, Postgres resumes from the slot's
	// own server-side position (confirmed_flush_lsn, bounded by restart_lsn)
	// regardless of the LSN we pass. This is identical whether the slot was just
	// created by ensureSlot or pre-provisioned by an admin, so there is no need to
	// query confirmed_flush_lsn (which can be NULL on a brand-new slot and would
	// otherwise crash startup).
	const startLSN pglogrepl.LSN = 0

	// Build wal2json options.
	pluginArgs := []string{
		`"format-version" '2'`,
		`"add-tables" '` + joinTables(r.tables) + `'`,
	}

	if err := pglogrepl.StartReplication(ctx, conn, r.slot, startLSN,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs}); err != nil {
		return fmt.Errorf("START_REPLICATION: %w", err)
	}

	slog.Info("replication started", "slot", r.slot)

	var confirmedFlushLSN pglogrepl.LSN // 0 until first XLogData; only used to report progress
	statusDeadline := time.Now().Add(r.statusInterval)

	for {
		if time.Now().After(statusDeadline) {
			if err := sendStatus(ctx, conn, confirmedFlushLSN); err != nil {
				return fmt.Errorf("standby status: %w", err)
			}
			statusDeadline = time.Now().Add(r.statusInterval)
		}

		receiveCtx, cancel := context.WithDeadline(ctx, statusDeadline)
		rawMsg, err := conn.ReceiveMessage(receiveCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue // deadline hit, loop to send status
			}
			return fmt.Errorf("receive: %w", err)
		}

		if rawMsg, ok := rawMsg.(*pgproto3.CopyData); ok {
			switch rawMsg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(rawMsg.Data[1:])
				if err != nil {
					return fmt.Errorf("parse keepalive: %w", err)
				}
				if pkm.ReplyRequested {
					if err := sendStatus(ctx, conn, confirmedFlushLSN); err != nil {
						return err
					}
					statusDeadline = time.Now().Add(r.statusInterval)
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(rawMsg.Data[1:])
				if err != nil {
					return fmt.Errorf("parse XLogData: %w", err)
				}

				changes, err := decode.Parse(xld.WALData)
				if err != nil {
					// Fail loud on decode error per spec.
					return fmt.Errorf("decode payload at LSN %s: %w\npayload: %s",
						xld.WALStart, err, string(xld.WALData))
				}

				for _, c := range changes {
					c.LSN = xld.WALStart.String()
					if err := r.sink.Write(ctx, c); err != nil {
						return fmt.Errorf("sink.Write: %w", err)
					}
				}

				// Advance past the record (start + length), not just to its start.
				// Using WALStart alone would re-deliver this record on every restart.
				endLSN := xld.WALStart + pglogrepl.LSN(len(xld.WALData))
				if endLSN > confirmedFlushLSN {
					confirmedFlushLSN = endLSN
					if err := sendStatus(ctx, conn, confirmedFlushLSN); err != nil {
						return fmt.Errorf("ack LSN: %w", err)
					}
					statusDeadline = time.Now().Add(r.statusInterval)
				}
			}
		}
	}
}

func (r *Runner) ensureSlot(ctx context.Context, conn *pgconn.PgConn) error {
	// Check if slot exists. If it does (e.g. pre-provisioned by an admin), we are
	// done — WALker can run with a consume-only role that lacks CREATE rights.
	// Only the create branch below requires the replication/create privilege.
	res := conn.Exec(ctx,
		"SELECT 1 FROM pg_replication_slots WHERE slot_name=$1", r.slot)
	rows, err := res.ReadAll()
	if err != nil {
		return fmt.Errorf("check slot: %w", err)
	}
	if len(rows) > 0 && len(rows[0].Rows) > 0 {
		slog.Info("replication slot exists", "slot", r.slot)
		return nil
	}
	// Create slot (requires create privilege; skip provisioning if you pre-create).
	if _, err := pglogrepl.CreateReplicationSlot(ctx, conn, r.slot, "wal2json",
		pglogrepl.CreateReplicationSlotOptions{Temporary: false}); err != nil {
		return fmt.Errorf("create slot: %w", err)
	}
	slog.Info("replication slot created", "slot", r.slot)
	return nil
}

func sendStatus(ctx context.Context, conn *pgconn.PgConn, lsn pglogrepl.LSN) error {
	return pglogrepl.SendStandbyStatusUpdate(ctx, conn,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: lsn})
}

func joinTables(tables []string) string {
	// wal2json expects comma-separated "schema.table" pairs.
	result := ""
	for i, t := range tables {
		if i > 0 {
			result += ","
		}
		result += t
	}
	return result
}
```

**Step 2: Verify it compiles**

```bash
cd walker && go build ./internal/replication/...
```

Expected: no errors.

**Step 3: Commit**

```bash
git add walker/internal/replication/
git commit -m "feat: replication runner with pglogrepl receive loop"
```

---

## Task 7: `cmd/walker/main.go` — Wiring

**Files:**
- Modify: `walker/cmd/walker/main.go`

**Step 1: Implement `main.go`**

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
	"4gclinical.com/walker/internal/config"
	"4gclinical.com/walker/internal/replication"
	"4gclinical.com/walker/internal/sink"
)

func main() {
	cfg := config.Load()

	slog.Info("WALker starting",
		"slot", cfg.Slot,
		"tables", cfg.Tables,
		"redis", cfg.RedisAddr,
	)

	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
	})

	s := sink.New(rdb, cfg.StreamPrefix, cfg.DB)
	runner := replication.New(cfg.PGDSN, cfg.Slot, cfg.Tables, s, cfg.StatusInterval)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := runner.Run(ctx); err != nil {
		slog.Error("runner exited", "err", err)
		os.Exit(1)
	}
}
```

**Step 2: Verify it compiles**

```bash
cd walker && go build ./cmd/walker/...
```

Expected: no errors, produces `walker` binary.

**Step 3: Commit**

```bash
git add walker/cmd/walker/main.go
git commit -m "feat: main wiring — config, sink, replication runner"
```

---

## Task 8: Makefile Targets & End-to-End Smoke Test

**Files:**
- Modify: `walker/Makefile` (add smoke target)
- Create: `Makefile` (root, convenience wrapper)

**Step 1: Add smoke target to `walker/Makefile`**

The whole recipe must run in **one shell** (so the background PID survives and
`kill` works), and the test INSERT must satisfy the `orders` NOT NULL columns
(`customer_name`, `item`). Use `.ONESHELL` + a single `bash` block:

```makefile
# Add to walker/Makefile
.ONESHELL:
SHELL := /bin/bash
.PHONY: smoke

smoke: ## End-to-end smoke: bring up stack, run walker, insert a row, check Redis
	@set -euo pipefail
	echo "=== Starting stack ==="
	docker compose -f ../docker-compose.yml up -d postgres redis
	echo "=== Waiting for Postgres ==="
	until docker exec rp_postgres pg_isready -U postgres -d mydb; do sleep 1; done
	echo "=== Building walker ==="
	go build -o /tmp/walker ./cmd/walker
	echo "=== Starting walker in background ==="
	WALKER_PG_DSN="postgres://postgres:postgres@localhost:5432/mydb?replication=database" \
	  WALKER_REDIS_ADDR="localhost:6380" \
	  /tmp/walker &
	WALKER_PID=$$!
	trap 'kill $$WALKER_PID 2>/dev/null || true' EXIT
	sleep 2
	echo "=== Inserting test row ==="
	docker exec rp_postgres psql -U postgres -d mydb -c \
	  "INSERT INTO orders(customer_name, item, quantity, status) VALUES ('Smoke', 'Widget A', 1, 'smoke-test');"
	sleep 1
	echo "=== Checking Redis stream (expect >= 1) ==="
	docker exec rp_redis redis-cli -p 6379 XLEN cdc.mydb.orders
```

Notes:
- `.ONESHELL` makes Make run all recipe lines in a single shell, so `WALKER_PID`
  and the `trap`-based cleanup work. `$$!` / `$$WALKER_PID` use `$$` to escape
  Make's `$`.
- The INSERT supplies `customer_name` and `item` (both NOT NULL); omitting them
  fails the constraint.

**Step 2: Create root `Makefile`**

```makefile
# Root Makefile
.PHONY: up down build test smoke

up:
	docker compose up -d

down:
	docker compose down -v

build:
	$(MAKE) -C walker build

test:
	$(MAKE) -C walker test

smoke:
	$(MAKE) -C walker smoke
```

**Step 3: Run the unit tests one final time**

```bash
cd walker && go test ./... -v -count=1
```

Expected: all tests PASS (config, decode, sink packages). Replication package has no unit tests (integration only).

**Step 4: Run the smoke test manually**

```bash
make smoke
```

Expected: `XLEN cdc.mydb.orders` returns `1` (or more if the slot had prior events).

**Step 5: Final commit**

```bash
git add Makefile walker/Makefile
git commit -m "feat: smoke test + root Makefile"
```

---

## Open Questions (to resolve before/during implementation)

1. ~~**Postgres `init.sql`**~~ RESOLVED: Task 1 Step 1b creates `postgres/init.sql`
   with the `orders`/`products` schema + seed rows.
2. ~~**Numeric JSON mapping**~~ RESOLVED: keep typed values; `decode` uses
   `json.Decoder.UseNumber()` so `bigint`/`numeric` are carried as exact decimal
   text (no `float64` rounding) and re-marshaled losslessly. Consumer contract
   (parse with a precision-preserving reader) documented in the design spec's
   "Value representation" section.
3. ~~**Start-LSN / `confirmedLSN` query.**~~ RESOLVED: removed entirely.
   `START_REPLICATION` always starts from `0`; for a logical slot Postgres
   resumes from its own server-side position, so no `confirmed_flush_lsn` query
   is needed. Works for both auto-created and pre-provisioned (consume-only) slots.
4. ~~**Module path**~~ RESOLVED: module is `4gclinical.com/walker`.
6. **Backfill** — out of scope; not in this plan.
