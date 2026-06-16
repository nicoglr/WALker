# WALker

A small Go service that reads committed Postgres changes via logical decoding and writes them to Redis Streams — one event per change, one stream per table.

## Learning Goal

**The primary purpose of this project is to learn how Postgres WAL and logical decoding actually work** — speaking the replication protocol directly, exactly as Debezium does under the hood. If you want to understand CDC mechanics firsthand, this is the point.

A useful side effect: a lean, in-house alternative to Debezium for a small set of tables.

## How it works

```
Postgres ──(logical replication slot, wal2json)──▶ WALker ──(XADD)──▶ Redis Streams
```

WALker opens a logical replication connection, starts receiving `XLogData` messages, decodes the wal2json payload, and writes each change to the appropriate Redis stream (`<instance>.cdc.<table>`, e.g. `bayer-17909.cdc.orders`). It reports the confirmed LSN back to Postgres immediately after each successful Redis write — keeping the at-least-once duplicate window as small as possible.

## Delivery semantics

**At-least-once.** A change may be re-delivered after a crash; it is never silently dropped. Each event carries its source `lsn`; downstream consumers should dedup on it (e.g. idempotent upsert ignoring `lsn <= last_seen`).

## Event shape

Each Redis stream entry contains:

| Field | Description |
|---|---|
| `op` | `insert` / `update` / `delete` |
| `table` | e.g. `orders` |
| `schema` | e.g. `public` |
| `lsn` | source LSN (use for dedup) |
| `streamed_at` | RFC3339 timestamp |
| `data` | full new row (INSERT/UPDATE) or PK only (DELETE) |
| `old` | primary key of previous row (UPDATE/DELETE) |

## Configuration

| Env var | Default | Description |
|---|---|---|
| `WALKER_PG_DSN` | `postgres://postgres:postgres@localhost:5432/mydb` | Replication DSN |
| `WALKER_TABLES` | `public.orders,public.products` | Tables to capture |
| `WALKER_INSTANCE_ID` | *(required)* | Application instance ID — leading segment of every stream key (e.g. `bayer-17909`); also drives the replication slot name (`walker_slot_<sanitized-id>`) |
| `WALKER_REDIS_ADDR` | `localhost:6380` | Redis address |
| `WALKER_STREAM_PREFIX` | `cdc` | Stream name prefix (trailing dot stripped automatically) |
| `WALKER_STATUS_INTERVAL` | `10s` | Standby status update cadence (idle keepalive) |

## Running locally

```bash
# Start Postgres (custom image with wal2json) and Redis
docker compose up -d

# Run WALker
go run ./cmd/walker
```

## Code layout

```
cmd/walker/        — wiring, config, entrypoint
internal/replication/ — replication connection, START_REPLICATION loop, standby status updates
internal/decode/   — parse wal2json v2 JSON → Change struct
internal/sink/     — map Change → Redis XADD
internal/config/   — env-based config
```

## Design notes

- Uses `pglogrepl` to speak the replication protocol directly (not SQL polling).
- wal2json output plugin — simple stateless JSON decode. The natural next step would be switching to `pgoutput` (built-in, no custom image), which is a localized change in `internal/decode`.
- One WALker instance per database (Postgres logical slots are per-database).
- On any fatal error: exit non-zero, let the supervisor restart, resume from the confirmed LSN.
- WAL accumulates while WALker is down (the slot pins WAL). Fine for a local POC; monitor slot lag in production.
