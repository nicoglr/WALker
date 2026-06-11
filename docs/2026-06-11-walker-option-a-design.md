# WALker — Design Spec (Option A: streaming replication)

Status: draft (for hand refinement)
Date: 2026-06-11

## Goal

A small, reliable Go service that reads committed Postgres changes via logical
decoding (wal2json) and writes one event per change to Redis Streams.

Two motivations:
1. Learn WAL / logical-decoding mechanics by speaking the replication protocol directly.
2. Have a lean in-house alternative to Debezium/Connect for a small set of tables.

Priorities, in order: **reliable**, **simple**. Latency is not important.

## Scope

In scope:
- Stream changes for tables `public.orders` and `public.products` (extensible list).
- INSERT / UPDATE / DELETE. (TRUNCATE: ignore for now.)
- One Redis stream per table, database-qualified: `cdc.<db>.<table>`
  (e.g. `cdc.mydb.orders`, `cdc.mydb.products`).
- Survive WALker restarts without losing or duplicating-beyond-at-least-once.
- Local Docker Compose env (reuse existing Postgres from parent dir).

Out of scope (for now):
- Initial snapshot of existing rows (only changes from slot creation onward).
- Schema-change handling / DDL.
- Multiple WALker instances / HA per database. One instance **per database**
  (see DECISIONS — per-DB instance model); no two instances on the same DB/slot.
- Metrics, auth, TLS.

## Delivery semantics

**At-least-once.** A change may be re-delivered after a crash; it is never
silently dropped. This is inherent to Postgres logical replication and cannot
be eliminated via the slot alone (see DECISIONS: "At-least-once duplicate
window").

WALker minimizes the window by reporting the confirmed LSN to Postgres
immediately after each Redis flush (not on a lazy timer). The residual window
cannot reach zero, so the contract is: **downstream consumers must be
idempotent.** Each event carries its source `lsn`; consumers dedup on it
(e.g. idempotent upsert on primary key, ignoring `lsn <= last_seen`).

## Architecture

```
Postgres ──(logical replication slot, wal2json)──▶ WALker ──(XADD)──▶ Redis Streams
  walker_slot                                       (Go)              cdc.orders
  (no publication needed)                                             cdc.products
```

Single Go process. One goroutine owns the replication connection and the
process loop; Redis writes happen inline (simple, sequential).

### Postgres-side setup

- `wal_level=logical` (already set in compose).
- A logical replication slot `walker_slot` using output plugin `wal2json`.
- A publication is NOT required for wal2json (publications are a pgoutput concept).
  Table filtering is done either by wal2json `add-tables` option or in WALker.
- Tables keep **default `REPLICA IDENTITY`** (no `FULL`): wal2json's `identity`
  block then carries only primary-key columns, which is exactly what we want for
  UPDATE/DELETE `old` values.
- wal2json is installed via a thin custom Postgres image (see below). It is NOT
  in stock `postgres:16`.

### Postgres image (wal2json)

wal2json ships in the PGDG apt repo, which the official Debian-based
`postgres:16` image already uses. So it's a one-package install, no compiling.

`postgres/Dockerfile`:
```dockerfile
FROM postgres:16
RUN apt-get update \
 && apt-get install -y postgresql-16-wal2json \
 && rm -rf /var/lib/apt/lists/*
```

Compose change (replace `image: postgres:16` with a build):
```yaml
  postgres:
    build: ./postgres
    container_name: rp_postgres
    command: [postgres, -c, wal_level=logical, ...]   # unchanged
```

Notes:
- The `-16-` must match the base image major version.
- Use the Debian image, NOT `postgres:16-alpine` (no PGDG apk package).
- No `shared_preload_libraries` needed; the plugin loads on demand at slot creation.

Verify:
```bash
docker exec rp_postgres psql -U postgres -d mydb \
  -c "SELECT pg_create_logical_replication_slot('walker_slot','wal2json');"
```

Slot is created once (idempotently) at startup if it does not exist:
`pg_create_logical_replication_slot('walker_slot', 'wal2json')`.

### WALker process loop (the core)

Using `github.com/jackc/pglogrepl` + `github.com/jackc/pgx/v5/pgconn`:

1. Connect with `replication=database`.
2. Ensure slot exists.
3. `START_REPLICATION SLOT walker_slot LOGICAL <startLSN>` with wal2json options
   (`format-version`, `add-tables`, etc.).
4. Receive loop:
   - On `XLogData`: parse the wal2json JSON payload → build event(s) → `XADD`
     to the right Redis stream → on success, advance `confirmedFlushLSN` in
     memory **and immediately `SendStandbyStatusUpdate(confirmedFlushLSN)`**
     (ack-after-flush, not on a lazy timer).
   - On `PrimaryKeepaliveMessage` with replyRequested: send standby status update.
   - The periodic timer (`WALKER_STATUS_INTERVAL`) is only a **fallback
     keepalive for idle periods**, not the primary ack path.
5. On any fatal error: log, exit non-zero, let the supervisor (compose
   `restart: on-failure`) restart. Restart resumes from the slot's confirmed LSN.

**Reliability rule:** never advance `confirmedFlushLSN` past a change until its
`XADD` to Redis has returned success, and report the new LSN to Postgres
immediately after the flush. This is what makes restart safe and keeps the
duplicate window small.

### Operational note: WAL retention while WALker is down

The replication slot pins WAL on the Postgres server from the last confirmed LSN
forward. While WALker is stopped or stalled (e.g. Redis unreachable and XADD
blocking), Postgres cannot recycle that WAL and `pg_wal` grows unbounded — the
classic logical-slot footgun that can fill the disk and take Postgres down.
For this POC we accept the risk (single local instance, short downtime). For
anything beyond it: monitor slot lag (`pg_replication_slots.confirmed_flush_lsn`
vs `pg_current_wal_lsn()`), and/or set `max_slot_wal_keep_size` so Postgres
drops a far-behind slot rather than exhausting disk (slot then needs recreation
— with data loss, by design).

## wal2json payload & event shape

wal2json `format-version=2` emits one message per change (not per transaction),
which keeps mapping simple. Each change looks roughly like:

```json
{
  "action": "I",            // I/U/D
  "schema": "public",
  "table": "orders",
  "columns": [{"name":"id","type":"integer","value":1}, ...],
  "identity": [{"name":"id","type":"integer","value":1}, ...]  // for U/D
}
```

WALker maps each change to a Redis stream entry on `cdc.<db>.<table>`:

| Field | Value |
|---|---|
| `op` | `insert` / `update` / `delete` |
| `table` | e.g. `orders` |
| `schema` | `public` |
| `lsn` | source LSN of the change (string) |
| `streamed_at` | RFC3339 timestamp WALker wrote it |
| `data` | INSERT/UPDATE: full new row as JSON object. DELETE: primary key only. |
| `old` | UPDATE/DELETE: **primary key only** (from wal2json `identity`). |

We deliberately capture only primary-key values for the identity/`old` portion
(not the full pre-image). Postgres default `REPLICA IDENTITY` already emits
exactly the PK in wal2json's `identity` block, so **no `REPLICA IDENTITY FULL`
is required** and WAL stays lean.

`XADD cdc.mydb.orders * op ... table ... data {json} ...`

### Value representation

Column values keep their natural JSON types (typed, not stringified):
- `int`/`bigint` and `numeric`/`decimal` → JSON numbers, carried as **exact
  decimal text** end-to-end (WALker decodes with `json.Decoder.UseNumber()`, so
  no `float64` rounding of large ints or high-precision numerics).
- `text`/`timestamp`/`timestamptz`/`uuid`/`bytea` → JSON strings.
- `json`/`jsonb` → nested JSON (object/array), not an escaped string.
- `bool` → `true`/`false`; SQL `NULL` → JSON `null`.

**Consumer contract:** parse the `data`/`old` JSON with a precision-preserving
reader for `bigint`/`numeric` (e.g. NOT default JavaScript `Number`, which is a
double and will round). WALker itself never loses precision; a lossy consumer
parser would.

## Components (Go packages)

Keep it flat and small:

- `cmd/walker/main.go` — wiring, config, start loop.
- `internal/replication` — connect, ensure slot, START_REPLICATION, receive loop,
  standby status updates.
- `internal/decode` — parse wal2json v2 JSON into a `Change` struct.
- `internal/sink` — Redis client, map `Change` → `XADD`, stream-name resolution.
- `internal/config` — env-based config.

Each has one job and a small interface (`Sink.Write(ctx, Change) error`,
`Decoder.Parse([]byte) ([]Change, error)`), so they can be tested in isolation.

## Configuration (env)

| Var | Default | Meaning |
|---|---|---|
| `WALKER_PG_DSN` | `postgres://postgres:postgres@localhost:5432/mydb` | replication DSN |
| `WALKER_SLOT` | `walker_slot` | replication slot name |
| `WALKER_TABLES` | `public.orders,public.products` | tables to capture |
| `WALKER_DB` | `mydb` | database name, used in stream names (or derived from DSN) |
| `WALKER_REDIS_ADDR` | `localhost:6380` | Redis address |
| `WALKER_STREAM_PREFIX` | `cdc.` | stream name = `prefix + db + "." + table` |
| `WALKER_STATUS_INTERVAL` | `10s` | standby status update cadence |

## Error handling

- **Redis XADD fails:** retry with backoff; do NOT advance LSN. If it keeps
  failing, block (back-pressure) rather than skip. Simplicity over cleverness.
- **Decode error on a payload:** log full payload, exit non-zero (fail loud
  during prototype) — do not silently skip, since that would advance past data.
  Known consequence: because the LSN is not advanced, restart re-reads the same
  payload and crash-loops until a human intervenes. Accepted for now (we *want*
  decode failures loud while learning). Future escape hatch: dead-letter the raw
  payload + advance (see DECISIONS — not implemented).
- **Connection drop:** exit; supervisor restarts; resume from confirmed LSN.
- **Slot missing/invalid:** recreate if absent; if present-but-broken, fail loud.

## Testing

- `internal/decode`: table-driven tests against captured wal2json v2 fixtures
  (sample I/U/D JSON) → expected `Change` structs. No DB needed.
- `internal/sink`: against a real Redis (compose) or miniredis; assert XADD
  fields.
- End-to-end smoke: `make up`, run WALker, `INSERT`/`UPDATE`/`DELETE`, assert
  entries land on `cdc.orders` / `cdc.products` (mirror existing Makefile
  `watch-*` targets).

## Open questions (for hand refinement)

1. ~~**wal2json availability.**~~ RESOLVED: thin custom image installs
   `postgresql-16-wal2json` from PGDG (see "Postgres image" above).
2. Do we want one stream per table, or a single `cdc.all` stream? (Spec assumes
   per-table.)
3. Transaction boundaries: format-version=2 drops explicit BEGIN/COMMIT framing.
   Do we ever need transaction grouping downstream? (Assumed no.)
4. How to represent Postgres types in Redis (everything as JSON strings vs typed)?
5. Backfill/snapshot of pre-existing rows — needed at all for the learning POC?

## DECISIONS

### Chosen: Option A — streaming replication via `pglogrepl`

Speak the Postgres logical-replication protocol directly: `START_REPLICATION`,
receive `XLogData`, send standby status updates to advance the confirmed LSN.

Rationale:
- Directly serves the **learning** goal — this is the real CDC mechanism
  Debezium uses.
- Reliability model is explicit and clean: WALker decides exactly when to send
  the confirmed-flush LSN, so "ack only after Redis XADD succeeds" falls out
  naturally.
- Push-based; efficient when idle via server keepalives.
- Extra cost is bounded boilerplate (keepalive replies + periodic status update).

### Set aside (temporarily): Option B — SQL polling

Poll the slot with `pg_logical_slot_peek_changes()` / `pg_logical_slot_get_changes()`
on a normal connection in a loop; advance with `pg_replication_slot_advance()`.

Why it was attractive:
- **Simplest possible** transport — just SQL over a normal `pgx` connection,
  trivial to print/debug/step through.
- No replication-protocol framing, keepalives, or standby-status timers.
- Latency floor = poll interval, which is fine here.

Why set aside for now:
- The convenient `get_changes` variant consumes **and** advances in one call, so
  a crash after read-but-before-XADD silently loses events. Doing it safely
  requires `peek_changes` + an explicit `pg_replication_slot_advance` only after
  Redis confirms — which adds back its own care/complexity.
- It hides the protocol mechanics we explicitly want to learn.

Revisit Option B if: the streaming boilerplate proves annoying, we drop the
learning goal, or we want the absolute minimum LOE. The decode/sink layers are
transport-agnostic, so switching is a localized change in `internal/replication`.

### Chosen (temporarily): wal2json output plugin

Use `wal2json` (`format-version=2`) as the logical-decoding output plugin,
installed via the thin custom Postgres image.

Why wal2json now:
- **Simplest decode path.** Each change is a self-describing JSON message →
  stateless `json.Unmarshal` in `internal/decode`. No relation/OID bookkeeping.
- Fastest path to seeing real events end-to-end, which serves both the learning
  and lean-alternative goals.
- Install cost turned out to be trivial (one PGDG apt package in a 3-line
  Dockerfile), so the usual "avoid wal2json because of setup" argument doesn't apply.

**This is explicitly a temporary decision.** The likely long-term choice is
`pgoutput` (built into Postgres, no custom image, the plugin Debezium uses).

Switching wal2json → pgoutput later, blast radius:
- ✅ Unchanged: `internal/sink`, `internal/config`, `cmd/walker` wiring, the
  streaming transport in `internal/replication` (`START_REPLICATION`,
  keepalives, standby status updates), and the reliability model (ack
  confirmed-LSN only after Redis success). Event shape is identical.
- ⚠️ Changes: `internal/decode` becomes a **stateful binary decoder** — parse
  the pgoutput message stream (`Begin`/`Relation`/`Insert`/`Update`/`Delete`/
  `Commit`) via `pglogrepl/pgoutput`, and maintain a relation cache
  (`map[OID]RelationInfo`) because data rows carry positional, un-named columns
  that must be joined against earlier `Relation` messages. Must also handle
  "row for not-yet-seen relation" after reconnect.
- ⚠️ Postgres setup changes: pgoutput requires `CREATE PUBLICATION` (table
  filtering moves there), and slot creation + `START_REPLICATION` pass
  `proto_version` + `publication_names`.

Net: architecturally near-trivial (clean boundary, one package + Postgres setup
touched, no ripple), but the work inside `internal/decode` is a non-trivial
stateless-JSON → stateful-binary rewrite (~a focused day). We accept wal2json's
slight type-fidelity looseness and larger wire format for now in exchange for
the simpler decoder.

### At-least-once duplicate window

The slot only knows the LSN WALker last *reported* via `SendStandbyStatusUpdate`,
not the LSN it last *processed*. Two LSN positions exist: in-memory
`confirmedFlushLSN` (advanced per successful XADD) and Postgres's
`confirmed_flush_lsn` (updated only on a reported status update). A crash
rewinds to position #2, so any gap between them is re-delivered.

Chosen mitigation: **report the LSN to Postgres immediately after each Redis
flush** (the periodic timer is demoted to an idle keepalive). For the common
failure mode (WALker crashes, Postgres stays up), Postgres retains the reported
LSN in memory across the reconnect, shrinking the duplicate window to roughly
"events since the last flush" — effectively near-zero.

Why we still don't claim exactly-once:
- `SendStandbyStatusUpdate` is async; a crash can land after XADD but before the
  ack is sent/received.
- Postgres persists `confirmed_flush_lsn` to disk only at checkpoints, so a
  *Postgres* crash can rewind the slot regardless of what was reported.

Therefore the durable contract is at-least-once + idempotent consumers (event
carries `lsn`).

Possible future hardening (b) — sink-side dedup, NOT in scope now:
Make XADD idempotent by writing with an explicit, monotonically-increasing,
LSN-derived stream ID instead of `*`. A replayed event would present an ID
`<=` the stream's current max; Redis rejects it and WALker treats that as
"already delivered," swallowing the duplicate without downstream cooperation.
Caveat: wal2json can emit multiple changes sharing one commit LSN, so the ID
must be `lsn` + an intra-transaction counter to stay unique and ordered. Defer
until/unless we want WALker itself (rather than consumers) to absorb replays.

### Multiple databases: per-DB instance model

Postgres logical decoding is **per-database**: a replication slot is bound to one
database, and a `START_REPLICATION` connection only sees changes in the database
it connected to. There is no server-wide logical stream, so one slot/connection
cannot cover multiple databases.

Chosen: run **one WALker instance per database** (process/container per DB), each
with its own `WALKER_PG_DSN` and slot. Rationale:
- Falls out naturally from the per-DB slot constraint.
- Essentially zero new code — run the binary N times with different DSNs; config
  stays single-DB.
- Full isolation: one DB's fail-loud crash-loop never affects others; independent
  restart/slot/offset. Preserves the "one goroutine, fail loud, supervisor
  restarts" model.

Rejected: one process with N goroutines (slot per DB). It saves containers but
re-implements supervision inside the process (a fatal error in one loop would
otherwise kill all DBs), which contradicts the simplicity goal.

Stream naming is database-qualified to avoid collisions when two databases share
a table name: `cdc.<db>.<table>` (`db` from `WALKER_DB` or derived from the DSN).

Composition with the per-table decision:
- **Database** = deployment unit (instance per DB, per-DB slot).
- **Table** = stream unit within that instance (`cdc.<db>.<table>`).
