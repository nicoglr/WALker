package replication

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
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
// error occurs. The caller should restart on error.
func (r *Runner) Run(ctx context.Context) error {
	conn, err := pgconn.Connect(ctx, r.dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)

	if err := r.ensureSlot(ctx, conn); err != nil {
		return err
	}

	// for logical replication postgres will use the slot's
	// confirmed_flush_lsn, when present
	const startLSN pglogrepl.LSN = 0

	pluginArgs := []string{
		`"format-version" '2'`,
		`"add-tables" '` + joinTables(r.tables) + `'`,
	}

	if err := pglogrepl.StartReplication(ctx, conn, r.slot, startLSN,
		pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs}); err != nil {
		return fmt.Errorf("START_REPLICATION: %w", err)
	}

	slog.Info("replication started", "slot", r.slot)

	var confirmedFlushLSN pglogrepl.LSN
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
				continue
			}
			return fmt.Errorf("receive: %w", err)
		}

		copyData, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
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
			xld, err := pglogrepl.ParseXLogData(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parse XLogData: %w", err)
			}

			changes, err := decode.Parse(xld.WALData)
			if err != nil {
				return fmt.Errorf("decode payload at LSN %s: %w\npayload: %s",
					xld.WALStart, err, string(xld.WALData))
			}

			for _, c := range changes {
				c.LSN = xld.WALStart.String()
				if err := r.sink.Write(ctx, c); err != nil {
					return fmt.Errorf("sink.Write: %w", err)
				}
			}

			// Advance LSN past this record and ack immediately.
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

func (r *Runner) ensureSlot(ctx context.Context, conn *pgconn.PgConn) error {
	res := conn.Exec(ctx,
		"SELECT 1 FROM pg_replication_slots WHERE slot_name='"+r.slot+"'")
	rows, err := res.ReadAll()
	if err != nil {
		return fmt.Errorf("check slot: %w", err)
	}
	if len(rows) > 0 && len(rows[0].Rows) > 0 {
		slog.Info("replication slot exists", "slot", r.slot)
		return nil
	}
	// this should fail in prod since we won't have privileges!!
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
	return strings.Join(tables, ",")
}
