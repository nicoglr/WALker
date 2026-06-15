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

// replConn is the minimal interface the receive loop needs.
// *pgconn.PgConn satisfies it.
type replConn interface {
	ReceiveMessage(ctx context.Context) (pgproto3.BackendMessage, error)
}

// statusSender is called to send a standby status update with the given LSN.
type statusSender func(ctx context.Context, lsn pglogrepl.LSN) error

// sinkWriter is the minimal interface the receive loop needs from the sink.
// *sink.Sink satisfies it.
type sinkWriter interface {
	Write(ctx context.Context, c decode.Change) error
}

// Streamer owns the replication connection and receive loop.
type Streamer struct {
	dsn            string
	slot           string
	tables         []string
	sink           sinkWriter
	statusInterval time.Duration
}

func New(dsn, slot string, tables []string, s *sink.Sink, statusInterval time.Duration) *Streamer {
	return &Streamer{dsn: dsn, slot: slot, tables: tables, sink: s, statusInterval: statusInterval}
}

// Run starts the replication loop. It blocks until ctx is cancelled or a fatal
// error occurs. The caller should restart on error.
func (r *Streamer) Run(ctx context.Context) error {
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

	sender := func(ctx context.Context, lsn pglogrepl.LSN) error {
		return sendStatus(ctx, conn, lsn)
	}
	return r.receiveLoop(ctx, conn, sender)
}

// receiveLoop runs the message receive loop
func (r *Streamer) receiveLoop(ctx context.Context, conn replConn, send statusSender) error {
	var confirmedFlushLSN pglogrepl.LSN
	statusDeadline := time.Now().Add(r.statusInterval)

	for {
		if time.Now().After(statusDeadline) {
			if err := send(ctx, confirmedFlushLSN); err != nil {
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
		if len(copyData.Data) == 0 {
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(copyData.Data[1:])
			if err != nil {
				return fmt.Errorf("parse keepalive: %w", err)
			}
			if pkm.ReplyRequested {
				if err := send(ctx, confirmedFlushLSN); err != nil {
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

			// endLSN is the WAL position immediately after this record.
			// It is stamped on each event and used as the ack LSN.
			// See Change.LSN doc for consumer dedup semantics.
			endLSN := xld.WALStart + pglogrepl.LSN(len(xld.WALData))

			for _, c := range changes {
				c.LSN = endLSN.String()
				if err := r.sink.Write(ctx, c); err != nil {
					return fmt.Errorf("sink.Write: %w", err)
				}
			}
			if endLSN > confirmedFlushLSN {
				confirmedFlushLSN = endLSN
				if err := send(ctx, confirmedFlushLSN); err != nil {
					return fmt.Errorf("ack LSN: %w", err)
				}
				statusDeadline = time.Now().Add(r.statusInterval)
			}
		}
	}
}

func (r *Streamer) ensureSlot(ctx context.Context, conn *pgconn.PgConn) error {
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
