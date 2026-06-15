package replication

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgproto3"
	"4gclinical.com/walker/internal/decode"
)

// ---------------------------------------------------------------------------
// Fake helpers
// ---------------------------------------------------------------------------

// fakeConn returns scripted messages then returns errDone.
type fakeConn struct {
	msgs []pgproto3.BackendMessage
	idx  int
}

var errDone = errors.New("scripted messages exhausted")

func (f *fakeConn) ReceiveMessage(_ context.Context) (pgproto3.BackendMessage, error) {
	if f.idx >= len(f.msgs) {
		return nil, errDone
	}
	m := f.msgs[f.idx]
	f.idx++
	return m, nil
}

// recordingSender records every LSN passed to it.
type recordingSender struct {
	lsns []pglogrepl.LSN
}

func (s *recordingSender) Send(_ context.Context, lsn pglogrepl.LSN) error {
	s.lsns = append(s.lsns, lsn)
	return nil
}

// recordingSink records every Change written to it.
type recordingSink struct {
	changes []decode.Change
	err     error // if non-nil, returned on Write
}

func (s *recordingSink) Write(_ context.Context, c decode.Change) error {
	if s.err != nil {
		return s.err
	}
	s.changes = append(s.changes, c)
	return nil
}

// ---------------------------------------------------------------------------
// Frame builders
// ---------------------------------------------------------------------------

// keepalive builds a CopyData frame for a PrimaryKeepaliveMessage.
func keepalive(replyRequested bool) *pgproto3.CopyData {
	buf := make([]byte, 18) // 1 byte ID + 17 bytes payload
	buf[0] = byte(pglogrepl.PrimaryKeepaliveMessageByteID)
	// ServerWALEnd (8 bytes) + ServerTime (8 bytes) + ReplyRequested (1 byte)
	// all zero except ReplyRequested
	if replyRequested {
		buf[17] = 1
	}
	return &pgproto3.CopyData{Data: buf}
}

// xlogData builds a CopyData frame for an XLogData message.
func xlogData(walStart pglogrepl.LSN, payload []byte) *pgproto3.CopyData {
	// 1 byte ID + 8 WALStart + 8 ServerWALEnd + 8 ServerTime + payload
	buf := make([]byte, 1+24+len(payload))
	buf[0] = byte(pglogrepl.XLogDataByteID)
	binary.BigEndian.PutUint64(buf[1:], uint64(walStart))
	// ServerWALEnd and ServerTime: zero is fine
	copy(buf[1+24:], payload)
	return &pgproto3.CopyData{Data: buf}
}

// newStreamer creates a Streamer with the given sink, with a very long statusInterval
// so the periodic-status branch never fires during tests.
func newStreamer(sw sinkWriter) *Streamer {
	return &Streamer{sink: sw, statusInterval: 24 * time.Hour}
}

// runLoop runs receiveLoop and expects it to return errDone (scripted messages
// exhausted). Returns the sender and error for inspection.
func runLoop(r *Streamer, msgs []pgproto3.BackendMessage) (*recordingSender, error) {
	conn := &fakeConn{msgs: msgs}
	sender := &recordingSender{}
	err := r.receiveLoop(context.Background(), conn, sender.Send)
	return sender, err
}

// ---------------------------------------------------------------------------
// Test cases
// ---------------------------------------------------------------------------

// insertPayload is a minimal wal2json v2 INSERT payload for table orders.
var insertPayload = []byte(`{"action":"I","schema":"public","table":"orders","columns":[{"name":"id","value":1}]}`)

// TestXLogData_SinkAndAck: one INSERT payload → sink gets change, LSN stamped, status sent.
func TestXLogData_SinkAndAck(t *testing.T) {
	sk := &recordingSink{}
	r := newStreamer(sk)

	walStart := pglogrepl.LSN(0x1000)
	frame := xlogData(walStart, insertPayload)
	sender, err := runLoop(r, []pgproto3.BackendMessage{frame})

	if !errors.Is(err, errDone) {
		t.Fatalf("expected errDone, got %v", err)
	}
	if len(sk.changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(sk.changes))
	}
	endLSN := walStart + pglogrepl.LSN(len(insertPayload))
	if sk.changes[0].LSN != endLSN.String() {
		t.Errorf("LSN = %q, want %q", sk.changes[0].LSN, endLSN.String())
	}
	if len(sender.lsns) != 1 || sender.lsns[0] != endLSN {
		t.Errorf("acked LSNs = %v, want [%v]", sender.lsns, endLSN)
	}
}

// TestXLogData_MultipleChanges: (only one change per wal2json message, but we
// verify LSN stamping for completeness — decode.Parse returns multiple changes
// for a future scenario; here we verify single is correct).
func TestXLogData_LSNStamping(t *testing.T) {
	sk := &recordingSink{}
	r := newStreamer(sk)

	walStart := pglogrepl.LSN(0x2000)
	frame := xlogData(walStart, insertPayload)
	sender, err := runLoop(r, []pgproto3.BackendMessage{frame})
	if !errors.Is(err, errDone) {
		t.Fatalf("expected errDone, got %v", err)
	}
	endLSN := walStart + pglogrepl.LSN(len(insertPayload))
	for _, c := range sk.changes {
		if c.LSN != endLSN.String() {
			t.Errorf("change LSN = %q, want %q", c.LSN, endLSN.String())
		}
	}
	// Exactly one ack at endLSN
	if len(sender.lsns) != 1 || sender.lsns[0] != endLSN {
		t.Errorf("acked LSNs = %v, want [%v]", sender.lsns, endLSN)
	}
}

// TestAckDedup: second XLogData with endLSN <= confirmedFlushLSN does NOT trigger a new status send.
func TestAckDedup(t *testing.T) {
	sk := &recordingSink{}
	r := newStreamer(sk)

	walStart := pglogrepl.LSN(0x3000)
	frame1 := xlogData(walStart, insertPayload)
	// Second frame has a smaller walStart so endLSN < confirmedFlushLSN
	frame2 := xlogData(pglogrepl.LSN(0x0100), insertPayload)

	sender, err := runLoop(r, []pgproto3.BackendMessage{frame1, frame2})
	if !errors.Is(err, errDone) {
		t.Fatalf("expected errDone, got %v", err)
	}
	// Only one ack should have been sent (for frame1)
	if len(sender.lsns) != 1 {
		t.Errorf("acked LSNs = %v, want exactly 1 (dedup should suppress second)", sender.lsns)
	}
}

// TestKeepaliveReplyRequested: sends status with current confirmedFlushLSN.
func TestKeepaliveReplyRequested(t *testing.T) {
	sk := &recordingSink{}
	r := newStreamer(sk)

	// First advance confirmedFlushLSN with an XLogData
	walStart := pglogrepl.LSN(0x4000)
	frame1 := xlogData(walStart, insertPayload)
	ka := keepalive(true)

	sender, err := runLoop(r, []pgproto3.BackendMessage{frame1, ka})
	if !errors.Is(err, errDone) {
		t.Fatalf("expected errDone, got %v", err)
	}
	endLSN := walStart + pglogrepl.LSN(len(insertPayload))
	// Two statuses: one for the XLogData ack, one for the keepalive reply
	if len(sender.lsns) != 2 {
		t.Fatalf("expected 2 status sends, got %d: %v", len(sender.lsns), sender.lsns)
	}
	if sender.lsns[1] != endLSN {
		t.Errorf("keepalive ack LSN = %v, want %v", sender.lsns[1], endLSN)
	}
}

// TestKeepaliveNoReply: no status sent.
func TestKeepaliveNoReply(t *testing.T) {
	sk := &recordingSink{}
	r := newStreamer(sk)

	ka := keepalive(false)
	sender, err := runLoop(r, []pgproto3.BackendMessage{ka})
	if !errors.Is(err, errDone) {
		t.Fatalf("expected errDone, got %v", err)
	}
	if len(sender.lsns) != 0 {
		t.Errorf("expected no status sends, got %v", sender.lsns)
	}
}

// TestNonCopyDataSkipped: non-CopyData messages are skipped.
func TestNonCopyDataSkipped(t *testing.T) {
	sk := &recordingSink{}
	r := newStreamer(sk)

	// DataRow is not a CopyData message
	msgs := []pgproto3.BackendMessage{
		&pgproto3.DataRow{},
		&pgproto3.DataRow{},
	}
	sender, err := runLoop(r, msgs)
	if !errors.Is(err, errDone) {
		t.Fatalf("expected errDone, got %v", err)
	}
	if len(sk.changes) != 0 || len(sender.lsns) != 0 {
		t.Error("expected no changes or status sends for non-CopyData messages")
	}
}

// TestEmptyCopyDataSkipped: zero-length CopyData skipped.
func TestEmptyCopyDataSkipped(t *testing.T) {
	sk := &recordingSink{}
	r := newStreamer(sk)

	msgs := []pgproto3.BackendMessage{
		&pgproto3.CopyData{Data: []byte{}},
	}
	sender, err := runLoop(r, msgs)
	if !errors.Is(err, errDone) {
		t.Fatalf("expected errDone, got %v", err)
	}
	if len(sk.changes) != 0 || len(sender.lsns) != 0 {
		t.Error("expected no changes or status sends for empty CopyData")
	}
}

// TestDecodeError: malformed WALData → receiveLoop returns error containing LSN.
func TestDecodeError(t *testing.T) {
	sk := &recordingSink{}
	r := newStreamer(sk)

	walStart := pglogrepl.LSN(0x5000)
	frame := xlogData(walStart, []byte("not valid json"))
	_, err := runLoop(r, []pgproto3.BackendMessage{frame})
	if err == nil {
		t.Fatal("expected error from malformed payload, got nil")
	}
	if !strings.Contains(err.Error(), walStart.String()) {
		t.Errorf("error %q does not contain LSN %s", err.Error(), walStart.String())
	}
}

// TestSinkWriteError: sink.Write error propagated.
func TestSinkWriteError(t *testing.T) {
	sinkErr := fmt.Errorf("redis unavailable")
	sk := &recordingSink{err: sinkErr}
	r := newStreamer(sk)

	walStart := pglogrepl.LSN(0x6000)
	frame := xlogData(walStart, insertPayload)
	_, err := runLoop(r, []pgproto3.BackendMessage{frame})
	if err == nil {
		t.Fatal("expected error from sink, got nil")
	}
	if !errors.Is(err, sinkErr) {
		t.Errorf("error chain does not wrap sinkErr: %v", err)
	}
}
