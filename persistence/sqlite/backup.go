// Backup and restore for a SQLite Store.
//
// A backup is a single portable artifact (not a raw database-file copy):
//
//	header  magic(8) | format version u32 | created_at i64 | room_count u64
//	records u16 type | u64 length | length bytes ...          (room payload)
//	end     u16 END  | u64 40 | sha256(records) | u64(record bytes)
//	footer  magic(8)
//
// All integers are big-endian. The END record carries a SHA-256 over every
// record-frame byte (frame header included, END and footer excluded), so the
// whole payload is authenticated, not just per-record. A reader rejects bad
// magic, an unsupported format version, any structural inconsistency, a
// checksum mismatch, a truncated tail, or trailing bytes.
//
// The backup reads through ONE read-only transaction. In WAL mode that
// transaction pins one committed snapshot: all rooms, their VISIBLE update
// history (rows a concurrent mid-prune has not yet deleted are filtered by the
// same checkpoint clamp the live read path uses), legacy name-keyed snapshots,
// enumerable (id-keyed) snapshots, and the in-progress-prune checkpoint rows
// that constrain the visible head are all observed at one instant. A
// concurrent writer lands entirely before or entirely after that instant; the
// artifact can never mix two table states.
//
// Restore validates the ENTIRE artifact before creating anything, then builds a
// fresh database in a temp file inside the target directory (one transaction
// for every row, followed by PRAGMA integrity_check) and swaps it into place
// with a single rename, so a store is never partially visible. Every failure
// path removes the temp file and leaves the original database untouched.
package sqlite

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/reearth/ygo/internal/updatecheck"
)

const (
	backupMagic         = "YGOBKUP!" // 8 bytes; also used as the trailing footer
	backupFormatVersion = uint32(1)
	backupHeaderLen     = 28 // magic(8) + version(4) + created_at(8) + room_count(8)
	backupFooterLen     = 8
	backupFrameHdrLen   = 10 // type u16 + length u64
	backupEndPayloadLen = 40 // sha256(32) + payload length u64(8)

	// maxBackupRecordLen bounds a single framed record when parsing. Its only
	// job is to turn a corrupt length field (e.g. up to 2^64) into a quick
	// error rather than a giant allocation attempt. 1 TiB is far above any
	// real snapshot blob.
	maxBackupRecordLen = uint64(1) << 40
)

// Record types.
const (
	recRoom              uint16 = 1
	recUpdate            uint16 = 2
	recNamedSnapshot     uint16 = 3
	recVersionedSnapshot uint16 = 4
	recRoomEnd           uint16 = 5
	recEnd               uint16 = 0xffff
)

// Sentinel errors so callers (and the CLI) can distinguish failure classes:
// a corrupt/incompatible artifact and an occupied target must both be reported
// honestly, and neither must touch the target.
var (
	// ErrNotABackup is returned when the input does not begin with a backup
	// magic header (truncated file, a raw SQLite database, random bytes).
	ErrNotABackup = errors.New("sqlite: not a ygo backup artifact")
	// ErrUnsupportedBackupVersion is returned when the artifact's format
	// version is newer (or zero/garbage) and this build cannot read it.
	ErrUnsupportedBackupVersion = errors.New("sqlite: unsupported backup format version")
	// ErrBackupChecksum is returned when the payload SHA-256 does not match
	// the digest in the END record (tampering, bit rot, truncation).
	ErrBackupChecksum = errors.New("sqlite: backup checksum mismatch")
	// ErrBackupCorrupt is returned for any structural inconsistency that is
	// not a checksum failure (bad framing, wrong counts or ordering, trailing
	// bytes, failed integrity check on restore).
	ErrBackupCorrupt = errors.New("sqlite: corrupt backup artifact")
	// ErrBackupTargetExists is returned by Restore when the destination
	// database (or one of its -wal/-shm sidecars) already exists and force is
	// false.
	ErrBackupTargetExists = errors.New("sqlite: restore target already exists")
	// ErrBackupFileExists is returned by BackupFile when the output path
	// already exists and force is false, so an existing backup is never
	// silently replaced by a failed run.
	ErrBackupFileExists = errors.New("sqlite: backup output already exists")
	// ErrInMemoryNotBackupable is returned when backup is asked for an
	// ephemeral in-memory store: there is no durable file to take a consistent
	// snapshot of, and the command's purpose is durable portability.
	ErrInMemoryNotBackupable = errors.New("sqlite: cannot back up an ephemeral in-memory store")
)

// BackupInfo describes a produced (or validated) backup artifact.
type BackupInfo struct {
	// FormatVersion is the artifact format (currently 1).
	FormatVersion uint32
	// CreatedAt is the producer's timestamp captured in the header.
	CreatedAt time.Time
	// Rooms is the number of rooms in the artifact.
	Rooms int
}

// bkRoom is the JSON payload of a recRoom record. Counts tell the reader
// exactly how many of each following record belongs to the room, so a
// truncated or spliced stream cannot be mistaken for a complete room.
type bkRoom struct {
	Room       string        `json:"room"`
	Updates    int           `json:"updates"`
	Named      int           `json:"named"`
	Versioned  int           `json:"versioned"`
	Checkpoint *bkCheckpoint `json:"checkpoint,omitempty"`
}

// bkCheckpoint is the in-progress-prune constraint on a room's visible head:
// the clamp ceiling and the forensic rolled-back head blob (nullable, as in
// the checkpoints table).
type bkCheckpoint struct {
	Target     int64  `json:"target"`
	RolledBack []byte `json:"rolled_back,omitempty"`
}

type bkUpdate struct {
	V  int64 `json:"v"`
	TS int64 `json:"ts"`
}

type bkNamed struct {
	Name string `json:"name"`
	V    int64  `json:"v"`
}

type bkVSnap struct {
	ID    int64  `json:"id"`
	Label string `json:"label"`
	TS    int64  `json:"ts"`
}

// Backup writes a portable, checksummed backup of every room in s to w.
//
// All reads happen in one read-only transaction (a single WAL snapshot), so
// the artifact reflects exactly one committed point even while other
// connections append updates or prune. The destination is typically a file;
// callers needing atomic placement should use BackupFile.
func Backup(ctx context.Context, s *Store, w io.Writer) (BackupInfo, error) {
	if err := ctx.Err(); err != nil {
		return BackupInfo{}, err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return BackupInfo{}, err
	}
	defer func() { _ = tx.Rollback() }() // read-only; rollback only releases the snapshot

	rooms, err := backupRooms(ctx, tx)
	if err != nil {
		return BackupInfo{}, err
	}

	bw := bufio.NewWriter(w)
	created := time.Now().UTC()
	if err := writeBackupHeader(bw, created, len(rooms)); err != nil {
		return BackupInfo{}, err
	}

	// dw hashes every room-record frame, including RoomEnd. Only the END
	// record and the footer are written to bw directly, so the digest covers
	// exactly the room payload.
	dw := &digestWriter{w: bw, h: sha256.New()}
	for _, room := range rooms {
		if err := ctx.Err(); err != nil {
			return BackupInfo{}, err
		}
		cp, err := backupCheckpoint(ctx, tx, room)
		if err != nil {
			return BackupInfo{}, err
		}
		nUpdates, err := backupCount(ctx, tx,
			`SELECT COUNT(*) FROM updates WHERE room = ? AND version <= `+clampSQL, room, room)
		if err != nil {
			return BackupInfo{}, err
		}
		nNamed, err := backupCount(ctx, tx,
			`SELECT COUNT(*) FROM snapshots WHERE room = ?`, room)
		if err != nil {
			return BackupInfo{}, err
		}
		nVersioned, err := backupCount(ctx, tx,
			`SELECT COUNT(*) FROM snapshot_versions WHERE room = ?`, room)
		if err != nil {
			return BackupInfo{}, err
		}

		hdr := bkRoom{Room: room, Updates: nUpdates, Named: nNamed, Versioned: nVersioned, Checkpoint: cp}
		if err := writeJSONFrame(dw, recRoom, hdr); err != nil {
			return BackupInfo{}, err
		}

		urows, err := tx.QueryContext(ctx,
			`SELECT version, created_at, data FROM updates
			 WHERE room = ? AND version <= `+clampSQL+` ORDER BY version ASC`,
			room, room)
		if err != nil {
			return BackupInfo{}, err
		}
		err = func() error {
			defer func() { _ = urows.Close() }()
			for urows.Next() {
				var version, ts int64
				var data []byte
				if err := urows.Scan(&version, &ts, &data); err != nil {
					return err
				}
				if err := writeBlobFrame(dw, recUpdate, bkUpdate{V: version, TS: ts}, data); err != nil {
					return err
				}
			}
			return urows.Err()
		}()
		if err != nil {
			return BackupInfo{}, err
		}

		nrows, err := tx.QueryContext(ctx,
			`SELECT name, version, state FROM snapshots WHERE room = ? ORDER BY name ASC`, room)
		if err != nil {
			return BackupInfo{}, err
		}
		err = func() error {
			defer func() { _ = nrows.Close() }()
			for nrows.Next() {
				var name string
				var version int64
				var state []byte
				if err := nrows.Scan(&name, &version, &state); err != nil {
					return err
				}
				if err := writeBlobFrame(dw, recNamedSnapshot, bkNamed{Name: name, V: version}, state); err != nil {
					return err
				}
			}
			return nrows.Err()
		}()
		if err != nil {
			return BackupInfo{}, err
		}

		vrows, err := tx.QueryContext(ctx,
			`SELECT id, label, created_at, state FROM snapshot_versions WHERE room = ? ORDER BY id ASC`, room)
		if err != nil {
			return BackupInfo{}, err
		}
		err = func() error {
			defer func() { _ = vrows.Close() }()
			for vrows.Next() {
				var id, ts int64
				var label string
				var state []byte
				if err := vrows.Scan(&id, &label, &ts, &state); err != nil {
					return err
				}
				if err := writeBlobFrame(dw, recVersionedSnapshot, bkVSnap{ID: id, Label: label, TS: ts}, state); err != nil {
					return err
				}
			}
			return vrows.Err()
		}()
		if err != nil {
			return BackupInfo{}, err
		}

		// RoomEnd is part of the payload and covered by the digest like every
		// other room frame; only END and the footer bypass it.
		if err := writeFrame(dw, recRoomEnd, nil); err != nil {
			return BackupInfo{}, err
		}
	}

	// END record: digest over room-record frames plus the exact byte count,
	// then the trailing magic. Neither is part of the hashed domain.
	var end [backupEndPayloadLen]byte
	copy(end[:32], dw.h.Sum(nil))
	binary.BigEndian.PutUint64(end[32:], dw.n)
	if err := writeFrame(bw, recEnd, end[:]); err != nil {
		return BackupInfo{}, err
	}
	if _, err := bw.Write([]byte(backupMagic)); err != nil {
		return BackupInfo{}, err
	}
	if err := bw.Flush(); err != nil {
		return BackupInfo{}, err
	}
	return BackupInfo{
		FormatVersion: backupFormatVersion,
		CreatedAt:     created,
		Rooms:         len(rooms),
	}, nil
}

// digestWriter accumulates the SHA-256 and byte count of everything written
// through it.
type digestWriter struct {
	w io.Writer
	h interface {
		io.Writer
		Sum(b []byte) []byte
	}
	n uint64
}

func (d *digestWriter) Write(p []byte) (int, error) {
	n, err := d.w.Write(p)
	// Hash whatever the underlying writer accepted: Backup fails hard on a
	// short write anyway (err non-nil for our file writers), and hashing the
	// accepted prefix keeps n/h consistent if a custom writer accepted bytes.
	if n > 0 {
		d.h.Write(p[:n])
		d.n += uint64(n)
	}
	return n, err
}

func backupRooms(ctx context.Context, q querier) ([]string, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT room FROM (
			SELECT room FROM updates
			UNION SELECT room FROM snapshots
			UNION SELECT room FROM snapshot_versions
			UNION SELECT room FROM checkpoints
		) ORDER BY room`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var rooms []string
	for rows.Next() {
		var room string
		if err := rows.Scan(&room); err != nil {
			return nil, err
		}
		rooms = append(rooms, room)
	}
	return rooms, rows.Err()
}

func backupCheckpoint(ctx context.Context, q querier, room string) (*bkCheckpoint, error) {
	var target int64
	var rolled []byte
	err := q.QueryRowContext(ctx,
		`SELECT target, rolled_back_head FROM checkpoints WHERE room = ?`, room).Scan(&target, &rolled)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &bkCheckpoint{Target: target, RolledBack: rolled}, nil
}

func backupCount(ctx context.Context, q querier, query string, args ...any) (int, error) {
	var n int
	if err := q.QueryRowContext(ctx, query, args...).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

// ---- wire format: writing -------------------------------------------------

func writeBackupHeader(w io.Writer, created time.Time, rooms int) error {
	var hdr [backupHeaderLen]byte
	copy(hdr[0:8], backupMagic)
	binary.BigEndian.PutUint32(hdr[8:12], backupFormatVersion)
	binary.BigEndian.PutUint64(hdr[12:20], uint64(created.UnixNano()))
	binary.BigEndian.PutUint64(hdr[20:28], uint64(rooms))
	_, err := w.Write(hdr[:])
	return err
}

// writeFrame writes one type|length|body frame to w. It is frame-exact:
// callers must already have produced the canonical body bytes.
func writeFrame(w io.Writer, typ uint16, body []byte) error {
	var hdr [backupFrameHdrLen]byte
	binary.BigEndian.PutUint16(hdr[0:2], typ)
	binary.BigEndian.PutUint64(hdr[2:10], uint64(len(body)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(body) > 0 {
		if _, err := w.Write(body); err != nil {
			return err
		}
	}
	return nil
}

func writeJSONFrame(w io.Writer, typ uint16, v any) error {
	body, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return writeFrame(w, typ, body)
}

// writeBlobFrame encodes a JSON header followed by a raw (unencoded) blob:
// u32 jsonLen | json | u64 blobLen | blob. Keeping the binary payload outside
// JSON avoids base64 inflation on every update/snapshot.
func writeBlobFrame(w io.Writer, typ uint16, meta any, blob []byte) error {
	jb, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if uint64(len(jb)) > uint64(^uint32(0)) {
		return fmt.Errorf("%w: metadata too large", ErrBackupCorrupt)
	}
	body := make([]byte, 0, 4+len(jb)+8+len(blob))
	var l4 [4]byte
	binary.BigEndian.PutUint32(l4[:], uint32(len(jb)))
	body = append(body, l4[:]...)
	body = append(body, jb...)
	var l8 [8]byte
	binary.BigEndian.PutUint64(l8[:], uint64(len(blob)))
	body = append(body, l8[:]...)
	body = append(body, blob...)
	return writeFrame(w, typ, body)
}

// ---- wire format: reading -------------------------------------------------

type backupHeader struct {
	version   uint32
	createdAt int64
	rooms     uint64
}

func (h backupHeader) info() BackupInfo {
	return BackupInfo{
		FormatVersion: h.version,
		CreatedAt:     time.Unix(0, h.createdAt).UTC(),
		Rooms:         int(h.rooms),
	}
}

// readBackupHeader parses and validates the fixed header. Magic errors are
// ErrNotABackup (that includes an empty/truncated file); a bad version is
// ErrUnsupportedBackupVersion.
func readBackupHeader(r io.Reader) (backupHeader, error) {
	var buf [backupHeaderLen]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return backupHeader{}, ErrNotABackup
		}
		return backupHeader{}, err
	}
	if string(buf[0:8]) != backupMagic {
		return backupHeader{}, ErrNotABackup
	}
	h := backupHeader{
		version:   binary.BigEndian.Uint32(buf[8:12]),
		createdAt: int64(binary.BigEndian.Uint64(buf[12:20])),
		rooms:     binary.BigEndian.Uint64(buf[20:28]),
	}
	if h.version != backupFormatVersion {
		return backupHeader{}, fmt.Errorf("%w: got version %d", ErrUnsupportedBackupVersion, h.version)
	}
	return h, nil
}

// backupVisitor receives every parsed, structurally-valid record. The scanner
// enforces framing, counts, ordering, room uniqueness and the END digest; the
// visitor owns payload-level checks/side effects (V1 validation, inserts).
type backupVisitor interface {
	visitRoom(h bkRoom) error
	visitUpdate(room string, m bkUpdate, blob []byte) error
	visitNamed(room string, m bkNamed, blob []byte) error
	visitVersioned(room string, m bkVSnap, blob []byte) error
	visitRoomEnd(room string) error
}

// scanBackup parses the record stream (r starts immediately after the header),
// verifies structural invariants and the END digest/footer, and dispatches
// each payload to v. roomCount is the count asserted in the header.
func scanBackup(ctx context.Context, r io.Reader, roomCount uint64, v backupVisitor) error {
	h := sha256.New()
	var payloadLen uint64

	// Parser state. Rooms are strictly ascending. Within a room: updates
	// (strictly ascending version), named snapshots (ascending unique name),
	// versioned snapshots (ascending unique id), RoomEnd.
	const (
		phaseStart     = 0 // expect Room
		phaseUpdates   = 1
		phaseNamed     = 2
		phaseVersioned = 3
		phaseRoomEnd   = 4
	)
	phase := phaseStart
	var (
		curRoom                        string
		lastRoom                       string
		haveRoom                       bool
		updatesLeft, namedLeft, vsLeft int
		lastVersion, lastVSnap         int64
		haveVersion, haveVSnap         bool
		lastName                       string
		haveName                       bool
		cpTarget                       int64
		haveCP                         bool
		gotRooms                       uint64
	)
	var frame [backupFrameHdrLen]byte
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := io.ReadFull(r, frame[:]); err != nil {
			return fmt.Errorf("%w: truncated frame: %v", ErrBackupCorrupt, err)
		}
		typ := binary.BigEndian.Uint16(frame[0:2])
		ln := binary.BigEndian.Uint64(frame[2:10])

		if typ == recEnd {
			if ln != backupEndPayloadLen {
				return fmt.Errorf("%w: END record length %d", ErrBackupCorrupt, ln)
			}
			var end [backupEndPayloadLen]byte
			if _, err := io.ReadFull(r, end[:]); err != nil {
				return fmt.Errorf("%w: truncated END: %v", ErrBackupCorrupt, err)
			}
			want := end[:32]
			gotPL := binary.BigEndian.Uint64(end[32:40])
			if gotPL != payloadLen {
				return fmt.Errorf("%w: payload length %d does not match END %d", ErrBackupCorrupt, payloadLen, gotPL)
			}
			if subtle.ConstantTimeCompare(h.Sum(nil), want) != 1 {
				return ErrBackupChecksum
			}
			if gotRooms != roomCount {
				return fmt.Errorf("%w: header declares %d rooms, stream has %d", ErrBackupCorrupt, roomCount, gotRooms)
			}
			if phase != phaseStart && phase != phaseRoomEnd {
				return fmt.Errorf("%w: END inside a room record block", ErrBackupCorrupt)
			}
			return verifyFooterAndEOF(r)
		}

		if ln > maxBackupRecordLen {
			return fmt.Errorf("%w: record %d length %d too large", ErrBackupCorrupt, typ, ln)
		}
		body := make([]byte, ln)
		if _, err := io.ReadFull(r, body); err != nil {
			return fmt.Errorf("%w: truncated record body: %v", ErrBackupCorrupt, err)
		}
		// Room frames are hashed exactly as they appeared on the wire.
		h.Write(frame[:])
		h.Write(body)
		payloadLen += uint64(backupFrameHdrLen) + ln

		switch typ {
		case recRoom:
			if phase != phaseStart && phase != phaseRoomEnd {
				return fmt.Errorf("%w: nested room record", ErrBackupCorrupt)
			}
			var rh bkRoom
			if err := json.Unmarshal(body, &rh); err != nil {
				return fmt.Errorf("%w: room header json: %v", ErrBackupCorrupt, err)
			}
			if rh.Updates < 0 || rh.Named < 0 || rh.Versioned < 0 {
				return fmt.Errorf("%w: negative record count in room %q", ErrBackupCorrupt, rh.Room)
			}
			if haveRoom && rh.Room <= lastRoom {
				return fmt.Errorf("%w: rooms out of order or duplicated: %q after %q", ErrBackupCorrupt, rh.Room, lastRoom)
			}
			if err := v.visitRoom(rh); err != nil {
				return err
			}
			curRoom, lastRoom, haveRoom = rh.Room, rh.Room, true
			updatesLeft, namedLeft, vsLeft = rh.Updates, rh.Named, rh.Versioned
			lastVersion, haveVersion = 0, false
			lastName, haveName = "", false
			lastVSnap, haveVSnap = 0, false
			if rh.Checkpoint != nil {
				if rh.Checkpoint.Target < 0 {
					return fmt.Errorf("%w: negative checkpoint target in room %q", ErrBackupCorrupt, rh.Room)
				}
				cpTarget, haveCP = rh.Checkpoint.Target, true
			} else {
				cpTarget, haveCP = 0, false
			}
			phase = phaseUpdates
			gotRooms++

		case recUpdate:
			if phase != phaseUpdates || updatesLeft == 0 {
				return fmt.Errorf("%w: update record outside its block", ErrBackupCorrupt)
			}
			m, blob, err := parseBlobRecord[bkUpdate](body)
			if err != nil {
				return err
			}
			if m.V < 1 {
				return fmt.Errorf("%w: update version %d < 1", ErrBackupCorrupt, m.V)
			}
			if haveVersion && m.V <= lastVersion {
				return fmt.Errorf("%w: update versions not strictly ascending (%d after %d)", ErrBackupCorrupt, m.V, lastVersion)
			}
			if haveCP && m.V > cpTarget {
				return fmt.Errorf("%w: update v%d above checkpoint ceiling %d", ErrBackupCorrupt, m.V, cpTarget)
			}
			if err := v.visitUpdate(curRoom, m, blob); err != nil {
				return err
			}
			lastVersion, haveVersion = m.V, true
			updatesLeft--

		case recNamedSnapshot:
			if phase == phaseUpdates && updatesLeft == 0 {
				phase = phaseNamed
			}
			if phase != phaseNamed || namedLeft == 0 {
				return fmt.Errorf("%w: named snapshot record outside its block", ErrBackupCorrupt)
			}
			m, blob, err := parseBlobRecord[bkNamed](body)
			if err != nil {
				return err
			}
			if m.V < 0 {
				return fmt.Errorf("%w: named snapshot %q has negative version", ErrBackupCorrupt, m.Name)
			}
			if haveName && m.Name <= lastName {
				return fmt.Errorf("%w: named snapshot names not strictly ascending/unique (%q after %q)", ErrBackupCorrupt, m.Name, lastName)
			}
			if err := v.visitNamed(curRoom, m, blob); err != nil {
				return err
			}
			lastName, haveName = m.Name, true
			namedLeft--

		case recVersionedSnapshot:
			if (phase == phaseNamed && namedLeft == 0) || (phase == phaseUpdates && updatesLeft == 0) {
				phase = phaseVersioned
			}
			if phase != phaseVersioned || vsLeft == 0 {
				return fmt.Errorf("%w: versioned snapshot record outside its block", ErrBackupCorrupt)
			}
			m, blob, err := parseBlobRecord[bkVSnap](body)
			if err != nil {
				return err
			}
			if m.ID < 1 {
				return fmt.Errorf("%w: versioned snapshot id %d < 1", ErrBackupCorrupt, m.ID)
			}
			if haveVSnap && m.ID <= lastVSnap {
				return fmt.Errorf("%w: snapshot ids not strictly ascending (%d after %d)", ErrBackupCorrupt, m.ID, lastVSnap)
			}
			if len(blob) == 0 {
				return fmt.Errorf("%w: versioned snapshot %d has empty state", ErrBackupCorrupt, m.ID)
			}
			if err := v.visitVersioned(curRoom, m, blob); err != nil {
				return err
			}
			lastVSnap, haveVSnap = m.ID, true
			vsLeft--

		case recRoomEnd:
			if len(body) != 0 {
				return fmt.Errorf("%w: RoomEnd has a body", ErrBackupCorrupt)
			}
			if updatesLeft != 0 || namedLeft != 0 || vsLeft != 0 {
				return fmt.Errorf("%w: room %q ended with %d/%d/%d unread records",
					ErrBackupCorrupt, curRoom, updatesLeft, namedLeft, vsLeft)
			}
			if err := v.visitRoomEnd(curRoom); err != nil {
				return err
			}
			phase = phaseRoomEnd

		default:
			return fmt.Errorf("%w: unknown record type %d", ErrBackupCorrupt, typ)
		}
	}
}

// parseBlobRecord splits a blob record body into its JSON metadata and blob.
func parseBlobRecord[T any](body []byte) (T, []byte, error) {
	var zero T
	if len(body) < 4 {
		return zero, nil, fmt.Errorf("%w: blob record too short (%d bytes)", ErrBackupCorrupt, len(body))
	}
	jl := int(binary.BigEndian.Uint32(body[0:4]))
	if jl < 0 || 4+jl+8 > len(body) {
		return zero, nil, fmt.Errorf("%w: blob record json length %d out of bounds", ErrBackupCorrupt, jl)
	}
	var m T
	if err := json.Unmarshal(body[4:4+jl], &m); err != nil {
		return zero, nil, fmt.Errorf("%w: blob record json: %v", ErrBackupCorrupt, err)
	}
	bl := binary.BigEndian.Uint64(body[4+jl : 12+jl])
	start := 12 + jl
	if uint64(len(body)-start) != bl {
		return zero, nil, fmt.Errorf("%w: blob length %d does not match frame (%d remaining)", ErrBackupCorrupt, bl, len(body)-start)
	}
	return m, body[start:], nil
}

func verifyFooterAndEOF(r io.Reader) error {
	var foot [backupFooterLen]byte
	if _, err := io.ReadFull(r, foot[:]); err != nil {
		return fmt.Errorf("%w: truncated footer: %v", ErrBackupCorrupt, err)
	}
	if string(foot[:]) != backupMagic {
		return fmt.Errorf("%w: bad footer magic", ErrBackupCorrupt)
	}
	// No trailing bytes: a correct artifact ends exactly at the footer.
	var one [1]byte
	n, err := r.Read(one[:])
	if n != 0 {
		return fmt.Errorf("%w: trailing bytes after footer", ErrBackupCorrupt)
	}
	if err == nil {
		// n==0, err==nil is legal for some readers; confirm with a second read.
		n, err = r.Read(one[:])
		if n != 0 || err == nil {
			return fmt.Errorf("%w: trailing bytes after footer", ErrBackupCorrupt)
		}
	}
	if !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: after footer: %v", ErrBackupCorrupt, err)
	}
	return nil
}

// validatingVisitor is the pass-1 visitor: hash and structure are already
// checked by the scanner; this adds payload-level validation (every visible
// update must decode as lib0 V1, as it did when first stored).
type validatingVisitor struct{}

func (validatingVisitor) visitRoom(bkRoom) error                       { return nil }
func (validatingVisitor) visitRoomEnd(string) error                    { return nil }
func (validatingVisitor) visitNamed(string, bkNamed, []byte) error     { return nil }
func (validatingVisitor) visitVersioned(string, bkVSnap, []byte) error { return nil }

func (validatingVisitor) visitUpdate(_ string, _ bkUpdate, blob []byte) error {
	if len(blob) == 0 {
		return fmt.Errorf("%w: update with empty data", ErrBackupCorrupt)
	}
	if err := updatecheck.ValidateV1(blob); err != nil {
		return fmt.Errorf("%w: update is not a valid V1 blob: %v", ErrBackupCorrupt, err)
	}
	return nil
}

// ---- restore --------------------------------------------------------------

// Restore fully validates the artifact read from r and, only if validation
// succeeds, installs a brand-new database at dstPath via a temp file and a
// single atomic rename.
//
// If dstPath (or a -wal/-shm sidecar of it) already exists, Restore fails
// with ErrBackupTargetExists unless force is true. No failure path — bad
// magic, incompatible version, checksum mismatch, context cancellation, disk
// full, or commit/integrity failure — modifies or removes the existing
// database: all construction happens in a temp file that is deleted on error.
func Restore(ctx context.Context, r io.ReadSeeker, dstPath string, force bool) (BackupInfo, error) {
	if err := ctx.Err(); err != nil {
		return BackupInfo{}, err
	}
	if dstPath == "" || dstPath == ":memory:" {
		return BackupInfo{}, fmt.Errorf("sqlite: restore requires a durable database path, got %q", dstPath)
	}

	// Pass 1: validate the ENTIRE artifact before creating anything.
	hdr, err := validateArtifact(ctx, r)
	if err != nil {
		return BackupInfo{}, err
	}

	// Preflight the destination only after the artifact has proven valid.
	occupied, err := targetOccupied(dstPath)
	if err != nil {
		return BackupInfo{}, err
	}
	if occupied && !force {
		return BackupInfo{}, fmt.Errorf("%w: %s (use force to replace)", ErrBackupTargetExists, dstPath)
	}
	dir := filepath.Dir(dstPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return BackupInfo{}, err
	}

	if err := installBackup(ctx, r, dstPath, hdr); err != nil {
		return BackupInfo{}, err
	}
	return hdr.info(), nil
}

// validateArtifact rewinds r and runs the full structural+checksum parse
// without writing anything.
func validateArtifact(ctx context.Context, r io.ReadSeeker) (backupHeader, error) {
	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return backupHeader{}, err
	}
	hdr, err := readBackupHeader(r)
	if err != nil {
		return backupHeader{}, err
	}
	if err := scanBackup(ctx, r, hdr.rooms, validatingVisitor{}); err != nil {
		return backupHeader{}, err
	}
	return hdr, nil
}

// inserter is the pass-2 visitor: every validated row goes into one
// transaction on the fresh temp database.
type inserter struct {
	tx *sql.Tx
}

func (in *inserter) visitRoom(h bkRoom) error {
	if h.Checkpoint == nil {
		return nil
	}
	_, err := in.tx.ExecContext(context.Background(),
		`INSERT INTO checkpoints (room, target, rolled_back_head) VALUES (?, ?, ?)`,
		h.Room, h.Checkpoint.Target, h.Checkpoint.RolledBack)
	return err
}

func (in *inserter) visitUpdate(room string, m bkUpdate, blob []byte) error {
	_, err := in.tx.ExecContext(context.Background(),
		`INSERT INTO updates (room, version, data, created_at) VALUES (?, ?, ?, ?)`,
		room, m.V, blob, m.TS)
	return err
}

func (in *inserter) visitNamed(room string, m bkNamed, blob []byte) error {
	_, err := in.tx.ExecContext(context.Background(),
		`INSERT INTO snapshots (room, name, version, state) VALUES (?, ?, ?, ?)`,
		room, m.Name, m.V, blob)
	return err
}

func (in *inserter) visitVersioned(room string, m bkVSnap, blob []byte) error {
	_, err := in.tx.ExecContext(context.Background(),
		`INSERT INTO snapshot_versions (id, room, label, created_at, state) VALUES (?, ?, ?, ?, ?)`,
		m.ID, room, m.Label, m.TS, blob)
	return err
}

func (in *inserter) visitRoomEnd(string) error { return nil }

// installBackup builds the new database at a temp path in one transaction,
// verifies it, and only then renames it over dstPath. Everything before the
// rename is invisible at dstPath and removed on any error.
func installBackup(ctx context.Context, r io.ReadSeeker, dstPath string, hdr backupHeader) (err error) {
	dir := filepath.Dir(dstPath)

	tmp, err := createTempDBPath(dir)
	if err != nil {
		return err
	}
	// Cleanup runs on every non-success path: close the store (also clears
	// its connection pool), then delete the temp DB and any sidecars.
	committed := false
	defer func() {
		if committed {
			return
		}
		removeDBFiles(tmp)
	}()

	db, err := Open(tmp)
	if err != nil {
		removeDBFiles(tmp)
		return err
	}

	closeAndFail := func(e error) error {
		_ = db.Close()
		removeDBFiles(tmp)
		return e
	}

	// Pass 2: re-verify digest/structure while inserting, all in one
	// transaction. Cancellation or a write/commit failure rolls the whole
	// build back, so the temp database can never contain a partial artifact.
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return closeAndFail(err)
	}
	in := &inserter{tx: tx}

	if _, err := r.Seek(0, io.SeekStart); err != nil {
		_ = tx.Rollback()
		return closeAndFail(err)
	}
	if _, err := readBackupHeader(r); err != nil {
		_ = tx.Rollback()
		return closeAndFail(err)
	}
	if err := scanBackup(ctx, r, hdr.rooms, in); err != nil {
		_ = tx.Rollback()
		return closeAndFail(err)
	}
	if err := ctx.Err(); err != nil {
		_ = tx.Rollback()
		return closeAndFail(err)
	}
	if err := tx.Commit(); err != nil {
		return closeAndFail(err)
	}

	// The whole artifact is in. Prove the database is internally consistent
	// before it can become anyone's store.
	var result string
	if err := db.db.QueryRowContext(ctx, `PRAGMA integrity_check`).Scan(&result); err != nil {
		return closeAndFail(err)
	}
	if result != "ok" {
		return closeAndFail(fmt.Errorf("%w: sqlite integrity_check = %q", ErrBackupCorrupt, result))
	}

	// Normalize to a single, portable file: checkpoint WAL into the DB and
	// leave the temp database in rollback-journal mode so there are no -wal/
	// -shm sidecars to carry through the rename. The server re-enables WAL on
	// open.
	if _, err := db.db.ExecContext(context.Background(), `PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
		return closeAndFail(err)
	}
	var mode string
	if err := db.db.QueryRowContext(context.Background(), `PRAGMA journal_mode=DELETE`).Scan(&mode); err != nil {
		return closeAndFail(err)
	}
	if err := db.Close(); err != nil {
		removeDBFiles(tmp)
		return err
	}
	// Defensive: even if the mode switch left sidecars behind, they must not
	// accompany the temp database to the destination.
	removeSidecars(tmp)
	if err := fsyncFile(tmp); err != nil {
		removeDBFiles(tmp)
		return err
	}

	// One last cancellation gate before the visible step.
	if err := ctx.Err(); err != nil {
		removeDBFiles(tmp)
		return err
	}
	if err := os.Rename(tmp, dstPath); err != nil {
		removeDBFiles(tmp)
		return err
	}
	if err := syncDir(dir); err != nil {
		// The rename is already durable-or-not per OS semantics; report but
		// the new database is in place. Sidecar cleanup below still runs.
		return err
	}
	// Remove sidecars the REPLACED database may have left (its data is fully
	// inside the new file). Done after the rename: if this step fails or is
	// interrupted, the old database was already atomically replaced rather
	// than partially deleted.
	removeSidecars(dstPath)

	committed = true
	return nil
}

// targetOccupied reports whether dst is an existing non-installable target:
// any file at dst (including zero bytes), or a leftover -wal/-shm sidecar that
// implies a live database. A directory at dst is reported as an error rather
// than "occupied".
func targetOccupied(dst string) (bool, error) {
	st, err := os.Stat(dst)
	if err == nil {
		if st.IsDir() {
			return false, fmt.Errorf("sqlite: restore target %q is a directory", dst)
		}
		return true, nil
	}
	if !os.IsNotExist(err) {
		return false, err
	}
	for _, sidecar := range []string{dst + "-wal", dst + "-shm"} {
		if _, err := os.Stat(sidecar); err == nil {
			return true, nil
		} else if !os.IsNotExist(err) {
			return false, err
		}
	}
	return false, nil
}

// createTempDBPath returns a fresh, nonexistent path in dir with a name that
// can never be confused with a finished database or backup (dot-prefix,
// .tmp suffix, random suffix, and never the target's basename).
func createTempDBPath(dir string) (string, error) {
	for range 4 {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return "", err
		}
		p := filepath.Join(dir, fmt.Sprintf(".ygorestore-%x.tmp", b))
		f, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			_ = f.Close()
			return p, nil
		}
		if !os.IsExist(err) {
			return "", err
		}
	}
	return "", errors.New("sqlite: could not allocate a unique temp database path")
}

func removeSidecars(path string) {
	_ = os.Remove(path + "-wal")
	_ = os.Remove(path + "-shm")
}

func removeDBFiles(path string) {
	removeSidecars(path)
	_ = os.Remove(path)
}

func fsyncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}

// ---- file-level wrappers (atomic temp + rename) ---------------------------

// BackupFile opens the durable SQLite database at srcPath and writes a backup
// artifact to dstPath. The artifact is assembled in a uniquely-named temp file
// in dstPath's directory and renamed into place only after a successful flush
// and fsync, so a failed or interrupted run never leaves a partial artifact at
// dstPath and never replaces an existing backup. If dstPath already exists the
// call fails with ErrBackupFileExists unless force is true.
//
// The source database is opened read-only-ish through the normal WAL-enabled
// driver: WAL permits this while the server is running, and Backup takes one
// read transaction for the whole capture. No listener or relay is involved.
func BackupFile(ctx context.Context, srcPath, dstPath string, force bool) (info BackupInfo, err error) {
	if srcPath == "" || srcPath == ":memory:" {
		return BackupInfo{}, ErrInMemoryNotBackupable
	}
	if dstPath == "" {
		return BackupInfo{}, errors.New("sqlite: backup output path is empty")
	}
	st, err := os.Stat(srcPath)
	if err != nil {
		return BackupInfo{}, fmt.Errorf("source store: %w", err)
	}
	if st.IsDir() {
		return BackupInfo{}, fmt.Errorf("source store %q is a directory", srcPath)
	}
	// The artifact must never be written over the live database or its
	// journaling sidecars: doing so while a server has the store open would
	// corrupt the running database, and the result would not be a backup.
	cleanDst := filepath.Clean(dstPath)
	for _, forbidden := range []string{srcPath, srcPath + "-wal", srcPath + "-shm", srcPath + "-journal"} {
		if cleanDst == filepath.Clean(forbidden) {
			return BackupInfo{}, fmt.Errorf("sqlite: backup output must not be the database or its sidecars (%s)", forbidden)
		}
	}

	dir := filepath.Dir(dstPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return BackupInfo{}, err
	}
	if _, err := os.Stat(dstPath); err == nil {
		if !force {
			return BackupInfo{}, fmt.Errorf("%w: %s", ErrBackupFileExists, dstPath)
		}
	} else if !os.IsNotExist(err) {
		return BackupInfo{}, err
	}

	store, err := Open(srcPath)
	if err != nil {
		return BackupInfo{}, err
	}
	defer func() { _ = store.Close() }()

	tmp, f, err := createTempArtifactFile(dir)
	if err != nil {
		return BackupInfo{}, err
	}
	success := false
	defer func() {
		if !success {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()

	info, err = Backup(ctx, store, f)
	if err != nil {
		return BackupInfo{}, err
	}
	if err := ctx.Err(); err != nil {
		return BackupInfo{}, err
	}
	if err := f.Sync(); err != nil {
		return BackupInfo{}, err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return BackupInfo{}, err
	}
	if err := os.Rename(tmp, dstPath); err != nil {
		_ = os.Remove(tmp)
		return BackupInfo{}, err
	}
	if err := syncDir(dir); err != nil {
		return BackupInfo{}, err
	}
	success = true
	return info, nil
}

// RestoreFile validates the artifact at backupPath and installs it at dstPath
// per Restore's contract. The input file is never modified; a leftover temp
// file or a raw SQLite database passed in here is rejected as ErrNotABackup
// before anything is built.
func RestoreFile(ctx context.Context, backupPath, dstPath string, force bool) (BackupInfo, error) {
	if backupPath == "" {
		return BackupInfo{}, errors.New("sqlite: backup input path is empty")
	}
	st, err := os.Stat(backupPath)
	if err != nil {
		return BackupInfo{}, fmt.Errorf("backup file: %w", err)
	}
	if st.IsDir() {
		return BackupInfo{}, fmt.Errorf("backup file %q is a directory", backupPath)
	}
	if st.Size() == 0 {
		return BackupInfo{}, ErrNotABackup
	}
	f, err := os.Open(backupPath)
	if err != nil {
		return BackupInfo{}, err
	}
	defer func() { _ = f.Close() }()
	return Restore(ctx, f, dstPath, force)
}

// createTempArtifactFile creates a uniquely-named temp file in dir with a name
// that cannot be mistaken for a finished backup or a database (dot-prefix,
// .tmp suffix). O_EXCL closes the race on the random name.
func createTempArtifactFile(dir string) (string, *os.File, error) {
	var lastErr error
	for range 4 {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return "", nil, err
		}
		p := filepath.Join(dir, fmt.Sprintf(".ygobackup-%x.tmp", b))
		f, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err == nil {
			return p, f, nil
		}
		lastErr = err
		if !os.IsExist(err) {
			return "", nil, err
		}
	}
	return "", nil, lastErr
}
