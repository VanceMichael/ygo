package sqlite

import (
	"context"
	"database/sql"
	"math"
	"time"

	"github.com/reearth/ygo/persistence"
)

// allRoomTables are every per-room table wiped by a permanent deletion. The
// room_tombstones row is handled separately by each caller: DeleteRoom /
// MarkRoomDeleted UPSERT it, while ResetRoom / MarkRoomReset delete it.
var allRoomTables = []string{
	`DELETE FROM updates WHERE room = ?`,
	`DELETE FROM snapshots WHERE room = ?`,
	`DELETE FROM snapshot_versions WHERE room = ?`,
	`DELETE FROM checkpoints WHERE room = ?`,
}

// tombstoneGen returns the recorded generation and true when the room is
// tombstoned. Lock-free read, like the other read methods (a concurrent
// DeleteRoom serialises its own writes under s.mu; SQLite sees the row only
// once that transaction commits).
func (s *Store) tombstoneGen(ctx context.Context, room string) (int64, bool, error) {
	var gen int64
	err := s.db.QueryRowContext(ctx,
		`SELECT generation FROM room_tombstones WHERE room = ?`, room).Scan(&gen)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return gen, true, nil
}

// checkAliveLocked returns persistence.ErrRoomDeleted when the room is
// tombstoned. Callers are mutating methods already holding s.mu.
func (s *Store) checkAliveLocked(ctx context.Context, room string) error {
	_, tomb, err := s.tombstoneGen(ctx, room)
	if err != nil {
		return err
	}
	if tomb {
		return persistence.ErrRoomDeleted
	}
	return nil
}

// roomHasAnyData reports whether any per-room table (other than the tombstone
// table) holds a row for room.
func (s *Store) roomHasAnyData(ctx context.Context, room string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `
SELECT EXISTS(SELECT 1 FROM updates WHERE room = ?)
    OR EXISTS(SELECT 1 FROM snapshots WHERE room = ?)
    OR EXISTS(SELECT 1 FROM snapshot_versions WHERE room = ?)
    OR EXISTS(SELECT 1 FROM checkpoints WHERE room = ?)`,
		room, room, room, room).Scan(&n)
	if err != nil {
		return false, err
	}
	return n != 0, nil
}

// clampGen normalises an incoming generation into int64 range with a floor of
// 1, matching the in-memory and file backends' "at least generation 1" rule.
func clampGen(gen uint64) int64 {
	if gen < 1 {
		return 1
	}
	if gen > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(gen)
}

// wipeRoomDataTx deletes every per-room row inside tx.
func wipeRoomDataTx(ctx context.Context, tx *sql.Tx, room string) error {
	for _, q := range allRoomTables {
		if _, err := tx.ExecContext(ctx, q, room); err != nil {
			return err
		}
	}
	return nil
}

// DeleteRoom permanently wipes the room and records a deletion tombstone at
// the next generation. See persistence.RoomTombstoneStore.
func (s *Store) DeleteRoom(ctx context.Context, room string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	gen, tomb, err := s.tombstoneGen(ctx, room)
	if err != nil {
		return 0, err
	}
	if tomb {
		return uint64(gen), nil // same deletion resubmitted: idempotent
	}
	next := gen + 1
	if next < 1 {
		next = 1
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO room_tombstones (room, generation, deleted_at) VALUES (?, ?, ?)`,
		room, next, time.Now().UnixNano()); err != nil {
		return 0, err
	}
	if err := wipeRoomDataTx(ctx, tx, room); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return uint64(next), nil
}

// MarkRoomDeleted records a remotely-originated deletion at AT LEAST
// generation. See persistence.RoomTombstoneStore.
func (s *Store) MarkRoomDeleted(ctx context.Context, room string, generation uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g := clampGen(generation)
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, tomb, err := s.tombstoneGen(ctx, room)
	if err != nil {
		return err
	}
	if tomb && existing >= g {
		return nil // stale or duplicate deletion event
	}
	if existing > g {
		g = existing
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO room_tombstones (room, generation, deleted_at) VALUES (?, ?, ?)
		 ON CONFLICT(room) DO UPDATE SET generation=excluded.generation, deleted_at=excluded.deleted_at`,
		room, g, time.Now().UnixNano()); err != nil {
		return err
	}
	if err := wipeRoomDataTx(ctx, tx, room); err != nil {
		return err
	}
	return tx.Commit()
}

// RoomGeneration reports the recorded generation and the tombstoned flag.
func (s *Store) RoomGeneration(ctx context.Context, room string) (uint64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	gen, tomb, err := s.tombstoneGen(ctx, room)
	if err != nil {
		return 0, false, err
	}
	return uint64(gen), tomb, nil
}

// ResetRoom removes all room data and the tombstone, returning the
// superseded generation + 1. See persistence.RoomTombstoneStore.
func (s *Store) ResetRoom(ctx context.Context, room string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	gen, tomb, err := s.tombstoneGen(ctx, room)
	if err != nil {
		return 0, err
	}
	if !tomb {
		has, err := s.roomHasAnyData(ctx, room)
		if err != nil {
			return 0, err
		}
		if !has {
			return 0, nil // the store has never heard of this room
		}
	}
	next := gen + 1
	if next < 1 {
		next = 1
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck
	if err := wipeRoomDataTx(ctx, tx, room); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM room_tombstones WHERE room = ?`, room); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return uint64(next), nil
}

// MarkRoomReset applies a remotely-originated reset, ignoring stale events.
// See persistence.RoomTombstoneStore.
func (s *Store) MarkRoomReset(ctx context.Context, room string, generation uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	g := clampGen(generation)
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, tomb, err := s.tombstoneGen(ctx, room)
	if err != nil {
		return err
	}
	if tomb && existing >= g {
		return nil // a newer-or-equal deletion supersedes this reset
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if err := wipeRoomDataTx(ctx, tx, room); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM room_tombstones WHERE room = ?`, room); err != nil {
		return err
	}
	return tx.Commit()
}
