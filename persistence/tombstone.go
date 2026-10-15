package persistence

import (
	"context"
	"errors"
)

// ErrRoomDeleted is returned by mutating and loading operations on a room that
// has been permanently deleted via a generation tombstone (RoomTombstoneStore).
// It is the durable, restart-surviving signal that distinguishes "this name is
// barred pending an explicit new generation" from an ordinary empty/unknown
// room (which Load reports as a zero LoadResult): a store MUST NOT bootstrap an
// empty room over a tombstone, otherwise a process restart or a reconnecting
// peer would resurrect deleted content.
var ErrRoomDeleted = errors.New("persistence: room is permanently deleted")

// ErrTombstonesUnsupported is returned by LegacyAdapter's generation-tombstone
// forwards when the wrapped VersionedPersistence does not implement
// RoomTombstoneStore. Surfaced (rather than silently treated as success) so an
// administrator cannot believe a permanent delete survived a restart when the
// backing store has nowhere to record it.
var ErrTombstonesUnsupported = errors.New("persistence: store does not implement RoomTombstoneStore")

// RoomTombstoneStore is the optional generation-tombstone extension of
// VersionedPersistence. A backend that implements it can permanently delete a
// room — its update history, checkpoints, and BOTH kinds of snapshots — and
// record a generation tombstone so the room name cannot be reused (and no
// delayed or restart-driven write can recreate the room) until an administrator
// explicitly opens a new generation with ResetRoom.
//
// # Generations
//
// A generation is a monotonically increasing uint64 per room name. A delete
// records generation G with the room tombstoned; a reset records that
// generations <= G are superseded and removes BOTH the data and the tombstone.
// Cluster-wide deletes/resets for the same room carry the generation in their
// relay event so every node applies the same operation once: an incoming
// event whose generation is not newer than the one recorded is a stale
// duplicate and is ignored.
//
// Method contracts:
//
//   - DeleteRoom permanently removes every byte stored for the room (update
//     log, named CaptureSnapshot snapshots, labelled SnapshotStore snapshots,
//     and the prune checkpoint) and records a tombstone at the NEXT
//     generation. It returns the recorded generation. It is idempotent for
//     the SAME deletion: deleting an already-tombstoned room returns the
//     existing generation without changing anything.
//
//   - MarkRoomDeleted applies a delete that another node originated at
//     generation. It performs the same full wipe as DeleteRoom but records AT
//     LEAST the supplied generation: a tombstone already at an equal or higher
//     generation makes the call a no-op. generation values below 1 are
//     normalised to 1. Idempotent.
//
//   - RoomGeneration reports the recorded generation and whether the room is
//     currently tombstoned. An unknown room returns (0, false, nil).
//
//   - ResetRoom is the explicit administrator action that opens a new
//     generation: it removes ALL room data and the tombstone, making the name
//     reusable from an empty state, and returns the superseded generation + 1
//     (0 for a room the store had never heard of, in which case it changes
//     nothing). After ResetRoom the room is not tombstoned.
//
//   - MarkRoomReset applies a remotely-originated reset. A tombstone whose
//     generation is equal to or higher than the supplied one means the reset
//     is stale (a newer delete already superseded it) and is ignored;
//     otherwise all data and the tombstone are removed.
//
// While a room is tombstoned, Load MUST return ErrRoomDeleted and every
// mutating call (AppendUpdate, CaptureSnapshot, SaveSnapshot, PruneAfter,
// Compact) MUST fail with ErrRoomDeleted, so even a writer that bypasses the
// provider's in-memory barrier cannot recreate the room. Read-only history and
// snapshot calls report empty / not-found, as for an unknown room.
//
// RoomTombstoneStore deliberately does not embed VersionedPersistence so the
// latter's contract stays frozen; every in-tree backend implements both.
type RoomTombstoneStore interface {
	DeleteRoom(ctx context.Context, room string) (generation uint64, err error)
	MarkRoomDeleted(ctx context.Context, room string, generation uint64) error
	RoomGeneration(ctx context.Context, room string) (generation uint64, deleted bool, err error)
	ResetRoom(ctx context.Context, room string) (generation uint64, err error)
	MarkRoomReset(ctx context.Context, room string, generation uint64) error
}

// Compile-time assertions that the in-tree backends implement tombstones.
// (sqlite asserts the same in its own package.)
var (
	_ RoomTombstoneStore = (*MemoryPersistence)(nil)
	_ RoomTombstoneStore = (*FilePersistence)(nil)
)
