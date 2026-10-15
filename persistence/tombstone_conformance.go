package persistence

import (
	"context"
	"errors"
	"testing"
)

// TombstoneConformanceStore is the surface RunTombstoneConformance needs:
// full versioned storage plus both snapshot kinds plus generation tombstones.
type TombstoneConformanceStore interface {
	VersionedPersistence
	SnapshotStore
	RoomTombstoneStore
}

// RunTombstoneConformance exercises the RoomTombstoneStore contract against a
// backend: permanent delete wipes updates, checkpoints and BOTH snapshot
// kinds and survives reopen; every mutating/load call is refused while
// tombstoned; resubmitting the same deletion is idempotent at one generation;
// remote Mark* calls converge by generation; ResetRoom reopens an empty new
// generation. factory must return a fresh, empty store per call.
//
// Like RunConformance it lives in the non-test build so backends in other
// packages (persistence/sqlite) can run it.
func RunTombstoneConformance(t *testing.T, factory func() TombstoneConformanceStore) {
	t.Helper()
	ctx := context.Background()

	t.Run("DeleteOnUnknownRoomTombstonesAtGen1", func(t *testing.T) {
		p := factory()
		gen, err := p.DeleteRoom(ctx, "ghost")
		if err != nil {
			t.Fatalf("DeleteRoom: %v", err)
		}
		if gen != 1 {
			t.Fatalf("gen = %d, want 1", gen)
		}
		g, deleted, err := p.RoomGeneration(ctx, "ghost")
		if err != nil || !deleted || g != 1 {
			t.Fatalf("RoomGeneration = (%d,%v,%v), want (1,true,nil)", g, deleted, err)
		}
		if _, err := p.Load(ctx, "ghost"); !errors.Is(err, ErrRoomDeleted) {
			t.Fatalf("Load err = %v, want ErrRoomDeleted", err)
		}
	})

	t.Run("DeleteWipesHistoryCheckpointAndBothSnapshotKinds", func(t *testing.T) {
		p := factory()
		updates, _ := genUpdates(t, 3)
		for _, u := range updates {
			if _, err := p.AppendUpdate(ctx, "r", u); err != nil {
				t.Fatalf("AppendUpdate: %v", err)
			}
		}
		full, err := p.Load(ctx, "r")
		if err != nil {
			t.Fatalf("Load before snapshot: %v", err)
		}
		if _, err := p.CaptureSnapshot(ctx, "r", "named", full.Update); err != nil {
			t.Fatalf("CaptureSnapshot: %v", err)
		}
		if _, err := p.SaveSnapshot(ctx, "r", "auto", full.Update); err != nil {
			t.Fatalf("SaveSnapshot: %v", err)
		}
		// A checkpoint, exercised through PruneAfter rolling back to v1.
		rolled, err := p.MaterializeAt(ctx, "r", 1)
		if err != nil {
			t.Fatalf("MaterializeAt: %v", err)
		}
		if err := p.PruneAfter(ctx, "r", 1, rolled); err != nil {
			t.Fatalf("PruneAfter: %v", err)
		}

		gen, err := p.DeleteRoom(ctx, "r")
		if err != nil {
			t.Fatalf("DeleteRoom: %v", err)
		}
		if gen != 1 {
			t.Fatalf("gen = %d, want 1", gen)
		}

		if _, err := p.Load(ctx, "r"); !errors.Is(err, ErrRoomDeleted) {
			t.Fatalf("Load after delete = %v, want ErrRoomDeleted", err)
		}
		if vs, err := p.ListVersions(ctx, "r"); err != nil || len(vs) != 0 {
			t.Fatalf("ListVersions = %v,%v; want empty", vs, err)
		}
		if _, _, ok, err := p.RestoreSnapshot(ctx, "r", "named"); err != nil || ok {
			t.Fatalf("RestoreSnapshot = ok=%v err=%v; want false,nil", ok, err)
		}
		if snaps, err := p.ListSnapshots(ctx, "r"); err != nil || len(snaps) != 0 {
			t.Fatalf("ListSnapshots = %v,%v; want empty", snaps, err)
		}
		if lister, ok := p.(RoomLister); ok {
			names, err := lister.ListRooms(ctx)
			if err != nil {
				t.Fatalf("ListRooms: %v", err)
			}
			for _, n := range names {
				if n == "r" {
					t.Fatalf("deleted room still listed by ListRooms: %v", names)
				}
			}
		}
	})

	t.Run("MutationsRefusedWhileTombstoned", func(t *testing.T) {
		p := factory()
		updates, _ := genUpdates(t, 1)
		if _, err := p.DeleteRoom(ctx, "r"); err != nil {
			t.Fatalf("DeleteRoom: %v", err)
		}
		if _, err := p.AppendUpdate(ctx, "r", updates[0]); !errors.Is(err, ErrRoomDeleted) {
			t.Fatalf("AppendUpdate = %v, want ErrRoomDeleted", err)
		}
		if _, err := p.CaptureSnapshot(ctx, "r", "n", updates[0]); !errors.Is(err, ErrRoomDeleted) {
			t.Fatalf("CaptureSnapshot = %v, want ErrRoomDeleted", err)
		}
		if _, err := p.SaveSnapshot(ctx, "r", "n", updates[0]); !errors.Is(err, ErrRoomDeleted) {
			t.Fatalf("SaveSnapshot = %v, want ErrRoomDeleted", err)
		}
		if err := p.PruneAfter(ctx, "r", 0, nil); !errors.Is(err, ErrRoomDeleted) {
			t.Fatalf("PruneAfter = %v, want ErrRoomDeleted", err)
		}
		if _, err := p.Compact(ctx, "r", 1); !errors.Is(err, ErrRoomDeleted) {
			t.Fatalf("Compact = %v, want ErrRoomDeleted", err)
		}
	})

	t.Run("ResubmittingDeleteIsIdempotentSameGeneration", func(t *testing.T) {
		p := factory()
		g1, err := p.DeleteRoom(ctx, "r")
		if err != nil {
			t.Fatalf("DeleteRoom: %v", err)
		}
		g2, err := p.DeleteRoom(ctx, "r")
		if err != nil {
			t.Fatalf("DeleteRoom again: %v", err)
		}
		if g1 != g2 {
			t.Fatalf("generations differ: %d vs %d", g1, g2)
		}
	})

	t.Run("MarkRoomDeletedConvergesByGeneration", func(t *testing.T) {
		p := factory()
		updates, _ := genUpdates(t, 2)
		if _, err := p.AppendUpdate(ctx, "r", updates[0]); err != nil {
			t.Fatalf("AppendUpdate: %v", err)
		}
		if err := p.MarkRoomDeleted(ctx, "r", 5); err != nil {
			t.Fatalf("MarkRoomDeleted(5): %v", err)
		}
		if g, deleted, _ := p.RoomGeneration(ctx, "r"); !deleted || g != 5 {
			t.Fatalf("after mark(5): (%d,%v)", g, deleted)
		}
		if _, err := p.Load(ctx, "r"); !errors.Is(err, ErrRoomDeleted) {
			t.Fatalf("Load = %v, want ErrRoomDeleted", err)
		}
		// Older or equal events are ignored.
		if err := p.MarkRoomDeleted(ctx, "r", 3); err != nil {
			t.Fatalf("MarkRoomDeleted(3): %v", err)
		}
		if g, _, _ := p.RoomGeneration(ctx, "r"); g != 5 {
			t.Fatalf("stale mark regressed generation to %d", g)
		}
		// Repeated same event is a no-op.
		if err := p.MarkRoomDeleted(ctx, "r", 5); err != nil {
			t.Fatalf("MarkRoomDeleted(5) repeat: %v", err)
		}
		// Newer event advances the generation.
		if err := p.MarkRoomDeleted(ctx, "r", 7); err != nil {
			t.Fatalf("MarkRoomDeleted(7): %v", err)
		}
		if g, deleted, _ := p.RoomGeneration(ctx, "r"); !deleted || g != 7 {
			t.Fatalf("after mark(7): (%d,%v)", g, deleted)
		}
	})

	t.Run("ResetRoomReopensEmptyNewGenerationAndRefusesStaleDeletes", func(t *testing.T) {
		p := factory()
		updates, _ := genUpdates(t, 2)
		if _, err := p.AppendUpdate(ctx, "r", updates[0]); err != nil {
			t.Fatalf("AppendUpdate: %v", err)
		}
		if _, err := p.DeleteRoom(ctx, "r"); err != nil {
			t.Fatalf("DeleteRoom: %v", err)
		}
		gen, err := p.ResetRoom(ctx, "r")
		if err != nil {
			t.Fatalf("ResetRoom: %v", err)
		}
		if gen != 2 {
			t.Fatalf("reset gen = %d, want 2", gen)
		}
		if _, deleted, err := p.RoomGeneration(ctx, "r"); err != nil || deleted {
			t.Fatalf("RoomGeneration after reset: deleted=%v err=%v", deleted, err)
		}
		if res, err := p.Load(ctx, "r"); err != nil || len(res.Update) != 0 {
			t.Fatalf("Load after reset = %+v,%v; want empty", res, err)
		}
		// Fresh writes succeed and materialise.
		if _, err := p.AppendUpdate(ctx, "r", updates[1]); err != nil {
			t.Fatalf("AppendUpdate after reset: %v", err)
		}
		if res, err := p.Load(ctx, "r"); err != nil || len(res.Update) == 0 {
			t.Fatalf("Load after rewrite = %+v,%v", res, err)
		}
		// A stale delete event for an older generation must not re-tombstone
		// the reset room.
		if err := p.MarkRoomReset(ctx, "r", 2); err != nil {
			t.Fatalf("MarkRoomReset: %v", err)
		}
		if g, deleted, _ := p.RoomGeneration(ctx, "r"); deleted {
			t.Fatalf("reset room reported tombstoned at gen %d", g)
		}
	})

	t.Run("ResetUnknownRoomIsANoop", func(t *testing.T) {
		p := factory()
		gen, err := p.ResetRoom(ctx, "never")
		if err != nil {
			t.Fatalf("ResetRoom: %v", err)
		}
		if gen != 0 {
			t.Fatalf("gen = %d, want 0 for an unknown room", gen)
		}
		if err := p.MarkRoomReset(ctx, "never", 1); err != nil {
			t.Fatalf("MarkRoomReset unknown: %v", err)
		}
	})

	t.Run("DeleteSurvivesReopen", func(t *testing.T) {
		p := factory()
		updates, _ := genUpdates(t, 1)
		if _, err := p.AppendUpdate(ctx, "r", updates[0]); err != nil {
			t.Fatalf("AppendUpdate: %v", err)
		}
		if _, err := p.DeleteRoom(ctx, "r"); err != nil {
			t.Fatalf("DeleteRoom: %v", err)
		}
		re, ok := p.(Reopener)
		if !ok {
			t.Logf("%T is not a Reopener; restart subtest skipped", p)
			return
		}
		fresh, err := re.Reopen()
		if err != nil {
			t.Fatalf("Reopen: %v", err)
		}
		if _, err := fresh.Load(ctx, "r"); !errors.Is(err, ErrRoomDeleted) {
			t.Fatalf("Load after reopen = %v, want ErrRoomDeleted", err)
		}
		ts, ok := fresh.(RoomTombstoneStore)
		if !ok {
			t.Fatalf("reopened store %T does not implement RoomTombstoneStore", fresh)
		}
		if g, deleted, err := ts.RoomGeneration(ctx, "r"); err != nil || !deleted || g != 1 {
			t.Fatalf("RoomGeneration after reopen = (%d,%v,%v)", g, deleted, err)
		}
		// Reset on the reopened handle lifts the barrier durably.
		if _, err := ts.ResetRoom(ctx, "r"); err != nil {
			t.Fatalf("ResetRoom after reopen: %v", err)
		}
		fresh2, err := re.Reopen()
		if err != nil {
			t.Fatalf("second Reopen: %v", err)
		}
		if res, err := fresh2.Load(ctx, "r"); err != nil || len(res.Update) != 0 {
			t.Fatalf("Load after reset+reopen = %+v,%v; want empty", res, err)
		}
	})
}
