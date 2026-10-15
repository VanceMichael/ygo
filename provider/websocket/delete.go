// Permanent room deletion and explicit new generations (room lifecycle).
//
// DeleteRoom is the administrative counterpart of CloseRoom: CloseRoom only
// unloads a room from THIS process's memory and leaves all durable state and
// cluster state intact (a later connection reloads it), while DeleteRoom
// installs a generation BARRIER, tears the live room down in order, wipes the
// update history, checkpoint and both snapshot kinds, records a restart-proof
// tombstone, and announces the deletion cluster-wide. The name then stays
// refused — new connections, Apply, BroadcastUpdate and relay Inject — until
// an administrator explicitly opens a new generation with ResetRoom.
package websocket

import (
	"context"
	"errors"
	"fmt"
	"time"

	gws "github.com/gorilla/websocket"

	"github.com/reearth/ygo/cluster"
	"github.com/reearth/ygo/persistence"
)

// Error sentinels for the room lifecycle API. Compare with errors.Is.
var (
	// ErrRoomDeleted is returned when an operation targets a room that has
	// been permanently deleted: peer upgrades surface it as HTTP 410 Gone;
	// Apply / BroadcastUpdate / Inject return it directly. The name remains
	// barred until ResetRoom opens a new generation.
	ErrRoomDeleted = errors.New("ygo/websocket: room is permanently deleted")
	// ErrDeletionUnsupported is returned by DeleteRoom/ResetRoom when a
	// persistence adapter is configured but does not implement durable
	// generation tombstones. The in-memory barrier is still installed (and
	// memory-side teardown completes), but the caller cannot be promised
	// restart safety; the error is decidable and the operation is retryable
	// after swapping the adapter.
	ErrDeletionUnsupported = errors.New("ygo/websocket: persistence adapter does not support permanent room deletion")
	// ErrDeletionStorage is returned when the durable delete/reset failed.
	// The barrier stays in place; retrying the same DeleteRoom/ResetRoom
	// converges without creating a second deletion.
	ErrDeletionStorage = errors.New("ygo/websocket: room deletion storage cleanup failed")
	// ErrDeletionPropagation is returned when announcing the lifecycle event
	// across the relay failed. Local deletion has completed and the barrier
	// stays; retrying republishes the same generation idempotently.
	ErrDeletionPropagation = errors.New("ygo/websocket: room deletion cluster propagation failed")
)

// roomGenerationStore is the durable generation-tombstone surface the server
// drives. persistence.LegacyAdapter satisfies it for every bundled store.
type roomGenerationStore interface {
	DeleteRoom(ctx context.Context, room string) (generation uint64, err error)
	MarkRoomDeleted(ctx context.Context, room string, generation uint64) error
	RoomGeneration(ctx context.Context, room string) (generation uint64, deleted bool, err error)
	ResetRoom(ctx context.Context, room string) (generation uint64, err error)
	MarkRoomReset(ctx context.Context, room string, generation uint64) error
}

// bumpLocalGenLocked returns the next in-process generation. Callers hold
// s.rmu. Only authoritative for deployments without a durable generation
// store (or until the store returns its generation and the grave is
// reconciled to the larger of the two).
func (s *Server) bumpLocalGenLocked() uint64 {
	s.localGen++
	return s.localGen
}

// setGrave records (gen, deleted) for name, never lowering the generation.
func (s *Server) setGrave(name string, gen uint64, deleted bool) {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	cur, ok := s.graves[name]
	if ok && cur.gen > gen {
		gen = cur.gen
	}
	s.graves[name] = roomGeneration{gen: gen, deleted: deleted}
}

// DeleteRoom permanently deletes the named room.
//
// It is the permanent counterpart of CloseRoom, whose unload/reload semantics
// are unchanged: CloseRoom does not touch durably stored content; DeleteRoom
// does.
//
// Sequence, matching the operational contract:
//
//  1. Barrier — the room is removed from the live map and a generation
//     tombstone is installed immediately, before any teardown: from this
//     instant new connections (HTTP 410), Apply, BroadcastUpdate and remote
//     Inject for the name are refused, and late frames on existing peer
//     connections are dropped.
//  2. Ordered teardown — in-flight joiners drain; existing peers are closed;
//     the relay subscription and outbound lane are retired and drained; the
//     persistence worker is stopped, its observer detached, and in-flight
//     observers joined; awareness is destroyed.
//  3. Durable wipe — the update history, prune checkpoint, named snapshots
//     and labelled snapshots are deleted and the generation tombstone is
//     recorded, so a process restart loads nothing and refuses the name.
//  4. Propagation — a KindDelete event carrying the generation is published
//     on the relay's cluster-wide control channel, so every reachable node
//     performs the same delete idempotently (including nodes that never
//     hosted the room).
//
// Idempotency: submitting the same deletion again re-runs the idempotent
// finalize steps and returns nil once they succeed. Failures return decidable
// sentinels (ErrDeletionStorage / ErrDeletionPropagation /
// ErrDeletionUnsupported) but NEVER revoke the barrier; the room stays barred
// and the retry converges. With no persistence and no relay configured, the
// call still completes the full memory-side teardown.
func (s *Server) DeleteRoom(ctx context.Context, name string) error {
	select {
	case <-s.shutdownCh:
		return ErrServerShutdown
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !isValidRoomName(name) {
		return ErrInvalidRoomName
	}

	s.rmu.Lock()
	if g, ok := s.graves[name]; ok && g.deleted {
		// Same deletion resubmitted: converge at the SAME generation. A live
		// instance should never coexist with the barrier, but retire it if
		// one is found (half-finished earlier attempt).
		gen := g.gen
		rm := s.rooms[name]
		if rm != nil {
			delete(s.rooms, name)
			rm.retired.Store(true)
		}
		s.rmu.Unlock()
		if rm != nil {
			if err := s.retireRoomInstance(ctx, name, rm); err != nil {
				return err
			}
		}
		return s.finishLocalDelete(ctx, name, gen)
	}
	gen := s.bumpLocalGenLocked()
	rm := s.installBarrierLocked(name, gen)
	s.rmu.Unlock()

	if rm != nil {
		if err := s.retireRoomInstance(ctx, name, rm); err != nil {
			return err
		}
	}
	return s.finishLocalDelete(ctx, name, gen)
}

// ResetRoom is the explicit administrator action that opens a NEW generation
// for a deleted room name: it tears down any resident instance, wipes all
// durable data and the tombstone, and announces a KindReset cluster-wide,
// after which the name may be used again from an EMPTY state. It does not
// itself create a room — the next connection/Apply does.
//
// During the operation the deletion barrier stays closed, so no connection
// can observe a half-reset room; if storage cleanup or propagation fails the
// barrier is NOT lifted and the call is retryable. Resetting a name the
// server and its store have never heard of is a successful no-op.
func (s *Server) ResetRoom(ctx context.Context, name string) error {
	select {
	case <-s.shutdownCh:
		return ErrServerShutdown
	default:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !isValidRoomName(name) {
		return ErrInvalidRoomName
	}

	s.rmu.Lock()
	grave, hadGrave := s.graves[name]
	rm := s.rooms[name]
	known := hadGrave || rm != nil
	gen := s.bumpLocalGenLocked()
	if hadGrave && grave.gen >= gen {
		gen = grave.gen + 1
		s.localGen = gen
	}
	// Temporary barrier for the reset itself.
	rm2 := s.installBarrierLocked(name, gen)
	if rm2 != nil {
		rm = rm2
	}
	s.rmu.Unlock()

	if rm != nil {
		if err := s.retireRoomInstance(ctx, name, rm); err != nil {
			return err
		}
	}

	// Durable reset.
	if s.persistence != nil {
		ts, ok := s.persistence.(roomGenerationStore)
		if !ok {
			return ErrDeletionUnsupported
		}
		storeGen, err := ts.ResetRoom(ctx, name)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrDeletionStorage, err)
		}
		if storeGen > gen {
			gen = storeGen
		}
		if storeGen == 0 && !known {
			// Nothing existed anywhere: drop the provisional barrier without
			// announcing a reset nobody needs.
			s.removeGrave(name, gen)
			return nil
		}
	} else if !known {
		s.removeGrave(name, gen)
		return nil
	}

	// Announce BEFORE lifting the local barrier: a peer that reconnects the
	// instant the barrier lifts must find every other node already reset.
	if s.relay != nil {
		if err := s.publishLifecycle(ctx, name, cluster.KindReset, gen); err != nil {
			return fmt.Errorf("%w: %v", ErrDeletionPropagation, err)
		}
	}
	s.setGrave(name, gen, false)
	return nil
}

// installBarrierLocked records a deleted generation barrier for name and
// detaches the live room instance (removed from the map, marked retired). It
// returns the detached instance (nil if none was resident). Caller holds
// s.rmu.
func (s *Server) installBarrierLocked(name string, gen uint64) *room {
	if cur, ok := s.graves[name]; ok && cur.gen > gen {
		gen = cur.gen
	}
	s.graves[name] = roomGeneration{gen: gen, deleted: true}
	rm := s.rooms[name]
	if rm != nil {
		delete(s.rooms, name)
		rm.retired.Store(true)
	}
	return rm
}

// removeGrave removes the barrier only while it still records generation gen
// or below; a newer lifecycle event in the meantime wins and is kept.
func (s *Server) removeGrave(name string, gen uint64) {
	s.rmu.Lock()
	defer s.rmu.Unlock()
	if cur, ok := s.graves[name]; ok && cur.gen <= gen {
		delete(s.graves, name)
	}
}

// finishLocalDelete performs the post-teardown steps of a LOCAL DeleteRoom:
// durable wipe (store mints the authoritative generation) then cluster
// announcement. Both are idempotent, so an admin retry after a partial failure
// converges at the same generation.
func (s *Server) finishLocalDelete(ctx context.Context, name string, gen uint64) error {
	var errs []error
	if s.persistence != nil {
		if ts, ok := s.persistence.(roomGenerationStore); ok {
			storeGen, err := ts.DeleteRoom(ctx, name)
			if err != nil {
				errs = append(errs, fmt.Errorf("%w: %v", ErrDeletionStorage, err))
			} else if storeGen > gen {
				gen = storeGen
				s.setGrave(name, gen, true)
			}
		} else {
			errs = append(errs, ErrDeletionUnsupported)
		}
	}
	if s.relay != nil {
		if err := s.publishLifecycle(ctx, name, cluster.KindDelete, gen); err != nil {
			errs = append(errs, fmt.Errorf("%w: %v", ErrDeletionPropagation, err))
		}
	}
	return errors.Join(errs...)
}

// publishLifecycle announces one room lifecycle event on the relay control
// plane. Unlike data-plane Publish calls this is expected to be synchronous
// with a decidable error (the Redis relay publishes control events
// synchronously; MemRelay hands to all in-process nodes).
func (s *Server) publishLifecycle(ctx context.Context, name string, kind cluster.Kind, gen uint64) error {
	return s.relay.Publish(ctx, cluster.Outbound{
		Room: name,
		Kind: kind,
		Data: cluster.EncodeLifecycleEvent(gen),
	})
}

// applyRemoteLifecycle applies a relay-delivered KindDelete/KindReset event.
// It never republishes (the echo guard for lifecycle events is structural:
// this path does not call Publish) and is idempotent by generation.
func (s *Server) applyRemoteLifecycle(ctx context.Context, in cluster.Inbound) error {
	gen, err := cluster.DecodeLifecycleEvent(in.Data)
	if err != nil {
		return err
	}

	s.rmu.Lock()
	if cur, ok := s.graves[in.Room]; ok && cur.gen >= gen {
		// Duplicate of the same generation or a stale event superseded by a
		// newer lifecycle operation: nothing to do.
		s.rmu.Unlock()
		return nil
	}
	rm := s.installBarrierLocked(in.Room, gen)
	s.rmu.Unlock()

	if rm != nil {
		if err := s.retireRoomInstance(ctx, in.Room, rm); err != nil {
			return err
		}
	}

	if s.persistence != nil {
		ts, ok := s.persistence.(roomGenerationStore)
		if !ok {
			// The origin owns the authoritative durable delete; on this node
			// the in-memory barrier still stands, and the failure is visible.
			return ErrDeletionUnsupported
		}
		switch in.Kind {
		case cluster.KindDelete:
			if err := ts.MarkRoomDeleted(ctx, in.Room, gen); err != nil {
				return fmt.Errorf("%w: %v", ErrDeletionStorage, err)
			}
			// Keep the barrier closed.
			return nil
		case cluster.KindReset:
			if err := ts.MarkRoomReset(ctx, in.Room, gen); err != nil {
				return fmt.Errorf("%w: %v", ErrDeletionStorage, err)
			}
		}
	}
	if in.Kind == cluster.KindReset {
		s.setGrave(in.Room, gen, false)
	}
	return nil
}

// retireRoomInstance performs the ordered teardown of ONE detached room
// instance: it assumes the generation barrier is already installed and the
// instance removed from s.rooms (so nothing new can register). Every step is
// resumable: if ctx expires mid-sequence the caller retries (the admin API is
// idempotent and relay redelivery re-enters here), and the second run
// continues from whatever step remains. retireMu serialises the runs; the
// completed signal lets concurrent callers await the same teardown.
func (s *Server) retireRoomInstance(ctx context.Context, name string, rm *room) error {
	// The barrier can be installed while the off-lock load is still running;
	// its placeholder has no peers/workers, but teardown must wait for the
	// load to finish wiring so nothing it creates outlives the delete.
	select {
	case <-rm.ready:
	case <-ctx.Done():
		return ctx.Err()
	}

	rm.retireMu.Lock()
	if rm.retireDone != nil {
		done := rm.retireDone
		rm.retireMu.Unlock()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	defer rm.retireMu.Unlock()

	if err := s.waitRoomInflight(ctx, rm); err != nil {
		return err
	}
	if err := s.closeRoomPeers(ctx, rm); err != nil {
		return err
	}
	if err := s.teardownRelayRoomAndWait(ctx, rm, name); err != nil {
		return err
	}
	if err := s.stopRoomPersistence(ctx, rm); err != nil {
		return err
	}
	// Awareness after its feeders (peers/relay/persistence) are gone: stop
	// the auto-expiry sweep and release the awareness state.
	rm.awareness.Destroy()
	if hook := s.OnUnloadDocument; hook != nil {
		s.safeHook("OnUnloadDocument", func() {
			hook(context.Background(), name)
		})
	}

	done := make(chan struct{})
	rm.retireDone = done
	close(done)
	return nil
}

// waitRoomInflight blocks until callers that obtained the room through
// getOrCreateRoom but have not yet registered a peer or finished a
// synchronous Apply/Inject are gone. The barrier already prevents NEW
// callers; only in-flight ones remain, and a failed WS join releases its
// count on the upgrade-failure paths. Bounded by ctx.
func (s *Server) waitRoomInflight(ctx context.Context, rm *room) error {
	for {
		rm.mu.Lock()
		n := rm.inflight
		rm.mu.Unlock()
		if n == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Microsecond):
		}
	}
}

// closeRoomPeers closes every peer connection of the detached room and waits
// for the disconnect handlers to finish (or ctx to expire).
func (s *Server) closeRoomPeers(ctx context.Context, rm *room) error {
	rm.mu.Lock()
	conns := make([]*gws.Conn, 0, len(rm.peers))
	dones := make([]chan struct{}, 0, len(rm.peers))
	for p := range rm.peers {
		conns = append(conns, p.conn)
		dones = append(dones, p.done)
	}
	rm.mu.Unlock()

	for _, c := range conns {
		_ = c.Close()
	}
	for _, d := range dones {
		select {
		case <-d:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// teardownRelayRoomAndWait is teardownRelayRoom plus a join on the outbound
// lane worker so the room publishes nothing more once it returns: the
// worker's final drain runs BEFORE the caller wipes storage and announces the
// deletion, so any trailing update reaches other nodes strictly ahead of the
// delete event (which then supersedes it everywhere).
func (s *Server) teardownRelayRoomAndWait(ctx context.Context, rm *room, name string) error {
	if s.relay == nil {
		return nil
	}
	rm.mu.Lock()
	unsubs := rm.relayUnsub
	rm.relayUnsub = nil
	lane := rm.relayLane
	rm.relayLane = nil
	rm.mu.Unlock()
	for _, u := range unsubs {
		u()
	}
	s.stopRelayLane(name, lane)
	s.relay.RoomDeactivated(name)
	if lane != nil {
		select {
		case <-lane.exited:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// stopRoomPersistence stops the room's worker (its exit flush drains whatever
// was already queued), detaches the doc observer, waits out observers still
// writing directly as stranded writers, and only then returns — so after a
// successful return no commit from a cached *crdt.Doc can reach storage.
// Everything queued or stranded before this point lands BEFORE the durable
// wipe in finishLocalDelete and is deleted by it.
func (s *Server) stopRoomPersistence(ctx context.Context, rm *room) error {
	if rm.persistStop != nil {
		select {
		case <-rm.persistStop:
		default:
			close(rm.persistStop)
		}
	}
	if rm.persistDone != nil {
		select {
		case <-rm.persistDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if rm.persistUnsub != nil {
		rm.persistUnsub()
		rm.persistUnsub = nil
	}
	rm.obsMu.Lock()
	rm.obsDone = true
	rm.obsMu.Unlock()
	return rm.waitObservers(ctx)
}

// observerEnter records a persistence observer entering. It returns false
// once retirement has detached the observer, in which case the update must
// not be written anywhere.
func (r *room) observerEnter() bool {
	r.obsMu.Lock()
	defer r.obsMu.Unlock()
	if r.obsDone {
		return false
	}
	r.obsN++
	return true
}

// observerLeave balances observerEnter and wakes the teardown wait on the
// 1→0 transition (latch replaced and closed, mirroring the server-wide
// stranded-writer broadcast).
func (r *room) observerLeave() {
	r.obsMu.Lock()
	r.obsN--
	if r.obsN == 0 {
		ch := r.obsZero
		r.obsZero = make(chan struct{})
		r.obsMu.Unlock()
		if ch != nil {
			close(ch)
		}
		return
	}
	r.obsMu.Unlock()
}

// waitObservers blocks until every observer that entered has left, or ctx
// expires.
func (r *room) waitObservers(ctx context.Context) error {
	for {
		r.obsMu.Lock()
		n := r.obsN
		ch := r.obsZero
		r.obsMu.Unlock()
		if n == 0 {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// The five generation-tombstone methods are also exposed on the in-package
// websocket.MemoryPersistence wrapper by delegation so the server detects the
// extension through the same adapter value the rest of persistence uses.
// (persistence.LegacyAdapter already implements them for the file, sqlite and
// raw in-memory stores.)

func (m *MemoryPersistence) DeleteRoom(ctx context.Context, room string) (uint64, error) {
	return m.adapter.DeleteRoom(ctx, room)
}

func (m *MemoryPersistence) MarkRoomDeleted(ctx context.Context, room string, generation uint64) error {
	return m.adapter.MarkRoomDeleted(ctx, room, generation)
}

func (m *MemoryPersistence) RoomGeneration(ctx context.Context, room string) (uint64, bool, error) {
	return m.adapter.RoomGeneration(ctx, room)
}

func (m *MemoryPersistence) ResetRoom(ctx context.Context, room string) (uint64, error) {
	return m.adapter.ResetRoom(ctx, room)
}

func (m *MemoryPersistence) MarkRoomReset(ctx context.Context, room string, generation uint64) error {
	return m.adapter.MarkRoomReset(ctx, room, generation)
}

// Compile-time guards.
var (
	_ roomGenerationStore = (*persistence.LegacyAdapter)(nil)
	_ roomGenerationStore = (*MemoryPersistence)(nil)
)
