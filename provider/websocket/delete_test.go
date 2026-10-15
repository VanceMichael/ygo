// Integration tests for permanent room deletion (DeleteRoom) and explicit
// new-generation reopening (ResetRoom). These exercise the operational
// contract: barrier-before-teardown, ordered teardown, durable wipe +
// tombstone, cluster-wide convergence, restart safety, idempotency, and the
// error/barrier-preservation rules on storage or propagation failure.
package websocket_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	gws "github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/reearth/ygo/cluster"
	"github.com/reearth/ygo/crdt"
	ygws "github.com/reearth/ygo/provider/websocket"
)

// roomContent applies a small edit to the named room via Apply so the room
// has observable state, then returns it for assertions.
func roomContent(t *testing.T, srv *ygws.Server, room string) string {
	t.Helper()
	d := srv.GetDoc(room)
	if d == nil {
		return ""
	}
	return d.GetText("t").ToString()
}

// writeText applies `text` to room via Apply.
func writeText(t *testing.T, srv *ygws.Server, room, text string) {
	t.Helper()
	require.NoError(t, srv.Apply(context.Background(), room, func(d *crdt.Doc, transact func(func(*crdt.Transaction))) {
		txt := d.GetText("t")
		transact(func(txn *crdt.Transaction) { txt.Insert(txn, 0, text, nil) })
	}))
}

// dialExpectGone dials room and asserts the upgrade is rejected with HTTP 410
// Gone (the permanent-delete barrier response).
func dialExpectGone(t *testing.T, ts *httptest.Server, room string) {
	t.Helper()
	_, resp, err := gws.DefaultDialer.Dial(wsURL(ts, room), nil)
	require.Error(t, err, "dial to a deleted room must fail")
	require.NotNil(t, resp, "expected an HTTP response carrying the 410 status")
	assert.Equal(t, http.StatusGone, resp.StatusCode,
		"deleted room must be refused with 410 Gone, got %d", resp.StatusCode)
	_ = resp.Body.Close()
}

// TestDeleteRoom_NoPersistenceNoRelay_MemorySideBarrier verifies that even with
// no persistence adapter and no relay attached, DeleteRoom installs the
// in-memory barrier: existing peers are closed, new connections get 410, and
// Apply / BroadcastUpdate return ErrRoomDeleted. The name cannot be reused
// until ResetRoom.
func TestDeleteRoom_NoPersistenceNoRelay_MemorySideBarrier(t *testing.T) {
	srv := ygws.NewServer()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	writeText(t, srv, "memroom", "hello")
	require.Equal(t, "hello", roomContent(t, srv, "memroom"))

	// A live peer must be closed by the delete.
	conn := dial(t, ts, "memroom")
	drainHandshake(t, conn, crdt.New())

	require.NoError(t, srv.DeleteRoom(context.Background(), "memroom"))

	// Peer connection was torn down: a read should fail quickly.
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("expected the deleted room's peer connection to be closed")
	}

	// New connection is refused with 410.
	dialExpectGone(t, ts, "memroom")

	// Apply and BroadcastUpdate are refused, distinct from ErrRoomNotFound.
	err := srv.Apply(context.Background(), "memroom", func(d *crdt.Doc, transact func(func(*crdt.Transaction))) {})
	assert.ErrorIs(t, err, ygws.ErrRoomDeleted)

	// BroadcastUpdate needs valid bytes; reuse an empty-update-ish frame is not
	// required — the barrier is checked before payload validation would matter
	// for a non-resident room, and for a resident-but-retired room it never
	// reaches here. Use a trivial valid V1 update (empty doc encode).
	empty := crdt.EncodeStateAsUpdateV1(crdt.New(), nil)
	err = srv.BroadcastUpdate(context.Background(), "memroom", empty)
	assert.ErrorIs(t, err, ygws.ErrRoomDeleted)

	// The room is gone from memory.
	assert.Nil(t, srv.GetDoc("memroom"))
}

// TestDeleteRoom_IdempotentRepeatedDelete verifies that submitting the same
// deletion twice is a no-op the second time (returns nil, same generation,
// no double teardown).
func TestDeleteRoom_IdempotentRepeatedDelete(t *testing.T) {
	srv := ygws.NewServer()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	writeText(t, srv, "idem", "x")
	require.NoError(t, srv.DeleteRoom(context.Background(), "idem"))
	// Second call converges at the same generation and returns nil.
	require.NoError(t, srv.DeleteRoom(context.Background(), "idem"))

	dialExpectGone(t, ts, "idem")
}

// TestResetRoom_ReopensEmptyGeneration verifies that after DeleteRoom the name
// is barred, and after ResetRoom a fresh connection materialises an EMPTY
// room (no trace of the deleted content).
func TestResetRoom_ReopensEmptyGeneration(t *testing.T) {
	p := ygws.NewMemoryPersistence()
	srv := ygws.NewServerWithPersistence(p)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	writeText(t, srv, "gen", "secret")
	require.NoError(t, srv.DeleteRoom(context.Background(), "gen"))
	dialExpectGone(t, ts, "gen")

	require.NoError(t, srv.ResetRoom(context.Background(), "gen"))

	// Name is reusable; the new room starts empty.
	conn := dial(t, ts, "gen")
	doc := crdt.New()
	drainHandshake(t, conn, doc)
	require.Equal(t, "", doc.GetText("t").ToString(),
		"reset must reopen from an empty state, not resurrect deleted content")

	// The new generation is writable again.
	writeText(t, srv, "gen", "fresh")
	require.Equal(t, "fresh", roomContent(t, srv, "gen"))
}

// TestDeleteRoom_TombstoneSurvivesRestart verifies restart safety: the durable
// tombstone recorded by DeleteRoom survives the server process cycling (here
// simulated by attaching the same persistence adapter to a fresh Server), so a
// reconnecting peer cannot resurrect the deleted room.
func TestDeleteRoom_TombstoneSurvivesRestart(t *testing.T) {
	p := ygws.NewMemoryPersistence()

	// First server instance: create content, delete, shut down.
	{
		srv := ygws.NewServerWithPersistence(p)
		writeText(t, srv, "durable", "before")
		require.NoError(t, srv.DeleteRoom(context.Background(), "durable"))
		require.NoError(t, srv.Shutdown(context.Background()))
	}

	// Second server instance reusing the SAME durable store: the tombstone
	// must be recognised at load time and the room refused with 410.
	srv2 := ygws.NewServerWithPersistence(p)
	ts2 := httptest.NewServer(srv2)
	defer ts2.Close()

	dialExpectGone(t, ts2, "durable")

	// A direct Apply must also be refused (ErrRoomDeleted), proving the
	// tombstone blocks even non-connection creation paths after restart.
	err := srv2.Apply(context.Background(), "durable", func(d *crdt.Doc, transact func(func(*crdt.Transaction))) {})
	assert.ErrorIs(t, err, ygws.ErrRoomDeleted)
}

// TestDeleteRoom_ClusterConvergence verifies that a delete originated on one
// node propagates to every other reachable node via the relay's control plane,
// so a peer connecting to a different node for the same name also gets 410.
func TestDeleteRoom_ClusterConvergence(t *testing.T) {
	relay := cluster.NewMemRelay()
	defer func() { require.NoError(t, relay.Close()) }()

	srvA := ygws.NewServer()
	srvB := ygws.NewServer()
	require.NoError(t, srvA.AttachRelay(relay))
	require.NoError(t, srvB.AttachRelay(relay))

	tsA := httptest.NewServer(srvA)
	defer tsA.Close()
	tsB := httptest.NewServer(srvB)
	defer tsB.Close()

	// Materialise the room on both nodes so B has a resident instance to retire.
	connA := dial(t, tsA, "shared")
	drainHandshake(t, connA, crdt.New())
	connB := dial(t, tsB, "shared")
	drainHandshake(t, connB, crdt.New())

	// Delete on A.
	require.NoError(t, srvA.DeleteRoom(context.Background(), "shared"))

	// B must converge: its resident instance is torn down and the barrier
	// installed. Poll because MemRelay delivery is async.
	require.Eventually(t, func() bool {
		return srvB.GetDoc("shared") == nil
	}, 2*time.Second, 10*time.Millisecond, "node B must retire its instance after the cluster delete")

	// New connections to B get 410.
	dialExpectGone(t, tsB, "shared")

	// Reset on A must also propagate to B and reopen the name everywhere.
	require.NoError(t, srvA.ResetRoom(context.Background(), "shared"))
	require.Eventually(t, func() bool {
		// After reset, B's barrier should be lifted: a connection succeeds and
		// the room is empty.
		c, _, err := gws.DefaultDialer.Dial(wsURL(tsB, "shared"), nil)
		if err != nil {
			return false
		}
		_ = c.Close()
		return true
	}, 2*time.Second, 10*time.Millisecond, "node B must reopen the room after the cluster reset")
}

// failingGenStore wraps a real *MemoryPersistence adapter but fails DeleteRoom
// and ResetRoom, simulating a durable-storage outage. It records that the
// methods were called so the test can assert the server attempted the wipe.
// MemoryPersistence already satisfies both PersistenceAdapter and the
// generation-tombstone surface, so embedding it lets the wrapper override only
// the two failing methods.
type failingGenStore struct {
	*ygws.MemoryPersistence
	deleteErr    error
	resetErr     error
	deleteCalled int
	resetCalled  int
}

func (f *failingGenStore) DeleteRoom(ctx context.Context, room string) (uint64, error) {
	f.deleteCalled++
	if f.deleteErr != nil {
		return 0, f.deleteErr
	}
	return f.MemoryPersistence.DeleteRoom(ctx, room)
}

func (f *failingGenStore) ResetRoom(ctx context.Context, room string) (uint64, error) {
	f.resetCalled++
	if f.resetErr != nil {
		return 0, f.resetErr
	}
	return f.MemoryPersistence.ResetRoom(ctx, room)
}

// TestDeleteRoom_StorageFailurePreservesBarrier verifies that when the durable
// wipe fails, the call returns a decidable ErrDeletionStorage BUT the in-memory
// barrier is NOT lifted: the room stays barred and retrying converges.
func TestDeleteRoom_StorageFailurePreservesBarrier(t *testing.T) {
	real := ygws.NewMemoryPersistence()
	store := &failingGenStore{
		MemoryPersistence: real,
		deleteErr:         errors.New("simulated disk failure"),
	}
	srv := ygws.NewServerWithPersistence(store)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	writeText(t, srv, "flaky", "x")

	err := srv.DeleteRoom(context.Background(), "flaky")
	require.ErrorIs(t, err, ygws.ErrDeletionStorage, "storage failure must surface ErrDeletionStorage")
	assert.Equal(t, 1, store.deleteCalled, "DeleteRoom must have been attempted on the store")

	// Barrier stays up despite the storage failure.
	dialExpectGone(t, ts, "flaky")
	assert.Nil(t, srv.GetDoc("flaky"), "room instance must be retired even when storage wipe fails")

	// Retry now that storage is healthy converges (idempotent at the same gen).
	store.deleteErr = nil
	require.NoError(t, srv.DeleteRoom(context.Background(), "flaky"))
	// Still barred, now durably.
	dialExpectGone(t, ts, "flaky")
}

// TestDeleteRoom_PropagationFailurePreservesBarrier verifies that when cluster
// propagation fails, the local deletion is complete and the barrier stays; the
// decidable ErrDeletionPropagation is returned and a retry republishes.
//
// MemRelay cannot fail Publish, so we exercise this with a relay wrapper that
// rejects KindDelete publications while letting everything else through.
type deleteFailingRelay struct {
	cluster.Relay
	failDelete bool
	pubDelete  int
}

func (r *deleteFailingRelay) Publish(ctx context.Context, out cluster.Outbound) error {
	if out.Kind == cluster.KindDelete {
		r.pubDelete++
		if r.failDelete {
			return errors.New("simulated control-channel failure")
		}
	}
	return r.Relay.Publish(ctx, out)
}

func TestDeleteRoom_PropagationFailurePreservesBarrier(t *testing.T) {
	inner := cluster.NewMemRelay()
	defer func() { require.NoError(t, inner.Close()) }()
	relay := &deleteFailingRelay{Relay: inner, failDelete: true}

	srv := ygws.NewServer()
	require.NoError(t, srv.AttachRelay(relay))
	ts := httptest.NewServer(srv)
	defer ts.Close()

	writeText(t, srv, "netfail", "x")

	err := srv.DeleteRoom(context.Background(), "netfail")
	require.ErrorIs(t, err, ygws.ErrDeletionPropagation,
		"propagation failure must surface ErrDeletionPropagation")
	assert.Equal(t, 1, relay.pubDelete, "KindDelete must have been published (and failed)")

	// Local barrier is intact: room is gone, connections get 410.
	dialExpectGone(t, ts, "netfail")
	assert.Nil(t, srv.GetDoc("netfail"))

	// Retry with propagation healthy republishes and succeeds.
	relay.failDelete = false
	require.NoError(t, srv.DeleteRoom(context.Background(), "netfail"))
	assert.Equal(t, 2, relay.pubDelete, "retry must republish the same deletion")
	dialExpectGone(t, ts, "netfail")
}
