// Room lifecycle control plane: permanent deletes and explicit new
// generations (cluster.KindDelete / cluster.KindReset) are published
// synchronously to one cluster-wide control channel and applied by a single
// goroutine per node, rather than travelling the per-room pub/sub or streams
// data plane. A lifecycle event must reach nodes that never activated the room
// (they may hold a durable tombstone, a room worker, or stream readers) and
// must never be coalesced like a KindSync blob.

package redis

import (
	"context"
	"errors"

	"github.com/reearth/ygo/cluster"
)

// runControl is the single consumer of controlCh. It serialises lifecycle
// events cluster-wide-in-order for this node (distinct rooms may be handled by
// the Sink concurrently elsewhere; same-room delete/reset ordering matters and
// is preserved by this one goroutine). Registered on r.wg in Start, so Close
// joins it and a lifecycle event cannot fire after Close returns.
func (r *Relay) runControl(ctx context.Context) {
	defer r.wg.Done()
	for {
		select {
		case <-r.done:
			return
		case <-ctx.Done():
			return
		case in := <-r.controlCh:
			r.handleControl(ctx, in)
		}
	}
}

// handleControl applies one lifecycle event: first the Sink (the websocket
// provider establishes the in-memory barrier, tears the room down, records
// the durable generation tombstone — and never republishes the event), then
// this relay's own durable tier (room stream keys) for a delete. Sink errors
// are logged but do not stop delivery: the origin already durably
// tombstoned, and a retried event converges idempotently by generation.
func (r *Relay) handleControl(ctx context.Context, in cluster.Inbound) {
	if r.closed.Load() {
		return
	}
	if err := r.sink.Inject(ctx, in); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		r.log.Warn("cluster/redis: lifecycle sink.Inject failed",
			"room", in.Room, "kind", in.Kind, "err", err)
	}
	// Reception-side convergence for the durable tier: remove the room's
	// streams even when the origin's own DEL failed. Idempotent.
	if in.Kind == cluster.KindDelete && r.scfg.transport.usesStreams() {
		if err := r.deleteRoomStreams(ctx, in.Room); err != nil && !errors.Is(err, context.Canceled) {
			r.log.Warn("cluster/redis: delete room streams failed",
				"room", in.Room, "err", err)
		}
	}
}

// publishLifecycle announces one lifecycle event on the cluster-wide control
// channel synchronously, so Publish's caller gets a decidable error when the
// announcement does not reach the broker (the provider keeps the deletion
// barrier in place and the admin retries). For KindDelete in Streams/Both
// mode the room's sync and awareness stream keys are removed as well, so a
// restarted reader cannot replay deleted content after the in-memory barrier
// is gone.
func (r *Relay) publishLifecycle(ctx context.Context, out cluster.Outbound) error {
	if r.scfg.transport.usesStreams() && out.Kind == cluster.KindDelete {
		if err := r.deleteRoomStreams(ctx, out.Room); err != nil {
			return err
		}
	}
	body := encodeOutbound(r.nodeID, out)
	return r.client.Publish(ctx, r.controlChannel(), body).Err()
}

// deleteRoomStreams removes a room's sync and awareness stream keys and this
// relay's per-key bookkeeping for them. Idempotent: DEL on missing keys is a
// no-op, and stale seq/cursor entries are bounded-map leftovers that the
// ordinary eviction sweeps also reclaim.
func (r *Relay) deleteRoomStreams(ctx context.Context, room string) error {
	syncKey := r.scfg.syncKey(room)
	awKey := r.scfg.awKey(room)
	if err := r.client.Del(ctx, syncKey, awKey).Err(); err != nil {
		return err
	}
	r.streamMu.Lock()
	delete(r.streamRooms, room)
	delete(r.seqs, syncKey)
	delete(r.seqs, awKey)
	delete(r.cursors, syncKey)
	r.streamMu.Unlock()
	return nil
}
