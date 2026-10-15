package cluster

import (
	"encoding/binary"
	"errors"
)

// ErrInvalidLifecycleEvent is returned by DecodeLifecycleEvent when the payload
// is not an 8-byte big-endian generation.
var ErrInvalidLifecycleEvent = errors.New("cluster: invalid lifecycle event payload")

// EncodeLifecycleEvent serialises the generation carried by a KindDelete or
// KindReset event. The payload is exactly 8 bytes: the generation as a
// big-endian uint64. The generation is the cluster-wide identity of the
// operation: a repeated delete at the same generation is the SAME deletion and
// must be applied idempotently, an event at a lower generation than one
// already observed is stale, and generations only move forward.
func EncodeLifecycleEvent(generation uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, generation)
	return b
}

// DecodeLifecycleEvent is EncodeLifecycleEvent's inverse.
func DecodeLifecycleEvent(data []byte) (uint64, error) {
	if len(data) != 8 {
		return 0, ErrInvalidLifecycleEvent
	}
	return binary.BigEndian.Uint64(data), nil
}

// IsLifecycle reports whether kind is a control-plane room lifecycle event.
func IsLifecycle(kind Kind) bool {
	return kind == KindDelete || kind == KindReset
}
