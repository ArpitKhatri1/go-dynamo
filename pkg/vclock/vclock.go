// Package vclock implements vector clocks, the mechanism Dynamo uses to track
// causality between different versions of the same key.
//
// A vector clock is just a map from a server id to a counter. Every time a
// server coordinates a write for a key, it bumps its own counter. By comparing
// two clocks we can answer one question: "did version A happen before version
// B, or did they happen at the same time (concurrently)?". If they are
// concurrent, neither is newer, so both are kept as "siblings" and the
// application decides how to merge them.
package vclock

// VectorClock maps a serverId to the number of writes that server has
// coordinated for a key. Example: {1: 3, 2: 1} means server 1 wrote 3 times and
// server 2 wrote once.
type VectorClock map[int]int

// New returns an empty vector clock.
func New() VectorClock {
	return VectorClock{}
}

// Increment bumps the counter for the given server. The coordinator of a write
// calls this once before storing/replicating the new version.
func (vc VectorClock) Increment(serverId int) {
	vc[serverId]++
}

// Copy returns an independent copy so callers can mutate it without touching
// the original (maps in Go are references).
func (vc VectorClock) Copy() VectorClock {
	out := make(VectorClock, len(vc))
	for serverId, counter := range vc {
		out[serverId] = counter
	}
	return out
}

// Descends reports whether vc is a descendant of (i.e. happened after or is
// equal to) other. This is true when vc has a counter >= other's for every
// server present in other. If vc descends other, then other is an older
// ancestor and can be safely discarded.
func (vc VectorClock) Descends(other VectorClock) bool {
	for serverId, otherCounter := range other {
		if vc[serverId] < otherCounter {
			return false
		}
	}
	return true
}

// Equal reports whether two clocks are identical.
func (vc VectorClock) Equal(other VectorClock) bool {
	if len(vc) != len(other) {
		return false
	}
	for serverId, counter := range vc {
		if other[serverId] != counter {
			return false
		}
	}
	return true
}

// Concurrent reports whether a and b conflict: neither descends the other. Two
// concurrent versions are siblings that must both be kept until reconciled.
func Concurrent(a, b VectorClock) bool {
	return !a.Descends(b) && !b.Descends(a)
}

// Merge returns a new clock that takes the maximum counter for every server
// seen in either clock. This is used when reconciling siblings into a single
// version that dominates both.
func Merge(a, b VectorClock) VectorClock {
	out := a.Copy()
	for serverId, counter := range b {
		if counter > out[serverId] {
			out[serverId] = counter
		}
	}
	return out
}
