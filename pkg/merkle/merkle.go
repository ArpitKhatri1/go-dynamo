// Package merkle builds a Merkle tree (a hash tree) over a node's data so two
// replicas can find out WHICH keys differ without shipping their whole dataset
// to each other. This is the "anti-entropy" mechanism from the Dynamo paper.
//
// How it works:
//   - Each key/value becomes a leaf, hashed into a single number.
//   - Parent nodes hash their children together, all the way up to one "root".
//   - If two nodes have the same root hash, their data is identical -> nothing
//     to do. If the roots differ, at least one key differs.
//
// This beginner version keeps a flat map of leaf hashes (key -> hash) and folds
// them into a single root. Comparing roots tells us "are we in sync?", and
// comparing the per-key leaf hashes tells us "exactly which keys differ?".
package merkle

import (
	"sort"

	"github.com/cespare/xxhash/v2"
)

// Tree holds the per-key leaf hashes and the folded root hash.
type Tree struct {
	leaves map[int]uint64 // key -> hash(value + clock)
	root   uint64
}

// Leaf is a single key and the hash of its (possibly multi-version) value.
type Leaf struct {
	Key  int
	Hash uint64
}

// hashInts folds a list of numbers into one hash in a stable, order-independent
// way by hashing their concatenated bytes.
func hashUint64(values ...uint64) uint64 {
	d := xxhash.New()
	buf := make([]byte, 8)
	for _, v := range values {
		for i := 0; i < 8; i++ {
			buf[i] = byte(v >> (8 * i))
		}
		_, _ = d.Write(buf)
	}
	return d.Sum64()
}

// LeafHash computes the hash of a single value version. Callers pass the value
// and each (serverId, counter) pair of its vector clock so that two nodes
// storing the same value+clock produce the same leaf hash.
func LeafHash(value int, clockPairs [][2]int) uint64 {
	nums := []uint64{uint64(value)}
	// sort clock pairs so map iteration order never changes the hash
	sort.Slice(clockPairs, func(i, j int) bool {
		return clockPairs[i][0] < clockPairs[j][0]
	})
	for _, p := range clockPairs {
		nums = append(nums, uint64(p[0]), uint64(p[1]))
	}
	return hashUint64(nums...)
}

// CombineLeafHashes folds several sibling-version hashes for one key into a
// single leaf hash, independent of the order the siblings arrive in.
func CombineLeafHashes(hashes []uint64) uint64 {
	sorted := append([]uint64(nil), hashes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return hashUint64(sorted...)
}

// New builds a tree from per-key leaf hashes. When a key has several sibling
// versions the caller should combine them into one leaf hash first.
func New(leaves map[int]uint64) *Tree {
	t := &Tree{leaves: leaves}
	t.root = t.computeRoot()
	return t
}

// computeRoot folds all leaves (sorted by key for determinism) into a single
// root hash.
func (t *Tree) computeRoot() uint64 {
	if len(t.leaves) == 0 {
		return 0
	}
	keys := make([]int, 0, len(t.leaves))
	for k := range t.leaves {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	nums := make([]uint64, 0, len(keys)*2)
	for _, k := range keys {
		nums = append(nums, uint64(k), t.leaves[k])
	}
	return hashUint64(nums...)
}

// Root returns the root hash. Equal roots mean identical data.
func (t *Tree) Root() uint64 {
	return t.root
}

// Leaves returns a copy of the per-key leaf hashes.
func (t *Tree) Leaves() map[int]uint64 {
	out := make(map[int]uint64, len(t.leaves))
	for k, v := range t.leaves {
		out[k] = v
	}
	return out
}

// Diff returns the set of keys that differ between this tree and another. A key
// differs when it is missing on one side or its leaf hash is different. This is
// what anti-entropy uses to decide which keys to exchange.
func (t *Tree) Diff(other *Tree) []int {
	differing := map[int]struct{}{}

	for k, h := range t.leaves {
		if oh, ok := other.leaves[k]; !ok || oh != h {
			differing[k] = struct{}{}
		}
	}
	for k := range other.leaves {
		if _, ok := t.leaves[k]; !ok {
			differing[k] = struct{}{}
		}
	}

	out := make([]int, 0, len(differing))
	for k := range differing {
		out = append(out, k)
	}
	sort.Ints(out)
	return out
}
