// Package storage is the in-memory, versioned key-value store that lives on
// every node. Unlike a normal map, a single key can hold MORE THAN ONE value at
// the same time: when two writes happen concurrently (their vector clocks
// conflict), we keep both as "siblings" instead of throwing one away. This is
// how Dynamo stays available for writes and resolves conflicts later.
package storage

import (
	"dynamo/pkg/vclock"
	"sync"
)

type Storage struct {
	storeLock  sync.RWMutex
	storageMap map[int][]StorageItem // key -> one or more versioned values (siblings)

	Handoffstorage *HandoffStorage
}

// HandoffStorage holds writes that were meant for a node that was down. A
// stand-in node keeps them here (a "hint") and replays them once the intended
// owner recovers. See hinted handoff.
type HandoffStorage struct {
	handoffLock sync.RWMutex
	items       []HandoffItem
}

type HandoffItem struct {
	BelongsToServer int // serverId this write really belongs to
	Item            StorageItem
}

// StorageItem is one version of a value: the value itself plus the vector clock
// describing its causal history.
type StorageItem struct {
	Key   int
	Value int
	Clock vclock.VectorClock
}

func CreateNewEmptyHandoffStorage() *HandoffStorage {
	return &HandoffStorage{
		items: []HandoffItem{},
	}
}

func CreateNewEmptyStorage() *Storage {
	handoffStorage := CreateNewEmptyHandoffStorage()
	return &Storage{
		storageMap:     make(map[int][]StorageItem),
		Handoffstorage: handoffStorage,
	}
}

// PutKey stores a new version of a key and reconciles it against existing
// versions using vector clocks:
//   - If the new version descends (is newer than) an existing sibling, the old
//     sibling is dropped.
//   - If the new version is an ancestor of an existing sibling, the write is a
//     no-op (we already have something newer).
//   - Otherwise the versions are concurrent and both are kept as siblings.
func (s *Storage) PutKey(key int, value int, clock vclock.VectorClock) StorageItem {
	s.storeLock.Lock()
	defer s.storeLock.Unlock()

	newItem := StorageItem{Key: key, Value: value, Clock: clock}
	existing := s.storageMap[key]

	kept := []StorageItem{}
	for _, old := range existing {
		if clock.Descends(old.Clock) {
			// new is newer than old -> drop old
			continue
		}
		if old.Clock.Descends(clock) {
			// we already store something newer -> keep everything, ignore new
			return old
		}
		// concurrent -> keep old as a sibling
		kept = append(kept, old)
	}

	kept = append(kept, newItem)
	s.storageMap[key] = kept
	return newItem
}

// GetKey returns every surviving version (sibling) of a key. An empty slice
// means the key is not present.
func (s *Storage) GetKey(key int) []StorageItem {
	s.storeLock.RLock()
	defer s.storeLock.RUnlock()

	return s.storageMap[key]
}

// AllItems returns a snapshot of every stored version across all keys. Used to
// build a Merkle tree for anti-entropy.
func (s *Storage) AllItems() []StorageItem {
	s.storeLock.RLock()
	defer s.storeLock.RUnlock()

	out := []StorageItem{}
	for _, versions := range s.storageMap {
		out = append(out, versions...)
	}
	return out
}

// AddHandoffItem records a write that belongs to intendedServer but is being
// temporarily stored here because that server was unreachable.
func (s *Storage) AddHandoffItem(intendedServer int, key int, value int, clock vclock.VectorClock) HandoffItem {
	s.Handoffstorage.handoffLock.Lock()
	defer s.Handoffstorage.handoffLock.Unlock()

	handoffItem := HandoffItem{
		BelongsToServer: intendedServer,
		Item:            StorageItem{Key: key, Value: value, Clock: clock},
	}

	s.Handoffstorage.items = append(s.Handoffstorage.items, handoffItem)
	return handoffItem
}

// DrainHints atomically removes and returns all hints destined for the given
// server. The caller then forwards them to the (now recovered) owner. Doing the
// remove and return in one locked step prevents the same hint being replayed
// twice by two concurrent handoff loops.
func (s *Storage) DrainHints(forServerId int) []HandoffItem {
	s.Handoffstorage.handoffLock.Lock()
	defer s.Handoffstorage.handoffLock.Unlock()

	drained := []HandoffItem{}
	remaining := []HandoffItem{}
	for _, hint := range s.Handoffstorage.items {
		if hint.BelongsToServer == forServerId {
			drained = append(drained, hint)
		} else {
			remaining = append(remaining, hint)
		}
	}
	s.Handoffstorage.items = remaining
	return drained
}

// HintCount returns how many hints are currently buffered (used in tests).
func (s *Storage) HintCount() int {
	s.Handoffstorage.handoffLock.RLock()
	defer s.Handoffstorage.handoffLock.RUnlock()
	return len(s.Handoffstorage.items)
}
