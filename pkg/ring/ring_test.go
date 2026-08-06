package ring

import (
	"reflect"
	"testing"
)

// helper: build a ring where each entry is one virtual node
func buildRing(entries map[uint64]int) *ConsistentHashingRing {
	hashIds := make([]uint64, 0, len(entries))
	for h := range entries {
		hashIds = append(hashIds, h)
	}
	return NewConsistentHashRing(entries, hashIds)
}

func TestGetNextServerId(t *testing.T) {
	r := buildRing(map[uint64]int{100: 1, 200: 2, 300: 3})
	if got := r.GetNextServerId(50); got != 1 {
		t.Fatalf("expected server 1, got %d", got)
	}
	if got := r.GetNextServerId(150); got != 2 {
		t.Fatalf("expected server 2, got %d", got)
	}
}

func TestGetNextServerIdWrapsAround(t *testing.T) {
	r := buildRing(map[uint64]int{100: 1, 200: 2, 300: 3})
	if got := r.GetNextServerId(999); got != 1 {
		t.Fatalf("expected wraparound to server 1, got %d", got)
	}
}

func TestInsertServerKeepsRingSorted(t *testing.T) {
	r := buildRing(map[uint64]int{100: 1, 300: 3})
	r.InsertServer(200, 2)
	if got := r.GetNextServerId(150); got != 2 {
		t.Fatalf("expected inserted server 2, got %d", got)
	}
}

func TestGetMembers(t *testing.T) {
	r := buildRing(map[uint64]int{100: 1, 200: 2})
	if len(r.GetMembers()) != 2 {
		t.Fatalf("expected 2 members, got %d", len(r.GetMembers()))
	}
}

func TestPreferenceListDistinctServers(t *testing.T) {
	r := buildRing(map[uint64]int{100: 1, 200: 2, 300: 3, 400: 4, 500: 5})
	pref := r.GetPreferenceListForKey(50)
	seen := map[int]bool{}
	for _, id := range pref {
		if seen[id] {
			t.Fatalf("duplicate server %d in preference list %v", id, pref)
		}
		seen[id] = true
	}
}

func TestPreferenceListLength(t *testing.T) {
	// 6 servers, N=3 (+2 extras) => 5 distinct entries
	r := buildRing(map[uint64]int{100: 1, 200: 2, 300: 3, 400: 4, 500: 5, 600: 6})
	pref := r.GetPreferenceListForKey(50)
	if len(pref) != 5 {
		t.Fatalf("expected 5 preference nodes, got %d: %v", len(pref), pref)
	}
}

func TestPreferenceListSkipsRepeatedVirtualNodes(t *testing.T) {
	// server 1 has three virtual nodes; it must appear only once
	r := buildRing(map[uint64]int{100: 1, 110: 1, 120: 1, 200: 2, 300: 3})
	pref := r.GetPreferenceListForKey(50)
	count := 0
	for _, id := range pref {
		if id == 1 {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("server 1 should appear once, appeared %d times: %v", count, pref)
	}
}

func TestPreferenceListStartsAtKeyPosition(t *testing.T) {
	r := buildRing(map[uint64]int{100: 1, 200: 2, 300: 3, 400: 4, 500: 5})
	pref := r.GetPreferenceListForKey(150)
	if pref[0] != 2 {
		t.Fatalf("expected preference list to start at server 2, got %v", pref)
	}
}

func TestPreferenceListWrapsAround(t *testing.T) {
	r := buildRing(map[uint64]int{100: 1, 200: 2, 300: 3, 400: 4, 500: 5})
	pref := r.GetPreferenceListForKey(450)
	// starts at 500 (server 5) then wraps to 1,2,3
	want := []int{5, 1, 2, 3, 4}
	if !reflect.DeepEqual(pref, want) {
		t.Fatalf("expected %v, got %v", want, pref)
	}
}

func TestPreferenceListFewerServersThanN(t *testing.T) {
	r := buildRing(map[uint64]int{100: 1, 200: 2})
	pref := r.GetPreferenceListForKey(50)
	if len(pref) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(pref))
	}
}

func TestPreferenceListEmptyRing(t *testing.T) {
	r := buildRing(map[uint64]int{})
	if pref := r.GetPreferenceListForKey(50); pref != nil {
		t.Fatalf("expected nil for empty ring, got %v", pref)
	}
}

func TestPreferenceListDeterministic(t *testing.T) {
	r := buildRing(map[uint64]int{100: 1, 200: 2, 300: 3, 400: 4})
	if !reflect.DeepEqual(r.GetPreferenceListForKey(250), r.GetPreferenceListForKey(250)) {
		t.Fatal("preference list should be deterministic for the same key")
	}
}
