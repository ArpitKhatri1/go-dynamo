package merkle

import "testing"

func TestEmptyTreeRootIsZero(t *testing.T) {
	if New(map[int]uint64{}).Root() != 0 {
		t.Fatal("empty tree root should be 0")
	}
}

func TestIdenticalDataSameRoot(t *testing.T) {
	a := New(map[int]uint64{1: 111, 2: 222})
	b := New(map[int]uint64{1: 111, 2: 222})
	if a.Root() != b.Root() {
		t.Fatal("identical leaf sets must produce the same root")
	}
}

func TestDifferentDataDifferentRoot(t *testing.T) {
	a := New(map[int]uint64{1: 111, 2: 222})
	b := New(map[int]uint64{1: 111, 2: 999})
	if a.Root() == b.Root() {
		t.Fatal("different leaves must produce different roots")
	}
}

func TestRootIndependentOfInsertionOrder(t *testing.T) {
	a := New(map[int]uint64{1: 111, 2: 222, 3: 333})
	b := New(map[int]uint64{3: 333, 1: 111, 2: 222})
	if a.Root() != b.Root() {
		t.Fatal("root must not depend on map iteration order")
	}
}

func TestDiffFindsChangedKey(t *testing.T) {
	a := New(map[int]uint64{1: 111, 2: 222})
	b := New(map[int]uint64{1: 111, 2: 999})
	diff := a.Diff(b)
	if len(diff) != 1 || diff[0] != 2 {
		t.Fatalf("expected diff [2], got %v", diff)
	}
}

func TestDiffFindsMissingKey(t *testing.T) {
	a := New(map[int]uint64{1: 111, 2: 222})
	b := New(map[int]uint64{1: 111})
	diff := a.Diff(b)
	if len(diff) != 1 || diff[0] != 2 {
		t.Fatalf("expected diff [2], got %v", diff)
	}
}

func TestDiffEmptyWhenEqual(t *testing.T) {
	a := New(map[int]uint64{1: 111, 2: 222})
	b := New(map[int]uint64{1: 111, 2: 222})
	if len(a.Diff(b)) != 0 {
		t.Fatal("equal trees should have empty diff")
	}
}

func TestDiffIsSymmetric(t *testing.T) {
	a := New(map[int]uint64{1: 111, 2: 222})
	b := New(map[int]uint64{1: 111, 3: 333})
	if len(a.Diff(b)) != len(b.Diff(a)) {
		t.Fatal("diff should be symmetric in size")
	}
}

func TestLeafHashDeterministic(t *testing.T) {
	h1 := LeafHash(100, [][2]int{{1, 2}, {2, 1}})
	h2 := LeafHash(100, [][2]int{{2, 1}, {1, 2}}) // different order, same content
	if h1 != h2 {
		t.Fatal("leaf hash must be independent of clock-pair order")
	}
}

func TestLeafHashChangesWithValue(t *testing.T) {
	if LeafHash(100, nil) == LeafHash(101, nil) {
		t.Fatal("different values should hash differently")
	}
}

func TestCombineLeafHashesOrderIndependent(t *testing.T) {
	if CombineLeafHashes([]uint64{1, 2, 3}) != CombineLeafHashes([]uint64{3, 2, 1}) {
		t.Fatal("combine should be order independent")
	}
}

func TestLeavesReturnsCopy(t *testing.T) {
	tree := New(map[int]uint64{1: 111})
	leaves := tree.Leaves()
	leaves[1] = 999
	if tree.Leaves()[1] != 111 {
		t.Fatal("Leaves() must return a copy, not the internal map")
	}
}
