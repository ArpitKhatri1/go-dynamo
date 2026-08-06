package vclock

import "testing"

func TestNewIsEmpty(t *testing.T) {
	vc := New()
	if len(vc) != 0 {
		t.Fatalf("expected empty clock, got %v", vc)
	}
}

func TestIncrementCreatesEntry(t *testing.T) {
	vc := New()
	vc.Increment(1)
	if vc[1] != 1 {
		t.Fatalf("expected counter 1 for server 1, got %d", vc[1])
	}
}

func TestIncrementBumpsExisting(t *testing.T) {
	vc := New()
	vc.Increment(1)
	vc.Increment(1)
	vc.Increment(1)
	if vc[1] != 3 {
		t.Fatalf("expected counter 3, got %d", vc[1])
	}
}

func TestCopyIsIndependent(t *testing.T) {
	vc := VectorClock{1: 2}
	cp := vc.Copy()
	cp.Increment(1)
	if vc[1] != 2 {
		t.Fatalf("mutating copy changed original: %v", vc)
	}
	if cp[1] != 3 {
		t.Fatalf("copy did not increment: %v", cp)
	}
}

func TestDescendsWhenNewer(t *testing.T) {
	older := VectorClock{1: 1}
	newer := VectorClock{1: 2}
	if !newer.Descends(older) {
		t.Fatal("newer should descend older")
	}
	if older.Descends(newer) {
		t.Fatal("older should not descend newer")
	}
}

func TestDescendsEqualBothTrue(t *testing.T) {
	a := VectorClock{1: 1, 2: 2}
	b := VectorClock{1: 1, 2: 2}
	if !a.Descends(b) || !b.Descends(a) {
		t.Fatal("equal clocks should descend each other")
	}
}

func TestDescendsWithMissingEntry(t *testing.T) {
	// {1:1,2:1} descends {1:1} because the missing key counts as 0
	a := VectorClock{1: 1, 2: 1}
	b := VectorClock{1: 1}
	if !a.Descends(b) {
		t.Fatal("a should descend b")
	}
	if b.Descends(a) {
		t.Fatal("b should not descend a")
	}
}

func TestConcurrentClocks(t *testing.T) {
	a := VectorClock{1: 1}
	b := VectorClock{2: 1}
	if !Concurrent(a, b) {
		t.Fatal("clocks from different servers should be concurrent")
	}
}

func TestNotConcurrentWhenOrdered(t *testing.T) {
	a := VectorClock{1: 1}
	b := VectorClock{1: 2}
	if Concurrent(a, b) {
		t.Fatal("ordered clocks are not concurrent")
	}
}

func TestMergeTakesMaximums(t *testing.T) {
	a := VectorClock{1: 3, 2: 1}
	b := VectorClock{1: 1, 2: 5, 3: 2}
	m := Merge(a, b)
	if m[1] != 3 || m[2] != 5 || m[3] != 2 {
		t.Fatalf("unexpected merge result: %v", m)
	}
}

func TestMergeDominatesBoth(t *testing.T) {
	a := VectorClock{1: 1}
	b := VectorClock{2: 1}
	m := Merge(a, b)
	if !m.Descends(a) || !m.Descends(b) {
		t.Fatal("merge should descend both inputs")
	}
}

func TestEqual(t *testing.T) {
	if !(VectorClock{1: 1}).Equal(VectorClock{1: 1}) {
		t.Fatal("identical clocks should be equal")
	}
	if (VectorClock{1: 1}).Equal(VectorClock{1: 2}) {
		t.Fatal("different clocks should not be equal")
	}
	if (VectorClock{1: 1}).Equal(VectorClock{1: 1, 2: 1}) {
		t.Fatal("different-size clocks should not be equal")
	}
}
