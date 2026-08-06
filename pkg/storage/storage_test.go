package storage

import (
	"dynamo/pkg/vclock"
	"testing"
)

func TestPutAndGet(t *testing.T) {
	s := CreateNewEmptyStorage()
	s.PutKey(1, 100, vclock.VectorClock{1: 1})

	items := s.GetKey(1)
	if len(items) != 1 || items[0].Value != 100 {
		t.Fatalf("expected value 100, got %v", items)
	}
}

func TestGetMissingKey(t *testing.T) {
	s := CreateNewEmptyStorage()
	if len(s.GetKey(99)) != 0 {
		t.Fatal("missing key should return no versions")
	}
}

func TestNewerVersionReplacesOlder(t *testing.T) {
	s := CreateNewEmptyStorage()
	s.PutKey(1, 100, vclock.VectorClock{1: 1})
	s.PutKey(1, 200, vclock.VectorClock{1: 2}) // descends the first

	items := s.GetKey(1)
	if len(items) != 1 || items[0].Value != 200 {
		t.Fatalf("expected single value 200, got %v", items)
	}
}

func TestOlderVersionIgnored(t *testing.T) {
	s := CreateNewEmptyStorage()
	s.PutKey(1, 200, vclock.VectorClock{1: 2})
	s.PutKey(1, 100, vclock.VectorClock{1: 1}) // older, should be dropped

	items := s.GetKey(1)
	if len(items) != 1 || items[0].Value != 200 {
		t.Fatalf("expected single value 200, got %v", items)
	}
}

func TestConcurrentWritesCreateSiblings(t *testing.T) {
	s := CreateNewEmptyStorage()
	s.PutKey(1, 100, vclock.VectorClock{1: 1})
	s.PutKey(1, 200, vclock.VectorClock{2: 1}) // concurrent -> sibling

	items := s.GetKey(1)
	if len(items) != 2 {
		t.Fatalf("expected 2 siblings, got %d: %v", len(items), items)
	}
}

func TestSiblingThenDescendantCollapses(t *testing.T) {
	s := CreateNewEmptyStorage()
	s.PutKey(1, 100, vclock.VectorClock{1: 1})
	s.PutKey(1, 200, vclock.VectorClock{2: 1})       // sibling
	s.PutKey(1, 300, vclock.VectorClock{1: 1, 2: 2}) // descends both

	items := s.GetKey(1)
	if len(items) != 1 || items[0].Value != 300 {
		t.Fatalf("expected reconciled single value 300, got %v", items)
	}
}

func TestAllItems(t *testing.T) {
	s := CreateNewEmptyStorage()
	s.PutKey(1, 10, vclock.VectorClock{1: 1})
	s.PutKey(2, 20, vclock.VectorClock{1: 1})
	if len(s.AllItems()) != 2 {
		t.Fatalf("expected 2 items across keys, got %d", len(s.AllItems()))
	}
}

func TestAddHandoffItem(t *testing.T) {
	s := CreateNewEmptyStorage()
	s.AddHandoffItem(5, 1, 100, vclock.VectorClock{1: 1})
	if s.HintCount() != 1 {
		t.Fatalf("expected 1 hint, got %d", s.HintCount())
	}
}

func TestDrainHintsReturnsOnlyMatching(t *testing.T) {
	s := CreateNewEmptyStorage()
	s.AddHandoffItem(5, 1, 100, vclock.VectorClock{1: 1})
	s.AddHandoffItem(6, 2, 200, vclock.VectorClock{1: 1})

	drained := s.DrainHints(5)
	if len(drained) != 1 || drained[0].BelongsToServer != 5 {
		t.Fatalf("expected 1 hint for server 5, got %v", drained)
	}
	if s.HintCount() != 1 {
		t.Fatalf("expected 1 hint remaining, got %d", s.HintCount())
	}
}

func TestDrainHintsIsAtomic(t *testing.T) {
	s := CreateNewEmptyStorage()
	s.AddHandoffItem(5, 1, 100, vclock.VectorClock{1: 1})
	s.DrainHints(5)
	if len(s.DrainHints(5)) != 0 {
		t.Fatal("second drain should return nothing (first removed them)")
	}
}

func TestPutReturnsStoredItem(t *testing.T) {
	s := CreateNewEmptyStorage()
	item := s.PutKey(7, 42, vclock.VectorClock{1: 1})
	if item.Key != 7 || item.Value != 42 {
		t.Fatalf("unexpected returned item: %v", item)
	}
}

func TestMultipleKeysIndependent(t *testing.T) {
	s := CreateNewEmptyStorage()
	s.PutKey(1, 10, vclock.VectorClock{1: 1})
	s.PutKey(2, 20, vclock.VectorClock{1: 1})
	if s.GetKey(1)[0].Value != 10 || s.GetKey(2)[0].Value != 20 {
		t.Fatal("keys should not interfere")
	}
}
