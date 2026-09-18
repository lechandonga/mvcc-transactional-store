package mvcc

import (
	"errors"
	"testing"
)

// TestGCOldReaderAcrossDeleteAndRecreate：最老快照持有期间发生
// 删除与重新写入，每次提交都会触发 GC；旧读者必须始终看到原值，
// 新读者必须依次看到“删除/新值”。
func TestGCOldReaderAcrossDeleteAndRecreate(t *testing.T) {
	s := openTestStore(t)
	seed(t, s, "k", "v0")

	old1 := mustBegin(t, s, true) // 快照停留在 v0
	if err := s.Update(func(tx *Txn) error { return tx.Delete("k") }); err != nil {
		t.Fatal(err)
	}
	old2 := mustBegin(t, s, true) // 快照停留在“已删除”
	if _, err := old2.Get("k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old2 should see deletion, got %v", err)
	}
	if v, err := old1.Get("k"); err != nil || string(v) != "v0" {
		t.Fatalf("old1 after delete+gc: %q %v", v, err)
	}

	if err := s.Update(func(tx *Txn) error { return tx.Put("k", []byte("v1")) }); err != nil {
		t.Fatal(err)
	}
	old3 := mustBegin(t, s, true) // 快照停留在 v1
	if v, err := old3.Get("k"); err != nil || string(v) != "v1" {
		t.Fatalf("old3 should see v1, got %q %v", v, err)
	}
	if _, err := old2.Get("k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old2 must keep seeing deletion after recreate, got %v", err)
	}
	if v, err := old1.Get("k"); err != nil || string(v) != "v0" {
		t.Fatalf("old1 after recreate+gc: %q %v", v, err)
	}

	// 全部旧快照结束后再多次提交，旧版本应被回收且行为依旧正确。
	old1.Rollback()
	old2.Rollback()
	old3.Rollback()
	if err := s.Update(func(tx *Txn) error { return tx.Put("k", []byte("v2")) }); err != nil {
		t.Fatal(err)
	}
	if err := s.Update(func(tx *Txn) error { return tx.Delete("k") }); err != nil {
		t.Fatal(err)
	}
	assertNotFound(t, s, "k")
	if err := s.Update(func(tx *Txn) error { return tx.Put("k", []byte("v3")) }); err != nil {
		t.Fatal(err)
	}
	assertValue(t, s, "k", "v3")

	// 直接检查内部版本链：无活跃长事务时只应剩一个版本。
	s.mu.RLock()
	n := 0
	for v := s.versions["k"]; v != nil; v = v.prev {
		n++
	}
	s.mu.RUnlock()
	if n != 1 {
		t.Fatalf("version chain not pruned: %d versions remain", n)
	}
}

// TestGCConflictAfterTombstonePrune：墓碑被 GC 整条移除后，
// 之后基于新快照的事务写该键不应收到伪冲突，删除已被正确遗忘。
func TestGCConflictAfterTombstonePrune(t *testing.T) {
	s := openTestStore(t)
	seed(t, s, "k", "v0")
	if err := s.Update(func(tx *Txn) error { return tx.Delete("k") }); err != nil {
		t.Fatal(err)
	}
	assertNotFound(t, s, "k")
	// 此刻无活跃事务，墓碑链应已被整条移除。
	s.mu.RLock()
	_, exists := s.versions["k"]
	s.mu.RUnlock()
	if exists {
		t.Fatal("tombstone chain should be fully pruned with no active txns")
	}
	if err := s.Update(func(tx *Txn) error { return tx.Put("k", []byte("revived")) }); err != nil {
		t.Fatalf("recreate after pruned tombstone: %v", err)
	}
	assertValue(t, s, "k", "revived")
}
