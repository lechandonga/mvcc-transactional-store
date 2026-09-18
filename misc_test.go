package mvcc

import (
	"errors"
	"testing"
)

func TestClosedStoreRejectsOps(t *testing.T) {
	s := openTestStore(t)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("double close: %v", err)
	}
	if _, err := s.Begin(true); !errors.Is(err, ErrClosed) {
		t.Fatalf("begin after close: %v", err)
	}
	if err := s.Update(func(tx *Txn) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("update after close: %v", err)
	}
}

func TestEmptyCommitAndDeleteAbsentKey(t *testing.T) {
	s := openTestStore(t)
	// 空写集合提交成功且不产生 WAL 记录。
	if err := s.Update(func(tx *Txn) error { return nil }); err != nil {
		t.Fatal(err)
	}
	// 删除不存在的键：提交成功；之后依然 NotFound。
	if err := s.Update(func(tx *Txn) error { return tx.Delete("ghost") }); err != nil {
		t.Fatalf("delete absent: %v", err)
	}
	assertNotFound(t, s, "ghost")
	// 墓碑 GC 后重建正常。
	if err := s.Update(func(tx *Txn) error { return tx.Put("ghost", []byte("boo")) }); err != nil {
		t.Fatal(err)
	}
	assertValue(t, s, "ghost", "boo")
}

func TestLargeValuesPreserved(t *testing.T) {
	s, err := Open(&Options{Dir: t.TempDir(), MaxLiveBytes: -1, MaxWALBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	big := make([]byte, 200_000)
	for i := range big {
		big[i] = byte(i % 251)
	}
	if err := s.Update(func(tx *Txn) error { return tx.Put("big", big) }); err != nil {
		t.Fatal(err)
	}
	got := getValueBytes(t, s, "big")
	if len(got) != len(big) {
		t.Fatalf("len mismatch %d vs %d", len(got), len(big))
	}
	for i := range big {
		if got[i] != big[i] {
			t.Fatalf("byte %d mismatch after recovery cycle", i)
		}
	}
}

func getValueBytes(t *testing.T, s *Store, k string) []byte {
	t.Helper()
	var out []byte
	if err := s.View(func(tx *Txn) error {
		v, err := tx.Get(k)
		if err != nil {
			return err
		}
		out = v
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}
