package mvcc

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// TestSnapshotReadStable：读事务开始后，其他事务的并发提交不得改变其可见视图。
func TestSnapshotReadStable(t *testing.T) {
	s := openTestStore(t)
	for _, k := range []string{"a", "b", "c"} {
		if err := s.Update(func(tx *Txn) error { return tx.Put(k, []byte("v0")) }); err != nil {
			t.Fatal(err)
		}
	}

	reader, err := s.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Rollback()

	// 读事务开始之后：修改 b、新增 d、删除 c。
	if err := s.Update(func(tx *Txn) error {
		if err := tx.Put("b", []byte("v1")); err != nil {
			return err
		}
		if err := tx.Put("d", []byte("v0")); err != nil {
			return err
		}
		return tx.Delete("c")
	}); err != nil {
		t.Fatal(err)
	}

	if v, err := reader.Get("a"); err != nil || string(v) != "v0" {
		t.Fatalf("a: %q %v", v, err)
	}
	if v, err := reader.Get("b"); err != nil || string(v) != "v0" {
		t.Fatalf("b snapshot should stay v0, got %q err %v", v, err)
	}
	if _, err := reader.Get("d"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("d must be invisible to old snapshot, got %v", err)
	}
	// c 在旧快照中必须依然存在（快照隔离）；删除只影响新快照。
	if v, err := reader.Get("c"); err != nil || string(v) != "v0" {
		t.Fatalf("c should remain visible in old snapshot, got %q err %v", v, err)
	}
}

// TestSnapshotDeleteVisibility：删除只影响删除之后的新快照。
func TestSnapshotDeleteVisibility(t *testing.T) {
	s := openTestStore(t)
	if err := s.Update(func(tx *Txn) error { return tx.Put("k", []byte("v0")) }); err != nil {
		t.Fatal(err)
	}
	old := mustBegin(t, s, true)
	if err := s.Update(func(tx *Txn) error { return tx.Delete("k") }); err != nil {
		t.Fatal(err)
	}
	new := mustBegin(t, s, true)

	if v, err := old.Get("k"); err != nil || string(v) != "v0" {
		t.Fatalf("old snapshot must still see deleted key, got %q %v", v, err)
	}
	if _, err := new.Get("k"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("new snapshot must see deletion, got %v", err)
	}
	old.Rollback()
	new.Rollback()
}

// TestScanSnapshotSemantics：扫描结果对快照可见，且扫描期间的并发写入不影响完整性。
func TestScanSnapshotSemantics(t *testing.T) {
	s := openTestStore(t)
	seed := map[string]string{"a1": "1", "a2": "2", "b1": "3", "c1": "4"}
	if err := s.Update(func(tx *Txn) error {
		for k, v := range seed {
			if err := tx.Put(k, []byte(v)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	tx := mustBegin(t, s, true)

	// 在扫描进行的同时持续提交写入（多次扫描交替触发并发提交）。
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		i := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			i++
			_ = s.Update(func(wt *Txn) error {
				if err := wt.Put(fmt.Sprintf("a9-%d", i), []byte("x")); err != nil {
					return err
				}
				return wt.Put("a1", []byte("changed"))
			})
			time.Sleep(time.Microsecond)
		}
	}()

	// 同一快照重复扫描多次，结果必须完全一致。
	var first []KeyValue
	for round := 0; round < 20; round++ {
		got, err := tx.Scan("a", "c") // a1,a2,b1（不含 c 起头的键）
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
		if first == nil {
			first = got
			if len(first) != 3 {
				t.Fatalf("want 3 stable rows, got %d: %+v", len(first), first)
			}
			if first[0].Key != "a1" || string(first[0].Value) != "1" ||
				first[1].Key != "a2" || first[2].Key != "b1" {
				t.Fatalf("unexpected initial scan: %+v", first)
			}
		} else if !scanEqual(first, got) {
			t.Fatalf("scan result changed under concurrent writes:\nfirst=%+v\ngot  =%+v", first, got)
		}
	}
	close(stop)
	<-done
	tx.Rollback()
}

// TestScanRanges：边界（空 start/end、空库、空结果）行为正确。
func TestScanRanges(t *testing.T) {
	s := openTestStore(t)
	empty, err := s.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := empty.Scan("", ""); err != nil || len(rows) != 0 {
		t.Fatalf("empty store scan: %+v %v", rows, err)
	}
	empty.Rollback()

	if err := s.Update(func(tx *Txn) error {
		for _, k := range []string{"a", "b", "c", "d"} {
			if err := tx.Put(k, []byte(k+"v")); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		start, end string
		wantKeys   []string
	}{
		{"", "", []string{"a", "b", "c", "d"}},
		{"b", "d", []string{"b", "c"}},
		{"b", "", []string{"b", "c", "d"}},
		{"x", "", []string{}},
		{"", "b", []string{"a"}},
		{"a1", "c", []string{"b"}}, // "a" < "a1"，故从 b 开始
	}
	for _, tc := range cases {
		var keys []string
		if err := s.View(func(tx *Txn) error {
			rows, err := tx.Scan(tc.start, tc.end)
			if err != nil {
				return err
			}
			for _, r := range rows {
				keys = append(keys, r.Key)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if !stringsEqual(keys, tc.wantKeys) {
			t.Fatalf("scan [%q,%q) = %v, want %v", tc.start, tc.end, keys, tc.wantKeys)
		}
	}

	// 事务内写入与删除应反映到自己的扫描中。
	if err := s.Update(func(tx *Txn) error {
		if err := tx.Put("aa", []byte("z")); err != nil {
			return err
		}
		if err := tx.Delete("b"); err != nil {
			return err
		}
		rows, err := tx.Scan("", "")
		if err != nil {
			return err
		}
		var keys []string
		for _, r := range rows {
			keys = append(keys, r.Key)
		}
		want := []string{"a", "aa", "c", "d"}
		if !stringsEqual(keys, want) {
			return fmt.Errorf("self scan = %v, want %v", keys, want)
		}
		return nil
	}); err != nil {
		t.Fatalf("txn self scan: %v", err)
	}
}

func mustBegin(t *testing.T, s *Store, ro bool) *Txn {
	t.Helper()
	tx, err := s.Begin(ro)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	return tx
}

func scanEqual(a, b []KeyValue) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Key != b[i].Key || string(a[i].Value) != string(b[i].Value) {
			return false
		}
	}
	return true
}

func stringsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
