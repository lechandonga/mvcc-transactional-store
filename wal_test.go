package mvcc

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func reopen(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(Config{Dir: dir, SyncWrites: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	return s
}

func TestRecoveryCommittedDataSurvives(t *testing.T) {
	dir := t.TempDir()
	s := reopen(t, dir)
	must(t, s.Put([]byte("a"), []byte("1")))
	must(t, s.Put([]byte("b"), []byte("2")))
	must(t, s.Delete([]byte("a")))

	// 模拟异常中断：不调用 Close，直接丢弃句柄后重开。
	s2 := reopen(t, dir)
	defer s2.Close()
	if _, err := s2.Get([]byte("a")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("a should stay deleted, err = %v", err)
	}
	v, err := s2.Get([]byte("b"))
	if err != nil || string(v) != "2" {
		t.Fatalf("b = %q, %v; want 2", v, err)
	}
}

func TestRecoveryUncommittedTxnInvisible(t *testing.T) {
	dir := t.TempDir()
	s := reopen(t, dir)
	must(t, s.Put([]byte("stable"), []byte("ok")))
	tx, _ := s.Begin()
	must(t, tx.Put([]byte("dirty"), []byte("x")))
	must(t, tx.Put([]byte("stable"), []byte("overwritten")))
	// 进程“崩溃”：事务未提交，句柄直接丢弃。

	s2 := reopen(t, dir)
	defer s2.Close()
	if _, err := s2.Get([]byte("dirty")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("uncommitted key visible: %v", err)
	}
	v, err := s2.Get([]byte("stable"))
	if err != nil || string(v) != "ok" {
		t.Fatalf("stable = %q, %v; want ok", v, err)
	}
}

func TestRecoveryIdempotent(t *testing.T) {
	dir := t.TempDir()
	s := reopen(t, dir)
	for i := 0; i < 50; i++ {
		must(t, s.Put([]byte(fmt.Sprintf("k%02d", i)), []byte(fmt.Sprintf("v%d", i))))
	}
	must(t, s.Close())

	// 连续恢复多次，结果必须稳定一致。
	var snapshots []string
	for i := 0; i < 3; i++ {
		si := reopen(t, dir)
		it, err := si.Begin()
		if err != nil {
			t.Fatal(err)
		}
		var state string
		scan := it.Scan(nil, nil)
		for scan.Valid() {
			state += fmt.Sprintf("%s=%s;", scan.Key(), scan.Value())
			scan.Next()
		}
		snapshots = append(snapshots, state)
		must(t, si.Close())
	}
	for i := 1; i < len(snapshots); i++ {
		if snapshots[i] != snapshots[0] {
			t.Fatalf("recovery %d differs from recovery 0", i)
		}
	}
}

func TestRecoveryTruncatesCorruptTail(t *testing.T) {
	dir := t.TempDir()
	s := reopen(t, dir)
	must(t, s.Put([]byte("good"), []byte("1")))
	must(t, s.Close())

	// 在 WAL 尾部追加垃圾字节，模拟崩溃时的半条记录。
	walPath := filepath.Join(dir, "store.wal")
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xde, 0xad, 0xbe, 0xef, 0x01}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	s2 := reopen(t, dir)
	v, err := s2.Get([]byte("good"))
	if err != nil || string(v) != "1" {
		t.Fatalf("good = %q, %v", v, err)
	}
	// 截断后应能继续正常写入并再次恢复。
	must(t, s2.Put([]byte("after"), []byte("2")))
	must(t, s2.Close())

	s3 := reopen(t, dir)
	defer s3.Close()
	v, err = s3.Get([]byte("after"))
	if err != nil || string(v) != "2" {
		t.Fatalf("after = %q, %v", v, err)
	}
}

func TestVersionCountBounded(t *testing.T) {
	s := openMem(t, func(c *Config) { c.MaxVersionsPerKey = 4 })
	for i := 0; i < 100; i++ {
		must(t, s.Put([]byte("hot"), []byte(fmt.Sprintf("%d", i))))
	}
	s.mu.RLock()
	n := len(s.data["hot"])
	s.mu.RUnlock()
	if n > 4 {
		t.Fatalf("versions kept = %d; want <= 4", n)
	}
	// 最新值仍然正确。
	v, err := s.Get([]byte("hot"))
	if err != nil || string(v) != "99" {
		t.Fatalf("hot = %q, %v", v, err)
	}
}
