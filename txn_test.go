package mvcc

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func openMem(t *testing.T, mutate func(*Config)) *Store {
	t.Helper()
	cfg := Config{}
	if mutate != nil {
		mutate(&cfg)
	}
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestBasicPutGetDelete(t *testing.T) {
	s := openMem(t, nil)
	if err := s.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	v, err := s.Get([]byte("a"))
	if err != nil || string(v) != "1" {
		t.Fatalf("Get = %q, %v", v, err)
	}
	if err := s.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Get([]byte("a")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("after delete Get err = %v", err)
	}
}

func TestSnapshotIsolation(t *testing.T) {
	s := openMem(t, nil)
	must(t, s.Put([]byte("k"), []byte("v0")))

	rtx, err := s.Begin() // 读事务开始，固定快照
	if err != nil {
		t.Fatal(err)
	}
	defer rtx.Rollback()

	// 读事务开始后发生并发写入与删除。
	must(t, s.Put([]byte("k"), []byte("v1")))
	must(t, s.Put([]byte("new"), []byte("x")))
	must(t, s.Delete([]byte("k")))
	must(t, s.Put([]byte("k"), []byte("v2")))

	v, err := rtx.Get([]byte("k"))
	if err != nil || string(v) != "v0" {
		t.Fatalf("snapshot read = %q, %v; want v0", v, err)
	}
	if _, err := rtx.Get([]byte("new")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("snapshot should not see new key, err = %v", err)
	}
}

func TestReadYourWrites(t *testing.T) {
	s := openMem(t, nil)
	tx, err := s.Begin()
	if err != nil {
		t.Fatal(err)
	}
	must(t, tx.Put([]byte("k"), []byte("mine")))
	v, err := tx.Get([]byte("k"))
	if err != nil || string(v) != "mine" {
		t.Fatalf("read-your-writes = %q, %v", v, err)
	}
	must(t, tx.Rollback())
	// 回滚后数据不可见。
	if _, err := s.Get([]byte("k")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("after rollback err = %v", err)
	}
}

func TestWriteConflictDeterministic(t *testing.T) {
	s := openMem(t, nil)
	must(t, s.Put([]byte("k"), []byte("base")))

	tx1, err := s.Begin()
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := s.Begin()
	if err != nil {
		t.Fatal(err)
	}
	must(t, tx1.Put([]byte("k"), []byte("from-tx1")))
	must(t, tx2.Put([]byte("k"), []byte("from-tx2")))

	// 先提交者获胜，后提交者必须收到可区分的冲突错误。
	must(t, tx1.Commit())
	if err := tx2.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatalf("tx2.Commit err = %v; want ErrConflict", err)
	}
	// 无丢失更新、无静默覆盖：最终值必须是获胜方的值。
	v, err := s.Get([]byte("k"))
	if err != nil || string(v) != "from-tx1" {
		t.Fatalf("final value = %q, %v; want from-tx1", v, err)
	}
}

func TestNoConflictOnDisjointKeys(t *testing.T) {
	s := openMem(t, nil)
	tx1, _ := s.Begin()
	tx2, _ := s.Begin()
	must(t, tx1.Put([]byte("a"), []byte("1")))
	must(t, tx2.Put([]byte("b"), []byte("2")))
	must(t, tx1.Commit())
	must(t, tx2.Commit())
}

func TestAbortedTxnDoesNotPollute(t *testing.T) {
	s := openMem(t, nil)
	must(t, s.Put([]byte("k"), []byte("committed")))

	tx, _ := s.Begin()
	must(t, tx.Put([]byte("k"), []byte("dirty")))
	must(t, tx.Put([]byte("junk"), []byte("x")))
	must(t, tx.Rollback())

	v, err := s.Get([]byte("k"))
	if err != nil || string(v) != "committed" {
		t.Fatalf("k = %q, %v", v, err)
	}
	if _, err := s.Get([]byte("junk")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("junk visible after rollback: %v", err)
	}
}

func TestConcurrentCommitsNoLostUpdate(t *testing.T) {
	s := openMem(t, nil)
	must(t, s.Put([]byte("counter"), []byte("0")))

	const n = 16
	// 先让全部事务基于同一快照开始，再并发提交，确保产生真实冲突。
	txns := make([]*Txn, 0, n)
	for i := 0; i < n; i++ {
		tx, err := s.Begin()
		if err != nil {
			t.Fatal(err)
		}
		must(t, tx.Put([]byte("counter"), []byte(fmt.Sprintf("%d", i))))
		txns = append(txns, tx)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	committed := 0
	for _, tx := range txns {
		wg.Add(1)
		go func(tx *Txn) {
			defer wg.Done()
			if err := tx.Commit(); err == nil {
				mu.Lock()
				committed++
				mu.Unlock()
			} else if !errors.Is(err, ErrConflict) {
				t.Errorf("unexpected commit error: %v", err)
			}
		}(tx)
	}
	wg.Wait()
	if committed != 1 {
		t.Fatalf("committed = %d; want exactly 1 winner", committed)
	}
}

func TestScanSnapshotSemantics(t *testing.T) {
	s := openMem(t, nil)
	for i := 0; i < 10; i++ {
		must(t, s.Put([]byte(fmt.Sprintf("key%02d", i)), []byte(fmt.Sprintf("v%d", i))))
	}
	tx, err := s.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	// 扫描期间发生并发写入/删除。
	must(t, s.Put([]byte("key05"), []byte("changed")))
	must(t, s.Delete([]byte("key07")))
	must(t, s.Put([]byte("key99"), []byte("new")))

	it := tx.Scan([]byte("key03"), []byte("key08"))
	var got []string
	for it.Valid() {
		got = append(got, fmt.Sprintf("%s=%s", it.Key(), it.Value()))
		it.Next()
	}
	want := []string{"key03=v3", "key04=v4", "key05=v5", "key06=v6", "key07=v7"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("scan = %v; want %v", got, want)
	}
}

func TestTooManyTxns(t *testing.T) {
	s := openMem(t, func(c *Config) { c.MaxActiveTxns = 2 })
	tx1, err := s.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx1.Rollback()
	tx2, err := s.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx2.Rollback()
	if _, err := s.Begin(); !errors.Is(err, ErrTooManyTxns) {
		t.Fatalf("third Begin err = %v; want ErrTooManyTxns", err)
	}
}

func TestTxnExpiry(t *testing.T) {
	s := openMem(t, func(c *Config) { c.MaxTxnDuration = 30 * time.Millisecond })
	tx, err := s.Begin()
	if err != nil {
		t.Fatal(err)
	}
	must(t, tx.Put([]byte("k"), []byte("v")))
	time.Sleep(60 * time.Millisecond)
	if err := tx.Commit(); !errors.Is(err, ErrTxnExpired) {
		t.Fatalf("expired Commit err = %v; want ErrTxnExpired", err)
	}
	// 过期事务的写入不得生效。
	if _, err := s.Get([]byte("k")); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expired txn write visible: %v", err)
	}
	// 事务槽位应被释放，可继续开启新事务。
	if _, err := s.Begin(); err != nil {
		t.Fatalf("Begin after expiry: %v", err)
	}
}

func TestTxnDoneErrors(t *testing.T) {
	s := openMem(t, nil)
	tx, _ := s.Begin()
	must(t, tx.Commit())
	if err := tx.Put([]byte("a"), []byte("1")); !errors.Is(err, ErrTxnDone) {
		t.Fatalf("Put after commit err = %v", err)
	}
	if _, err := tx.Get([]byte("a")); !errors.Is(err, ErrTxnDone) {
		t.Fatalf("Get after commit err = %v", err)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
