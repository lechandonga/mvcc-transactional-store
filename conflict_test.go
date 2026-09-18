package mvcc

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// TestConflictFirstCommitterWins：两个并发事务写同一键，
// 一个成功、另一个必须收到 *ConflictError，且不得丢失更新或静默覆盖。
func TestConflictFirstCommitterWins(t *testing.T) {
	s := openTestStore(t)
	if err := s.Update(func(tx *Txn) error { return tx.Put("k", []byte("base")) }); err != nil {
		t.Fatal(err)
	}

	t1, err := s.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	t2, err := s.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := t1.Put("k", []byte("from-t1")); err != nil {
		t.Fatal(err)
	}
	if err := t2.Put("k", []byte("from-t2")); err != nil {
		t.Fatal(err)
	}

	// 确定性：固定先提交 t1，则 t1 必胜、t2 必败。
	if err := t1.Commit(); err != nil {
		t.Fatalf("first commit should win, got %v", err)
	}
	err = t2.Commit()
	var ce *ConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("loser must get *ConflictError, got %T: %v", err, err)
	}
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict must satisfy errors.Is(ErrConflict)")
	}
	if ce.Key != "k" || ce.WinnerID != t1.id || ce.LoserID != t2.id {
		t.Fatalf("conflict detail = %+v", ce)
	}

	if err := s.View(func(tx *Txn) error {
		v, err := tx.Get("k")
		if err != nil {
			return err
		}
		if string(v) != "from-t1" {
			t.Fatalf("lost update! value = %q", v)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// 被中止的事务再操作/提交应报错，且回滚可安全调用。
	if err := t2.Put("k", []byte("x")); !errors.Is(err, ErrTxnClosed) {
		t.Fatalf("aborted txn use: %v", err)
	}
	t1.Rollback()
	t2.Rollback()
}

// TestConflictOrderReversed：交换提交顺序，胜负也必须确定地交换。
func TestConflictOrderReversed(t *testing.T) {
	s := openTestStore(t)
	seed(t, s, "k", "base")

	t1, _ := s.Begin(false)
	t2, _ := s.Begin(false)
	_ = t1.Put("k", []byte("t1"))
	_ = t2.Put("k", []byte("t2"))
	if err := t2.Commit(); err != nil {
		t.Fatalf("t2 first: %v", err)
	}
	if err := t1.Commit(); err == nil || !errors.Is(err, ErrConflict) {
		t.Fatalf("t1 must lose, got %v", err)
	}
	assertValue(t, s, "k", "t2")
}

// TestConflictDeleteVsPut：删除与写入并发也必须检测冲突。
func TestConflictDeleteVsPut(t *testing.T) {
	s := openTestStore(t)
	seed(t, s, "k", "base")

	t1, _ := s.Begin(false)
	t2, _ := s.Begin(false)
	_ = t1.Delete("k")
	_ = t2.Put("k", []byte("put"))
	if err := t1.Commit(); err != nil {
		t.Fatalf("delete first: %v", err)
	}
	err := t2.Commit()
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("put after concurrent delete must conflict, got %v", err)
	}
	assertNotFound(t, s, "k")
}

// TestConflictOnlyOverlappingKey：写不同键的并发事务互不影响。
func TestConflictOnlyOverlappingKey(t *testing.T) {
	s := openTestStore(t)
	seed(t, s, "k1", "a")
	seed(t, s, "k2", "b")

	t1, _ := s.Begin(false)
	t2, _ := s.Begin(false)
	if err := t1.Put("k1", []byte("a1")); err != nil {
		t.Fatal(err)
	}
	if err := t2.Put("k2", []byte("b1")); err != nil {
		t.Fatal(err)
	}
	if err := t1.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := t2.Commit(); err != nil {
		t.Fatalf("disjoint writes must both commit: %v", err)
	}
	assertValue(t, s, "k1", "a1")
	assertValue(t, s, "k2", "b1")
}

// TestConflictMultiKeyWriteSet：事务写多个键时，只要其中一个键被抢先
// 提交，整个事务必须原子地中止（其他键也不得部分生效）。
func TestConflictMultiKeyWriteSet(t *testing.T) {
	s := openTestStore(t)
	seed(t, s, "a", "0")
	seed(t, s, "b", "0")

	big, _ := s.Begin(false)
	_ = big.Put("a", []byte("A"))
	_ = big.Put("b", []byte("B"))

	// 另一个事务抢先提交其中一个键。
	if err := s.Update(func(tx *Txn) error { return tx.Put("b", []byte("interloper")) }); err != nil {
		t.Fatal(err)
	}
	err := big.Commit()
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("multi-key txn must abort entirely, got %v", err)
	}
	assertValue(t, s, "a", "0") // 不得部分生效
	assertValue(t, s, "b", "interloper")
}

// TestConcurrentConflictsDeterministicCount：N 个并发事务在同一快照上
// 各写同一键后同时提交，必须恰好 1 个成功、N-1 个冲突，最终值唯一
// 确定（无丢失更新/静默覆盖）。重复多轮确认结果与调度无关。
func TestConcurrentConflictsDeterministicCount(t *testing.T) {
	for iter := 0; iter < 20; iter++ {
		s := openTestStore(t)
		seed(t, s, "counter", "0")

		const n = 16
		txns := make([]*Txn, n)
		for i := 0; i < n; i++ {
			tx, err := s.Begin(false) // 全部先开始 -> 同一快照
			if err != nil {
				t.Fatal(err)
			}
			if err := tx.Put("counter", []byte(fmt.Sprintf("tx%d", i))); err != nil {
				t.Fatal(err)
			}
			txns[i] = tx
		}

		var wg sync.WaitGroup
		var commits, conflicts int32
		var mu sync.Mutex
		ready := make(chan struct{})    // 所有事务已建完并写完
		commitGo := make(chan struct{}) // 统一放行提交
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(tx *Txn) {
				defer wg.Done()
				<-ready
				<-commitGo
				err := tx.Commit()
				mu.Lock()
				defer mu.Unlock()
				switch {
				case err == nil:
					commits++
				case errors.Is(err, ErrConflict):
					conflicts++
				default:
					t.Errorf("unexpected err: %v", err)
				}
			}(txns[i])
		}
		close(ready)
		close(commitGo) // 所有事务基于同一快照、同时提交
		wg.Wait()
		if commits != 1 || conflicts != n-1 {
			t.Fatalf("iter %d: commits=%d conflicts=%d, want 1 and %d",
				iter, commits, conflicts, n-1)
		}
		got := getValue(t, s, "counter")
		if len(got) < 2 || got[:2] != "tx" {
			t.Fatalf("unexpected final value %q", got)
		}
	}
}

// TestAbortedTxnDoesNotAffectOthers：被中止事务之后启动的新事务能正常工作。
func TestAbortedTxnDoesNotAffectOthers(t *testing.T) {
	s := openTestStore(t)
	seed(t, s, "k", "v")
	loser, _ := s.Begin(false)
	_ = loser.Put("k", []byte("x"))
	if err := s.Update(func(tx *Txn) error { return tx.Put("k", []byte("winner")) }); err != nil {
		t.Fatal(err)
	}
	if err := loser.Commit(); !errors.Is(err, ErrConflict) {
		t.Fatalf("want conflict, got %v", err)
	}
	if err := s.Update(func(tx *Txn) error {
		v, err := tx.Get("k")
		if err != nil {
			return err
		}
		if string(v) != "winner" {
			return fmt.Errorf("got %q", v)
		}
		return tx.Put("k2", []byte("ok"))
	}); err != nil {
		t.Fatalf("later txn after abort: %v", err)
	}
}

func seed(t *testing.T, s *Store, k, v string) {
	t.Helper()
	if err := s.Update(func(tx *Txn) error { return tx.Put(k, []byte(v)) }); err != nil {
		t.Fatal(err)
	}
}

func assertValue(t *testing.T, s *Store, k, want string) {
	t.Helper()
	if got := getValue(t, s, k); got != want {
		t.Fatalf("key %s = %q, want %q", k, got, want)
	}
}

func assertNotFound(t *testing.T, s *Store, k string) {
	t.Helper()
	if err := s.View(func(tx *Txn) error {
		_, err := tx.Get(k)
		return err
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound for %s, got %v", k, err)
	}
}

func getValue(t *testing.T, s *Store, k string) string {
	t.Helper()
	var out string
	if err := s.View(func(tx *Txn) error {
		v, err := tx.Get(k)
		if err != nil {
			return err
		}
		out = string(v)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}
