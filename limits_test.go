package mvcc

import (
	"errors"
	"testing"
	"time"
)

func TestMaxActiveTransactions(t *testing.T) {
	s, err := Open(&Options{Dir: t.TempDir(), MaxActiveTxns: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	tx1, err := s.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	tx2, err := s.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Begin(true); !errors.Is(err, ErrTooManyTransactions) {
		t.Fatalf("want ErrTooManyTransactions, got %v", err)
	}
	// 结束一个后名额应释放。
	tx1.Rollback()
	tx3, err := s.Begin(true)
	if err != nil {
		t.Fatalf("slot should be released: %v", err)
	}
	_ = tx2
	tx3.Rollback()
	// 提交（而非回滚）同样释放名额。
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}
	tx4, err := s.Begin(true)
	if err != nil {
		t.Fatalf("commit should release slot: %v", err)
	}
	tx4.Rollback()
}

func TestMaxWritesPerTxn(t *testing.T) {
	s, err := Open(&Options{Dir: t.TempDir(), MaxWritesPerTxn: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	err = s.Update(func(tx *Txn) error {
		if err := tx.Put("a", []byte("1")); err != nil {
			return err
		}
		if err := tx.Put("b", []byte("2")); err != nil {
			return err
		}
		// 同一键重复写不算新键（按唯一键计费）。
		if err := tx.Put("a", []byte("11")); err != nil {
			return err
		}
		if err := tx.Delete("a"); err != nil {
			return err
		}
		if err := tx.Put("c", []byte("3")); err != nil {
			return err
		}
		if err := tx.Put("d", []byte("4")); !errors.Is(err, ErrTxnTooLarge) {
			t.Fatalf("want ErrTxnTooLarge, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("committing 3 unique keys should succeed: %v", err)
	}
	assertValue(t, s, "b", "2")
	assertValue(t, s, "c", "3")
	assertNotFound(t, s, "a") // 最终操作是删除
}

func TestMaxLiveBytes(t *testing.T) {
	// 上限 10 字节；key+value 计费。
	s, err := Open(&Options{Dir: t.TempDir(), MaxLiveBytes: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// "ab" + "cdefgh" = 10 字节：恰好允许。
	if err := s.Update(func(tx *Txn) error {
		return tx.Put("ab", []byte("cdefgh"))
	}); err != nil {
		t.Fatalf("at-limit write should succeed: %v", err)
	}
	// 超限事务必须整体被拒，且不能影响既有数据。
	err = s.Update(func(tx *Txn) error {
		return tx.Put("zzz", []byte("zzzzzzzz"))
	})
	if !errors.Is(err, ErrStorageLimit) {
		t.Fatalf("want ErrStorageLimit, got %v", err)
	}
	assertValue(t, s, "ab", "cdefgh")

	// 用更小的值覆盖可以成功（容量缩减）。
	if err := s.Update(func(tx *Txn) error {
		return tx.Put("ab", []byte("x")) // 3 字节
	}); err != nil {
		t.Fatalf("shrinking overwrite should succeed: %v", err)
	}
	// 删除释放容量后可重新写入。
	if err := s.Update(func(tx *Txn) error {
		if err := tx.Delete("ab"); err != nil {
			return err
		}
		return tx.Put("new", []byte("data"))
	}); err != nil {
		t.Fatalf("delete should free budget: %v", err)
	}
}

// fakeClock 是可手动推进的时钟，用于确定性地测试事务寿命上限。
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

func TestTxnLifetimeLimit(t *testing.T) {
	s, err := Open(&Options{
		Dir: t.TempDir(), MaxTxnLifetime: 0, // 0 在 normalize 后变默认值，下面手动覆盖
	})
	if err != nil {
		t.Fatal(err)
	}
	fc := &fakeClock{}
	s.clock = fc
	s.opts.MaxTxnLifetime = 5 // 5 个时间单位（fakeClock 用零时刻 + Add）

	defer s.Close()

	tx, err := s.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Put("k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	// 提前 1 单位：正常。
	fc.t = tx.start.Add(4)
	if err := tx.Put("k2", []byte("v2")); err != nil {
		t.Fatalf("within lifetime: %v", err)
	}
	// 超过寿命：读写都应被拒。
	fc.t = tx.start.Add(6)
	if _, err := tx.Get("k"); !errors.Is(err, ErrTxnTimeout) {
		t.Fatalf("read after deadline: %v", err)
	}
	if err := tx.Put("k3", []byte("v")); !errors.Is(err, ErrTxnTimeout) {
		t.Fatalf("write after deadline: %v", err)
	}
	if err := tx.Commit(); !errors.Is(err, ErrTxnTimeout) {
		t.Fatalf("commit after deadline: %v", err)
	}
	// 超时事务已结束，新事务正常。
	if err := s.Update(func(ntx *Txn) error { return ntx.Put("ok", []byte("1")) }); err != nil {
		t.Fatalf("new txn after timeout: %v", err)
	}
}

// TestLimitsDoNotBreakCompaction：小 MaxWALBytes 下高频写入稳定，
// 数据不丢、不无限增长（压缩持续生效）。
func TestLimitsDoNotBreakCompaction(t *testing.T) {
	s, err := Open(&Options{Dir: t.TempDir(), MaxWALBytes: 512})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	const n = 200
	for i := 0; i < n; i++ {
		v := "v" + itoa(uint64(i))
		if err := s.Update(func(tx *Txn) error {
			return tx.Put("hotkey", []byte(v))
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	assertValue(t, s, "hotkey", "v"+itoa(n-1))
}
