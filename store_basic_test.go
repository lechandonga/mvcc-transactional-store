package mvcc

import (
	"errors"
	"testing"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(&Options{Dir: dir})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestBasicPutGet(t *testing.T) {
	s := openTestStore(t)
	err := s.Update(func(tx *Txn) error {
		return tx.Put("k1", []byte("v1"))
	})
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := s.View(func(tx *Txn) error {
		v, err := tx.Get("k1")
		if err != nil {
			return err
		}
		if string(v) != "v1" {
			t.Fatalf("got %q want v1", v)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestGetNotFound(t *testing.T) {
	s := openTestStore(t)
	err := s.View(func(tx *Txn) error {
		if _, err := tx.Get("missing"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("want ErrNotFound, got %v", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view should succeed even when key missing, got %v", err)
	}
}

func TestRollbackDoesNotLeak(t *testing.T) {
	s := openTestStore(t)
	boom := errors.New("boom")
	err := s.Update(func(tx *Txn) error {
		if err := tx.Put("k", []byte("v")); err != nil {
			t.Fatalf("put: %v", err)
		}
		return boom // 触发回滚
	})
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	if err := s.View(func(tx *Txn) error {
		if _, err := tx.Get("k"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("rolled-back write leaked, got %v", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestReadYourWrites(t *testing.T) {
	s := openTestStore(t)
	if err := s.Update(func(tx *Txn) error {
		if err := tx.Put("k", []byte("old")); err != nil {
			return err
		}
		return tx.Put("k", []byte("new"))
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := s.Update(func(tx *Txn) error {
		// 覆盖已存在键后，事务内应读到新值。
		if err := tx.Put("k", []byte("newer")); err != nil {
			return err
		}
		v, err := tx.Get("k")
		if err != nil {
			return err
		}
		if string(v) != "newer" {
			t.Fatalf("read-your-writes got %q", v)
		}
		// 删除后事务内应表现为不存在。
		if err := tx.Delete("k"); err != nil {
			return err
		}
		if _, err := tx.Get("k"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("post-delete self read got %v", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := s.View(func(tx *Txn) error {
		if _, err := tx.Get("k"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("committed delete should be invisible, got %v", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("view: %v", err)
	}
}

func TestReadOnlyRejectsWrites(t *testing.T) {
	s := openTestStore(t)
	tx, err := s.Begin(true)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Put("k", []byte("v")); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("want ErrReadOnly, got %v", err)
	}
	if err := tx.Delete("k"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("want ErrReadOnly, got %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("read-only commit: %v", err)
	}
	// 关闭后重复操作应有明确错误。
	if err := tx.Commit(); !errors.Is(err, ErrTxnClosed) {
		t.Fatalf("want ErrTxnClosed, got %v", err)
	}
	tx.Rollback() // 必须安全可重复
}

func TestGetValueIsolation(t *testing.T) {
	s := openTestStore(t)
	if err := s.Update(func(tx *Txn) error { return tx.Put("k", []byte("v")) }); err != nil {
		t.Fatal(err)
	}
	v, err := func() ([]byte, error) {
		var out []byte
		err := s.View(func(tx *Txn) error {
			v, err := tx.Get("k")
			if err != nil {
				return err
			}
			out = v
			return nil
		})
		return out, err
	}()
	if err != nil {
		t.Fatal(err)
	}
	v[0] = 'X' // 修改返回值不得影响存储
	if err := s.View(func(tx *Txn) error {
		got, err := tx.Get("k")
		if err != nil {
			return err
		}
		if string(got) != "v" {
			t.Fatalf("store mutated through returned slice: %q", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
