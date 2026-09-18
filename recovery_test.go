package mvcc

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestRecoveryCommittedData(t *testing.T) {
	dir := t.TempDir()
	open := func() *Store {
		s, err := Open(&Options{Dir: dir})
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		return s
	}

	s := open()
	for i := 0; i < 25; i++ {
		k := "k" + itoa(uint64(i))
		v := "value-" + itoa(uint64(i))
		if err := s.Update(func(tx *Txn) error { return tx.Put(k, []byte(v)) }); err != nil {
			t.Fatal(err)
		}
	}
	// 删除一个键，再覆盖一个键，验证墓碑与多版本重放。
	if err := s.Update(func(tx *Txn) error { return tx.Delete("k3") }); err != nil {
		t.Fatal(err)
	}
	if err := s.Update(func(tx *Txn) error { return tx.Put("k7", []byte("overwritten")) }); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新打开：已提交数据必须完整。
	s = open()
	defer s.Close()
	for i := 0; i < 25; i++ {
		k := "k" + itoa(uint64(i))
		if i == 3 {
			assertNotFound(t, s, k)
			continue
		}
		want := "value-" + itoa(uint64(i))
		if i == 7 {
			want = "overwritten"
		}
		assertValue(t, s, k, want)
	}

	// 恢复后写入必须继续正常工作（计数器延续，且不会与历史冲突）。
	if err := s.Update(func(tx *Txn) error { return tx.Put("new", []byte("x")) }); err != nil {
		t.Fatalf("write after recovery: %v", err)
	}
	assertValue(t, s, "new", "x")
}

// TestRecoveryUncommittedNotPersisted：未 Commit 的事务不写 WAL，
// “进程消失”后重开数据必须保持原状。
func TestRecoveryUncommittedNotPersisted(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(&Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Update(func(tx *Txn) error { return tx.Put("committed", []byte("1")) }); err != nil {
		t.Fatal(err)
	}
	tx, err := s.Begin(false)
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Put("phantom", []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := tx.Delete("committed"); err != nil {
		t.Fatal(err)
	}
	// 不提交、不显式回滚，直接关闭（模拟进程异常中断，缓冲随内存消失）。
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(&Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	assertNotFound(t, s2, "phantom")
	assertValue(t, s2, "committed", "1")
}

// TestRecoveryTornTail：在 WAL 尾部追加任意半条垃圾，模拟写入中途掉电；
// 已提交记录必须全部保留，半条记录被忽略。
func TestRecoveryTornTail(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(&Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Update(func(tx *Txn) error { return tx.Put("a", []byte("1")) }); err != nil {
		t.Fatal(err)
	}
	if err := s.Update(func(tx *Txn) error { return tx.Put("b", []byte("2")) }); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	walPath := filepath.Join(dir, walFileName)
	good, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	junk := append([]byte(nil), good...)
	junk = append(junk, 0xAA, 0xBB, 0xCC, 0xDD, 0x01, 0x00, 0x00, 0x00)
	junk = append(junk, "partial record body"...)
	if err := os.WriteFile(walPath, junk, 0o644); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(&Options{Dir: dir})
	if err != nil {
		t.Fatalf("open with torn tail: %v", err)
	}
	assertValue(t, s2, "a", "1")
	assertValue(t, s2, "b", "2")
	// 恢复后还能继续提交，新记录追加在截断视角之后。
	if err := s2.Update(func(tx *Txn) error { return tx.Put("c", []byte("3")) }); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	// 再开一次，确认状态稳定（恢复可重复执行）。
	s3, err := Open(&Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	assertValue(t, s3, "a", "1")
	assertValue(t, s3, "b", "2")
	assertValue(t, s3, "c", "3")
}

// TestRecoveryIdempotent：对同一目录连续 Open（每次之间不写）结果必须完全一致。
func TestRecoveryIdempotent(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(&Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	for _, kv := range []KeyValue{{"x", []byte("1")}, {"y", []byte("2")}, {"z", []byte("3")}} {
		if err := s.Update(func(tx *Txn) error { return tx.Put(kv.Key, kv.Value) }); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	var first []KeyValue
	for round := 0; round < 3; round++ {
		sn, err := Open(&Options{Dir: dir})
		if err != nil {
			t.Fatal(err)
		}
		var rows []KeyValue
		if err := sn.View(func(tx *Txn) error {
			r, err := tx.Scan("", "")
			if err != nil {
				return err
			}
			rows = r
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := sn.Close(); err != nil {
			t.Fatal(err)
		}
		if first == nil {
			first = rows
		} else if !scanEqual(first, rows) {
			t.Fatalf("round %d drift:\n%+v\n%+v", round, first, rows)
		}
	}
}

// TestRecoveryFromSnapshotWithReplayedWAL：压缩产生快照后继续写入，
// 重启时快照 + 新 WAL 必须合并出正确状态；再压缩、再重启依然稳定。
func TestRecoveryFromSnapshotWithReplayedWAL(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(&Options{Dir: dir, MaxWALBytes: 1}) // 极小阈值：每次提交都压缩
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Update(func(tx *Txn) error { return tx.Put("a", []byte("1")) }); err != nil {
		t.Fatal(err)
	}
	if err := s.Update(func(tx *Txn) error { return tx.Put("b", []byte("2")) }); err != nil {
		t.Fatal(err)
	}
	if err := s.Update(func(tx *Txn) error { return tx.Put("a", []byte("11")) }); err != nil {
		t.Fatal(err)
	}
	if err := s.Update(func(tx *Txn) error { return tx.Delete("b") }); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(&Options{Dir: dir, MaxWALBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	assertValue(t, s2, "a", "11")
	assertNotFound(t, s2, "b")
	// 恢复后再次触发压缩，验证“读快照+重放 WAL”写出的快照仍然自洽。
	if err := s2.Update(func(tx *Txn) error { return tx.Put("c", []byte("3")) }); err != nil {
		t.Fatal(err)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	s3, err := Open(&Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s3.Close()
	assertValue(t, s3, "a", "11")
	assertNotFound(t, s3, "b")
	assertValue(t, s3, "c", "3")
}

// TestRecoveryCorruptMidWAL：WAL 中间记录 CRC 损坏必须作为硬错误暴露，
// 不能静默吞掉已提交数据。
func TestRecoveryCorruptMidWAL(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(&Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Update(func(tx *Txn) error { return tx.Put("a", []byte("1")) }); err != nil {
		t.Fatal(err)
	}
	walPath := filepath.Join(dir, walFileName)
	if err := s.Update(func(tx *Txn) error { return tx.Put("b", []byte("2")) }); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	// 翻转第一条提交记录 body 内的一个字节：第一帧从偏移 6 开始，
	// 帧头 8 字节，body 紧随其后。该记录后面还有第二帧，属于中部损坏，
	// 必须作为硬错误暴露而不是当作崩溃尾部忽略。
	data[6+8+10] ^= 0xFF
	if err := os.WriteFile(walPath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(&Options{Dir: dir}); !errors.Is(err, errCorrupt) {
		t.Fatalf("mid-WAL corruption must surface, got %v", err)
	}
}

// (end of recovery tests)

// TestCompactionWindowNoLoss：极小 WAL 阈值使几乎每次提交都触发
// 快照+WAL 重写。多个 goroutine 持续写不同键并更新热键，完成后关闭重开：
// 每一个“提交成功”的键值都必须存活——这专门验证“构建快照”与“重写 WAL”
// 原子化后不会丢掉压缩窗口中的提交。
func TestCompactionWindowNoLoss(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(&Options{Dir: dir, MaxWALBytes: 128, MaxActiveTxns: -1})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	committed := make(map[string]string) // 已确认提交成功的唯一键 -> 值
	const writers, rounds = 6, 60
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				uniq := fmt.Sprintf("u-w%d-%d", w, i)
				hot := fmt.Sprintf("hot-w%d-%d", w, i)
				err := s.Update(func(tx *Txn) error {
					if err := tx.Put("hot", []byte(hot)); err != nil {
						return err
					}
					return tx.Put(uniq, []byte(hot))
				})
				switch {
				case err == nil:
					mu.Lock()
					committed[uniq] = hot
					mu.Unlock()
				case errors.Is(err, ErrConflict):
					// 热键冲突导致整事务中止是预期行为（该唯一键未提交）。
				default:
					t.Errorf("writer %d round %d: %v", w, i, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := Open(&Options{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()

	for k, want := range committed {
		assertValue(t, s2, k, want)
	}
}
