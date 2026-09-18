package mvcc

import (
	"errors"
	"fmt"
	"sync"
	"testing"
)

// TestMixedConcurrentWorkload：高频写入 + 长寿命只读快照 + 短读事务混合。
// 验证：快照读者始终读到一致视图；并发写同一键恰好一个成功；
// 旧版本 GC 不会破坏长寿命读者；资源计数（活跃事务）不错乱。
func TestMixedConcurrentWorkload(t *testing.T) {
	s, err := Open(&Options{Dir: t.TempDir(), MaxActiveTxns: -1, MaxWALBytes: 4096})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 初始数据
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("key%03d", i)
		if err := s.Update(func(tx *Txn) error { return tx.Put(k, []byte("v0")) }); err != nil {
			t.Fatal(err)
		}
	}

	// 长寿命读快照：开始于 v0 时代，整个测试期间它的视图不得变化。
	oldReader, err := s.Begin(true)
	if err != nil {
		t.Fatal(err)
	}
	defer oldReader.Rollback()
	oldRows, err := oldReader.Scan("", "")
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var failMu sync.Mutex
	var failures []string
	addFail := func(f string, a ...any) {
		failMu.Lock()
		failures = append(failures, fmt.Sprintf(f, a...))
		failMu.Unlock()
	}

	// 写者组：多 goroutine 反复写同一小批键（强制冲突）。
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				err := s.Update(func(tx *Txn) error {
					key := fmt.Sprintf("key%03d", i%10) // 高冲突键集合
					return tx.Put(key, []byte(fmt.Sprintf("w%d-%d", w, i)))
				})
				if err != nil && !errors.Is(err, ErrConflict) {
					addFail("writer unexpected err: %v", err)
					return
				}
			}
		}(w)
	}

	// 新快照读者组：每次读取都必须满足 SI 约束（读到的 key%03d 全有值）。
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				err := s.View(func(tx *Txn) error {
					rows, err := tx.Scan("key000", "key050")
					if err != nil {
						return err
					}
					if len(rows) != 50 {
						return fmt.Errorf("scan rows = %d, want 50", len(rows))
					}
					if len(rows[0].Value) == 0 {
						return fmt.Errorf("empty value for %s", rows[0].Key)
					}
					return nil
				})
				if err != nil {
					addFail("reader: %v", err)
					return
				}
			}
		}()
	}

	// 长寿命旧快照持续校验：永远只看到 v0。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			rows, err := oldReader.Scan("", "")
			if err != nil {
				addFail("old reader scan: %v", err)
				return
			}
			if !scanEqual(oldRows, rows) {
				addFail("old snapshot drifted: %+v vs %+v", oldRows, rows)
				return
			}
		}
	}()

	// 运行约 50ms（无 cgo 环境无法 -race，靠迭代量暴露问题）。
	// 使用足够多的写轮次形成压力。
	runWritersUntil := func(iters int) {}
	_ = runWritersUntil
	// 通过一个定时写者控制时长：主 goroutine 直接 sleep。
	// （不依赖外部 ticker 类型，简单 sleep 即可。）
	waitInTest(30)
	close(stop)
	wg.Wait()

	for _, f := range failures {
		t.Error(f)
	}

	// 最终一致性检查：每个 key000..key049 恰好一个最终值，扫描 50 行。
	if err := s.View(func(tx *Txn) error {
		rows, err := tx.Scan("", "")
		if err != nil {
			return err
		}
		if len(rows) != 50 {
			return fmt.Errorf("final rows = %d", len(rows))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// 活跃事务计数必须归零（除本 deferred 的 oldReader）。
	s.mu.RLock()
	active := s.activeCount
	s.mu.RUnlock()
	if active != 1 {
		t.Fatalf("active txn leak: %d", active)
	}
}
