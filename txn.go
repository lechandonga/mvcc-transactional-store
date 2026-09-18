package mvcc

import (
	"bytes"
	"sort"
	"time"
)

// Txn 是一个具备快照隔离语义的事务：
// 读取基于 beginTS 的一致快照，本事务的缓冲写入对自己可见。
type Txn struct {
	store     *Store
	id        uint64
	beginTS   uint64
	writes    map[string]*[]byte // 指针为 nil 表示删除
	done      bool
	expiresAt time.Time
}

// checkUsable 校验事务仍可操作且未超过配置的最大存活时长。
func (t *Txn) checkUsable() error {
	if t.done {
		return ErrTxnDone
	}
	if time.Now().After(t.expiresAt) {
		t.Rollback()
		return ErrTxnExpired
	}
	return nil
}

// Get 在事务快照上读取键；本事务自己的写入可见（read-your-writes）。
func (t *Txn) Get(key []byte) ([]byte, error) {
	if err := t.checkUsable(); err != nil {
		return nil, err
	}
	k := string(key)
	if w, ok := t.writes[k]; ok {
		if w == nil {
			return nil, ErrKeyNotFound
		}
		return append([]byte(nil), (*w)...), nil
	}
	t.store.mu.RLock()
	defer t.store.mu.RUnlock()
	v, ok := t.store.visibleAt(k, t.beginTS)
	if !ok {
		return nil, ErrKeyNotFound
	}
	return append([]byte(nil), v...), nil
}

// Put 在事务内缓冲一个写入，提交时生效。
func (t *Txn) Put(key, value []byte) error {
	if err := t.checkUsable(); err != nil {
		return err
	}
	v := append([]byte(nil), value...)
	t.writes[string(key)] = &v
	return nil
}

// Delete 在事务内缓冲一个删除，提交时生效。
func (t *Txn) Delete(key []byte) error {
	if err := t.checkUsable(); err != nil {
		return err
	}
	t.writes[string(key)] = nil
	return nil
}

// Commit 执行冲突检测并原子提交。
// 冲突规则（first-committer-wins）：若本事务写入的某个键，
// 其最新已提交版本的 commitTS 大于本事务 beginTS，则提交被拒绝并返回
// ErrConflict，事务随之结束，缓冲写入全部丢弃，不产生任何部分效果。
func (t *Txn) Commit() error {
	if err := t.checkUsable(); err != nil {
		return err
	}
	s := t.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		t.finish()
		return ErrStoreClosed
	}
	// 写-写冲突检测。
	for k := range t.writes {
		if versions := s.data[k]; len(versions) > 0 && versions[0].commitTS > t.beginTS {
			t.finish()
			return ErrConflict
		}
	}
	if len(t.writes) > 0 {
		s.commitTS++
		rec := walRecord{commitTS: s.commitTS}
		keys := make([]string, 0, len(t.writes))
		for k := range t.writes {
			keys = append(keys, k)
		}
		sort.Strings(keys) // 确定性写入顺序，保证 WAL 内容可复现
		for _, k := range keys {
			w := t.writes[k]
			op := walOp{key: []byte(k)}
			if w == nil {
				op.deleted = true
			} else {
				op.value = *w
			}
			rec.ops = append(rec.ops, op)
		}
		// 先写 WAL 再应用内存，保证已提交数据不丢失。
		if s.wal != nil {
			if err := s.wal.append(rec, s.cfg.SyncWrites); err != nil {
				t.finish()
				return err
			}
		}
		s.applyRecord(rec)
		s.gc()
	}
	t.finish()
	return nil
}

// Rollback 丢弃事务的所有缓冲写入。
func (t *Txn) Rollback() error {
	if t.done {
		return nil
	}
	t.store.mu.Lock()
	t.finish()
	t.store.mu.Unlock()
	return nil
}

// finish 结束事务并从活跃集合移除；调用方需持 store 写锁，
// 或由已保证互斥的路径调用。
func (t *Txn) finish() {
	t.done = true
	t.writes = nil
	delete(t.store.active, t.id)
}

// Scan 返回事务快照上 [start, end) 范围的有序迭代器。
// 结果在调用时一次性物化，之后的并发写入不影响本次扫描结果。
func (t *Txn) Scan(start, end []byte) *Iterator {
	s := t.store
	s.mu.RLock()
	_, vals := s.snapshotKeys(t.beginTS)
	s.mu.RUnlock()

	// 合并本事务的缓冲写入。
	for k, w := range t.writes {
		if w == nil {
			delete(vals, k)
		} else {
			vals[k] = *w
		}
	}
	merged := make([][]byte, 0, len(vals))
	for k := range vals {
		if (len(start) == 0 || bytes.Compare([]byte(k), start) >= 0) &&
			(len(end) == 0 || bytes.Compare([]byte(k), end) < 0) {
			merged = append(merged, []byte(k))
		}
	}
	sort.Slice(merged, func(i, j int) bool { return bytes.Compare(merged[i], merged[j]) < 0 })
	it := &Iterator{pos: 0}
	for _, k := range merged {
		it.keys = append(it.keys, k)
		it.vals = append(it.vals, append([]byte(nil), vals[string(k)]...))
	}
	return it
}

// Iterator 在快照上按序遍历键值。
type Iterator struct {
	keys [][]byte
	vals [][]byte
	pos  int
}

// Valid 报告迭代器是否指向有效条目。
func (it *Iterator) Valid() bool {
	return it.pos < len(it.keys)
}

// Next 前进到下一条目。
func (it *Iterator) Next() {
	it.pos++
}

// Key 返回当前键。
func (it *Iterator) Key() []byte {
	return it.keys[it.pos]
}

// Value 返回当前值。
func (it *Iterator) Value() []byte {
	return it.vals[it.pos]
}
