package mvcc

import "sort"

// Txn 是一个快照隔离事务。零值不可用，必须经由 Store.Begin 创建。
//
// 语义：
//   - Begin 时固化读快照（snapshot timestamp），之后 Get/Scan 只能看到
//     快照时刻已提交的版本，加上本事务自己的写入（read-your-writes）。
//   - Put/Delete 只写入事务私有缓冲，Commit 成功后才对外可见。
//   - Commit 采用“首提交者胜”（first-committer-wins）：若写集合中的任一键
//     在本事务快照之后被其他事务提交过，则本事务被中止并返回 *ConflictError。
//   - 任何未提交（回滚、中止、进程消失）的事务都不会留下任何痕迹。
type Txn struct {
	store      *Store
	id         uint64 // 事务唯一编号（冲突反馈中引用）
	snapshotTS uint64 // 读快照：仅可见 commitTS <= snapshotTS 的版本
	readOnly   bool
	start      timeStub // 由 store.clock 注入，便于测试
	deadline   timeStub

	closed bool
	// 写缓冲：每个键保留最后一次操作；writeOrder 记录首次出现顺序，
	// 使 WAL 记录与冲突键的顺序稳定可复现。
	writes     map[string]writeOp
	writeOrder []string
}

// ID 返回事务的唯一编号（单调递增，冲突反馈中会引用）。
func (t *Txn) ID() uint64 { return t.id }

// Get 读取键在事务快照中的值；不存在或已删除返回 ErrNotFound。
// 返回的字节切片是副本，调用方可自由修改。
func (t *Txn) Get(key string) ([]byte, error) {
	if t.closed {
		return nil, ErrTxnClosed
	}
	if err := t.store.checkLifetime(t); err != nil {
		return nil, err
	}
	// read-your-writes：本事务的缓冲优先于快照中的旧版本。
	if op, ok := t.writes[key]; ok {
		if op.delete {
			return nil, ErrNotFound
		}
		return append([]byte(nil), op.value...), nil
	}
	v, ok, err := t.store.getAt(key, t.snapshotTS)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNotFound
	}
	return v, nil
}

// Put 写入键值（事务缓冲，提交时才对外可见）。
func (t *Txn) Put(key string, value []byte) error {
	if t.closed {
		return ErrTxnClosed
	}
	if t.readOnly {
		return ErrReadOnly
	}
	if err := t.store.checkLifetime(t); err != nil {
		return err
	}
	if _, exists := t.writes[key]; !exists {
		if t.store.opts.MaxWritesPerTxn >= 0 &&
			len(t.writes) >= t.store.opts.MaxWritesPerTxn {
			return ErrTxnTooLarge
		}
		t.writeOrder = append(t.writeOrder, key)
	}
	t.writes[key] = writeOp{key: key, value: append([]byte(nil), value...)}
	return nil
}

// Delete 删除键（以墓碑标记表示，同样在提交时生效）。
// 删除一个不存在的键不是错误（提交时墓碑可能对旧快照读者无影响）。
func (t *Txn) Delete(key string) error {
	if t.closed {
		return ErrTxnClosed
	}
	if t.readOnly {
		return ErrReadOnly
	}
	if err := t.store.checkLifetime(t); err != nil {
		return err
	}
	if _, exists := t.writes[key]; !exists {
		if t.store.opts.MaxWritesPerTxn >= 0 &&
			len(t.writes) >= t.store.opts.MaxWritesPerTxn {
			return ErrTxnTooLarge
		}
		t.writeOrder = append(t.writeOrder, key)
	}
	t.writes[key] = writeOp{key: key, delete: true}
	return nil
}

// Scan 返回 [start, end) 字典序范围内、对事务快照可见的键值，
// 结果按键升序排列，并合并本事务自身的写入/删除。
// end 为空字符串时表示扫描到最后。扫描基于事务开始时的快照，
// 扫描期间其他事务的提交不会影响结果。
func (t *Txn) Scan(start, end string) ([]KeyValue, error) {
	if t.closed {
		return nil, ErrTxnClosed
	}
	if err := t.store.checkLifetime(t); err != nil {
		return nil, err
	}
	keys, err := t.store.scanKeysAt(start, end, t.snapshotTS)
	if err != nil {
		return nil, err
	}
	// 合并写缓冲中落在范围内的键。
	seen := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		seen[k] = struct{}{}
	}
	for _, k := range t.writeOrder {
		if inRange(k, start, end) {
			if _, ok := seen[k]; !ok {
				keys = append(keys, k)
				seen[k] = struct{}{}
			}
		}
	}
	sort.Strings(keys)

	out := make([]KeyValue, 0, len(keys))
	for _, k := range keys {
		var val []byte
		if op, ok := t.writes[k]; ok {
			if op.delete {
				continue // 本事务删除：对自己的扫描也不可见
			}
			val = append([]byte(nil), op.value...)
		} else {
			v, ok, err := t.store.getAt(k, t.snapshotTS)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			val = v
		}
		out = append(out, KeyValue{Key: k, Value: val})
	}
	return out, nil
}

// Commit 尝试提交；若与先提交的事务冲突，返回包裹 ErrConflict 的 *ConflictError。
// 只读事务的 Commit 等价于结束事务、释放配额。
func (t *Txn) Commit() error {
	if t.closed {
		return ErrTxnClosed
	}
	return t.store.commit(t)
}

// Rollback 放弃事务；可安全地重复调用，提交之后再调用也无副作用。
func (t *Txn) Rollback() {
	if t == nil || t.closed || t.store == nil {
		return
	}
	t.store.finishTxn(t)
}

// KeyValue 是扫描结果中的一条键值。
type KeyValue struct {
	Key   string
	Value []byte
}

// writeOp 是事务内缓冲的单个写操作。
type writeOp struct {
	key    string
	value  []byte
	delete bool
}

func inRange(key, start, end string) bool {
	if key < start {
		return false
	}
	if end != "" && key >= end {
		return false
	}
	return true
}
