package mvcc

import (
	"sync"
	"time"
)

// version 是一个键的某一已提交版本。
// 同一 key 的版本构成链表，head 指向最新；版本一经发布即不可变。
type version struct {
	value    []byte // 墓碑时为 nil
	delete   bool
	commitTS uint64 // 提交时间戳（单调递增，可见性判定用）
	txnID    uint64 // 创建该版本的事务 ID（冲突反馈用）
	prev     *version
}

// clock 抽象时间来源，便于在测试中确定性地推进时钟。
type clock interface{ now() time.Time }

type wallClock struct{}

func (wallClock) now() time.Time { return time.Now() }

// timeStub 是时间点的内部别名（字段以 stub 命名，避免污染公共 API）。
type timeStub = time.Time

// Store 是支持快照隔离事务的 MVCC 存储引擎。
type Store struct {
	dir   string
	opts  *Options
	wal   *walWriter
	clock clock

	mu sync.RWMutex

	seq             uint64 // 单调编号源：同时承担事务 ID 与提交时间戳的分配
	committedHighTS uint64 // 已提交水位：最近一次提交的时间戳，新事务的读快照取它

	versions map[string]*version // key -> 版本链头（最新）

	active      map[*Txn]struct{}
	activeCount int
	liveBytes   int64 // 最新版本（不含墓碑）的键值总字节
	closed      bool
}

// Open 打开（或初始化）位于 opts.Dir 的存储，并执行崩溃恢复。
// 恢复是幂等的：对同一目录重复调用 Open 得到相同状态。
func Open(opts *Options) (*Store, error) {
	o := opts.normalize()
	wal, err := openWAL(o.Dir)
	if err != nil {
		return nil, err
	}
	live, lastTS, err := recoverState(o.Dir)
	if err != nil {
		_ = wal.close()
		return nil, err
	}
	s := &Store{
		dir:      o.Dir,
		opts:     o,
		wal:      wal,
		clock:    wallClock{},
		seq:      lastTS,
		versions: make(map[string]*version),
		active:   make(map[*Txn]struct{}),
	}
	for k, v := range live {
		// 快照来源的版本时间戳为 0：它在任何事务开始之前就已存在。
		s.versions[k] = &version{value: v, commitTS: 0}
		s.liveBytes += entryBytes(k, v)
	}
	return s, nil
}

// Close 刷盘并释放资源。关闭后不可再使用。可安全重复调用。
func (s *Store) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	s.closed = true
	wal := s.wal
	s.mu.Unlock()
	return wal.close()
}

// Begin 开始一个事务。readOnly 为 true 时事务不允许写入，
// 且不占用单事务写入条数的配额。
func (s *Store) Begin(readOnly bool) (*Txn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	if s.opts.MaxActiveTxns >= 0 && s.activeCount >= s.opts.MaxActiveTxns {
		return nil, ErrTooManyTransactions
	}
	s.seq++
	now := s.clock.now()
	t := &Txn{
		store:      s,
		id:         s.seq,
		snapshotTS: s.committedHighTS,
		readOnly:   readOnly,
		start:      now,
		deadline:   now,
		writes:     make(map[string]writeOp),
	}
	if s.opts.MaxTxnLifetime >= 0 {
		t.deadline = now.Add(s.opts.MaxTxnLifetime)
	}
	s.active[t] = struct{}{}
	s.activeCount++
	return t, nil
}

// View 在一个只读事务中执行 fn。fn 返回的错误会原样向上传递。
func (s *Store) View(fn func(*Txn) error) (err error) {
	t, err := s.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			t.Rollback()
			panic(p)
		}
	}()
	if err = fn(t); err != nil {
		t.Rollback()
		return err
	}
	return t.Commit()
}

// Update 在一个读写事务中执行 fn；fn 返回 nil 时提交，否则回滚并返回该错误。
// 提交阶段的冲突等错误同样向上传递。
func (s *Store) Update(fn func(*Txn) error) (err error) {
	t, err := s.Begin(false)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			t.Rollback()
			panic(p)
		}
	}()
	if err = fn(t); err != nil {
		t.Rollback()
		return err
	}
	return t.Commit()
}

// checkLifetime 在事务每次操作与提交时校验最长存活时间。
func (s *Store) checkLifetime(t *Txn) error {
	if s.opts.MaxTxnLifetime < 0 {
		return nil
	}
	if s.clock.now().After(t.deadline) {
		return ErrTxnTimeout
	}
	return nil
}

// getAt 返回 key 在时间戳 ts 下可见的版本值。
// 整个读取过程持读锁：版本链一旦发布就不可变，读锁只防止与
// GC 剪枝并发，天然保证“扫描期间并发写入不影响结果”。
func (s *Store) getAt(key string, ts uint64) ([]byte, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, false, ErrClosed
	}
	for v := s.versions[key]; v != nil; v = v.prev {
		if v.commitTS <= ts {
			if v.delete {
				return nil, false, nil
			}
			return append([]byte(nil), v.value...), true, nil
		}
	}
	return nil, false, nil
}

// scanKeysAt 收集 [start, end) 内在 ts 快照下可见（非墓碑）的键。
func (s *Store) scanKeysAt(start, end string, ts uint64) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return nil, ErrClosed
	}
	keys := make([]string, 0)
	for k, head := range s.versions {
		if !inRange(k, start, end) {
			continue
		}
		for v := head; v != nil; v = v.prev {
			if v.commitTS <= ts {
				if !v.delete {
					keys = append(keys, k)
				}
				break
			}
		}
	}
	return keys, nil
}

// commit 是提交的唯一入口。冲突检测、时间戳分配、发布、GC、WAL
// 压缩在同一把提交锁内串行完成，因此结果完全确定、可复现，
// 不依赖 goroutine 调度时机。
func (s *Store) commit(t *Txn) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if err := checkLifetimeLocked(s, t); err != nil {
		s.mu.Unlock()
		s.finishTxn(t)
		return err
	}

	// 只读或空写集合：没有任何内容需要冲突检测或持久化。
	if t.readOnly || len(t.writes) == 0 {
		s.mu.Unlock()
		s.finishTxn(t)
		return nil
	}

	// 第 1 阶段（全程持提交锁）：冲突检测、容量校验、时间戳分配一次完成。
	// 它们必须与其他提交者的“版本发布”动作互斥，否则两个并发提交可能
	// 都在对方发布之前通过检测而双双提交（丢失更新）。互斥也使提交顺序
	// 与结果完全确定、可复现，不依赖 goroutine 调度。
	ops, delta, commitTS, verr := s.validateAndStampLocked(t)
	if verr != nil {
		s.finishTxnLocked(t)
		s.mu.Unlock()
		return verr
	}

	// 只读或空写集合：没有需要持久化的内容（validate 已短路）。
	if len(ops) == 0 {
		s.finishTxnLocked(t)
		s.mu.Unlock()
		return nil
	}

	// 第 2 阶段（仍持提交锁）：先持久化（fsync），再发布到内存。
	// 崩溃时要么整条记录存在，要么根本不存在
	// （帧带 CRC，尾部半条会在恢复时被忽略）。
	if err := s.wal.appendCommit(commitTS, ops); err != nil {
		// 编号不回退：只要求唯一单调，稀疏无害；若帧其实已落盘只是
		// fsync 报错，回退反而会造成时间戳复用。
		s.finishTxnLocked(t)
		s.mu.Unlock()
		return err
	}
	for _, op := range ops {
		head := s.versions[op.key]
		nv := &version{commitTS: commitTS, txnID: t.id, delete: op.delete, prev: head}
		if !op.delete {
			nv.value = append([]byte(nil), op.value...)
		}
		s.versions[op.key] = nv
	}
	s.liveBytes += delta
	if commitTS > s.committedHighTS {
		s.committedHighTS = commitTS
	}
	// 先把本事务移出活跃集合，再做 GC：提交者自己的快照停留在过去，
	// 若仍把它算作“活跃读者”，GC 会过度保守，连无人需要的墓碑都无法回收。
	s.finishTxnLocked(t)
	s.gcLocked()
	needCompact := s.opts.MaxWALBytes >= 0 && s.wal.bytes() >= s.opts.MaxWALBytes
	s.mu.Unlock()

	// 第 3 阶段：WAL 超阈值时物化快照并重写 WAL。为保证“快照 + WAL”
	// 整体原子（不会丢掉压缩窗口内的新提交），压缩全程持提交锁。
	if needCompact {
		if err := s.compactWAL(); err != nil {
			// 压缩失败不影响已提交数据的正确性（旧 WAL 仍完整），
			// 后续提交会重试压缩。
			return nil
		}
	}
	return nil
}

func checkLifetimeLocked(s *Store, t *Txn) error {
	if s.opts.MaxTxnLifetime < 0 {
		return nil
	}
	if s.clock.now().After(t.deadline) {
		return ErrTxnTimeout
	}
	return nil
}

// validateAndStampLocked 在持有提交锁的前提下完成首提交者胜冲突检测、
// 容量边界校验与提交时间戳分配。只读/空写事务返回空 ops 与零时间戳。
func (s *Store) validateAndStampLocked(t *Txn) (ops []writeOp, delta int64, commitTS uint64, err error) {
	if t.readOnly || len(t.writes) == 0 {
		return nil, 0, 0, nil
	}
	ops = make([]writeOp, 0, len(t.writeOrder))
	for _, k := range t.writeOrder {
		op := t.writes[k]
		if head := s.versions[k]; head != nil && head.commitTS > t.snapshotTS {
			// 该键在本事务快照之后已被别的事务抢先提交：拒绝后提交者。
			return nil, 0, 0, &ConflictError{
				Key: k, WinnerID: head.txnID, LoserID: t.id,
			}
		}
		ops = append(ops, op)
	}
	d := s.liveDeltaLocked(ops)
	if s.opts.MaxLiveBytes >= 0 && s.liveBytes+d > s.opts.MaxLiveBytes {
		return nil, 0, 0, ErrStorageLimit
	}
	s.seq++
	return ops, d, s.seq, nil
}

// liveDeltaLocked 计算应用 ops 后“最新版本总字节”的增量。
func (s *Store) liveDeltaLocked(ops []writeOp) int64 {
	var delta int64
	// 同一事务内每个键只出现一次（写缓冲按键合并），可直接基于当前 head 计算。
	for _, op := range ops {
		head := s.versions[op.key]
		var oldSize int64
		if head != nil && !head.delete {
			oldSize = entryBytes(op.key, head.value)
		}
		if op.delete {
			delta -= oldSize
		} else {
			delta += entryBytes(op.key, op.value) - oldSize
		}
	}
	return delta
}

// gcLocked 回收任何活跃快照都不可能再看到的旧版本。
//
// 设 cutoff = 所有活跃事务中最老的 snapshotTS。版本链上
//   - commitTS > cutoff 的节点：对旧快照不可见，但冲突检测与新快照
//     可能需要，全部保留；
//   - 从链头往下第一个 commitTS <= cutoff 的节点 v 是旧快照的“可见点”。
//
// 注意：即使 v 是墓碑，v 的存活前驱仍是比 v 更老的快照的内容来源——
// 但那些快照的时间戳必然 < v.commitTS <= cutoff，即比 cutoff 更老，
// 不可能是当前任何活跃事务。因此只需保留 v，v.prev 可安全断开；
// 而当 v 是墓碑且链上没有更新的存活版本时，整条链（含墓碑）可删除。
func (s *Store) gcLocked() {
	var cutoff uint64
	hasActive := false
	for t := range s.active {
		if !hasActive || t.snapshotTS < cutoff {
			cutoff = t.snapshotTS
			hasActive = true
		}
	}
	for key, head := range s.versions {
		if !hasActive {
			// 没有任何旧快照：仅链头可能被新事务的冲突检测引用。
			if head.delete {
				delete(s.versions, key)
			} else {
				head.prev = nil
			}
			continue
		}

		// 找链上第一个 commitTS <= cutoff 的节点作为旧快照可见点。
		v := head
		for v != nil && v.commitTS > cutoff {
			v = v.prev
		}
		switch {
		case v == nil:
			// 整条链都比 cutoff 新：全部需要保留，不动。
		case v == head:
			// 链头就是可见点：更早版本全部回收；
			// 链头是墓碑时整条删除（没有更新的存活版本需要保留）。
			if v.delete {
				delete(s.versions, key)
			} else {
				v.prev = nil
			}
		default:
			// 可见点 v 之前还有更新的节点（对旧快照不可见，须保留）。
			// 可见点若为墓碑，保留它（让旧快照看到“已删除”），但不能
			// 连带删掉更新节点；只断开 v 的更老前驱。
			v.prev = nil
		}
	}
}

// compactWAL 物化当前最新状态并重写 WAL。
//
// 关键不变量：构建快照与重写 WAL 必须在同一把提交锁内原子完成。
// 否则可能交错出“快照不含事务 T、重写后的 WAL 也不含 T 的记录”
// （T 的记录在旧 WAL 中被丢弃），崩溃后 T 就会丢失。
// 代价是压缩期间的 fsync 会短暂阻塞读者与提交，换取严格正确性。
func (s *Store) compactWAL() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	live := make(map[string][]byte, len(s.versions))
	for k, head := range s.versions {
		if head == nil || head.delete {
			continue
		}
		live[k] = append([]byte(nil), head.value...)
	}
	return s.wal.compact(live)
}

// finishTxn 把从事务活跃集合移除的逻辑集中到一处（提交或回滚共用）。
// 绝不与 commit 的主临界区嵌套：commit 路径使用 finishTxnLocked。
func (s *Store) finishTxn(t *Txn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finishTxnLocked(t)
}

// finishTxnLocked 要求调用方已持有 s.mu。
func (s *Store) finishTxnLocked(t *Txn) {
	if t.closed {
		return
	}
	if _, ok := s.active[t]; ok {
		delete(s.active, t)
		s.activeCount--
	}
	t.closed = true
	// 清空缓冲引用，帮助 GC 回收值字节。
	t.writes = nil
	t.writeOrder = nil
}

// entryBytes 统计一条最新版本键值在“逻辑数据量”口径下的字节数。
func entryBytes(key string, value []byte) int64 {
	return int64(len(key) + len(value))
}
