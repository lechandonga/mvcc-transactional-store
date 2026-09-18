package mvcc

import (
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// versionedValue 是某个键在特定提交时间戳上的一个版本。
type versionedValue struct {
	commitTS uint64
	value    []byte // nil 表示墓碑（删除）
}

// Store 是支持快照隔离的 MVCC 事务键值存储。
type Store struct {
	mu       sync.RWMutex
	data     map[string][]versionedValue // 每个键的版本按 commitTS 降序
	commitTS uint64
	nextID   uint64
	active   map[uint64]*Txn
	wal      *WAL
	cfg      Config
	closed   bool
}

// Open 打开存储；若配置了 Dir，则重放 WAL 恢复已提交状态。
// 恢复是幂等的：重复执行得到一致结果。
func Open(cfg Config) (*Store, error) {
	c := cfg.withDefaults()
	s := &Store{
		data:   make(map[string][]versionedValue),
		active: make(map[uint64]*Txn),
		cfg:    c,
	}
	if c.Dir != "" {
		if err := os.MkdirAll(c.Dir, 0o755); err != nil {
			return nil, err
		}
		w, err := openWAL(filepath.Join(c.Dir, "store.wal"))
		if err != nil {
			return nil, err
		}
		s.wal = w
		if err := w.replay(func(rec walRecord) error {
			s.applyRecord(rec)
			return nil
		}); err != nil {
			w.close()
			return nil, err
		}
	}
	return s, nil
}

// applyRecord 将一条已提交记录应用到内存版本链（需持锁或在恢复期间调用）。
func (s *Store) applyRecord(rec walRecord) {
	for _, op := range rec.ops {
		vv := versionedValue{commitTS: rec.commitTS, value: op.value}
		if op.deleted {
			vv.value = nil
		}
		k := string(op.key)
		versions := s.data[k]
		// 幂等：相同 commitTS 的版本不重复插入（恢复可重入）。
		if len(versions) > 0 && versions[0].commitTS >= rec.commitTS {
			continue
		}
		s.data[k] = append([]versionedValue{vv}, versions...)
	}
	if rec.commitTS > s.commitTS {
		s.commitTS = rec.commitTS
	}
}

// Begin 开启一个新事务，受 MaxActiveTxns 资源边界约束。
func (s *Store) Begin() (*Txn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrStoreClosed
	}
	if len(s.active) >= s.cfg.MaxActiveTxns {
		return nil, ErrTooManyTxns
	}
	s.nextID++
	t := &Txn{
		store:     s,
		id:        s.nextID,
		beginTS:   s.commitTS,
		writes:    make(map[string]*[]byte),
		expiresAt: time.Now().Add(s.cfg.MaxTxnDuration),
	}
	s.active[t.id] = t
	return t, nil
}

// visibleAt 返回键在 ts 快照上可见的版本；未找到返回 false。
// 调用方需至少持有读锁。
func (s *Store) visibleAt(key string, ts uint64) ([]byte, bool) {
	for _, v := range s.data[key] {
		if v.commitTS <= ts {
			if v.value == nil {
				return nil, false // 墓碑
			}
			return v.value, true
		}
	}
	return nil, false
}

// oldestActiveTS 返回最老活跃事务的 beginTS；无活跃事务时返回当前 commitTS。
// 调用方需持锁。
func (s *Store) oldestActiveTS() uint64 {
	oldest := s.commitTS
	for _, t := range s.active {
		if t.beginTS < oldest {
			oldest = t.beginTS
		}
	}
	return oldest
}

// gc 裁剪版本链：保留最老活跃事务仍可见的版本，且不超过 MaxVersionsPerKey。
// 调用方需持写锁。
func (s *Store) gc() {
	oldest := s.oldestActiveTS()
	maxV := s.cfg.MaxVersionsPerKey
	for k, versions := range s.data {
		keep := len(versions)
		// 找到最老活跃事务仍需的最旧版本位置。
		for i, v := range versions {
			if v.commitTS <= oldest {
				keep = i + 1
				break
			}
		}
		if keep > maxV {
			keep = maxV
		}
		versions = versions[:keep]
		if len(versions) == 1 && versions[0].value == nil {
			delete(s.data, k) // 只剩墓碑时可整体清除
			continue
		}
		s.data[k] = versions
	}
}

// snapshotKeys 返回 ts 快照上所有可见（非删除）键的有序副本。
// 调用方需至少持有读锁。
func (s *Store) snapshotKeys(ts uint64) ([]string, map[string][]byte) {
	keys := make([]string, 0, len(s.data))
	vals := make(map[string][]byte, len(s.data))
	for k := range s.data {
		if v, ok := s.visibleAt(k, ts); ok {
			keys = append(keys, k)
			vals[k] = v
		}
	}
	sort.Strings(keys)
	return keys, vals
}

// Get 是单语句只读快捷方式，等价于 Begin + Get + Commit。
func (s *Store) Get(key []byte) ([]byte, error) {
	tx, err := s.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return tx.Get(key)
}

// Put 是单语句写入快捷方式，等价于 Begin + Put + Commit。
func (s *Store) Put(key, value []byte) error {
	tx, err := s.Begin()
	if err != nil {
		return err
	}
	if err := tx.Put(key, value); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Delete 是单语句删除快捷方式。
func (s *Store) Delete(key []byte) error {
	tx, err := s.Begin()
	if err != nil {
		return err
	}
	if err := tx.Delete(key); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Close 关闭存储与 WAL。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.wal != nil {
		return s.wal.close()
	}
	return nil
}

