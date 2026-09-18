# mvcc-transactional-store

基于 MVCC（多版本并发控制）的嵌入式事务键值存储（Go）。

## 事务语义

- **快照隔离**：`Begin` 时事务获得一致的快照（`beginTS`）。事务开始后的任何并发
  提交写入、删除，对该事务的 `Get`/`Scan` 均不可见；事务内读取永远返回快照时刻
  的数据视图。
- **原子性**：事务的所有写入在 `Commit` 时一次性生效；`Rollback`、提交失败或
  事务过期时，缓冲写入全部丢弃，不产生任何部分效果，也不会污染其他事务。
- **读己之写**：事务可以读到自己在本次事务中缓冲的写入。
- **持久性**：提交时先写 WAL（预写日志）再应用内存状态；`SyncWrites: true`
  时每次提交都 fsync。进程崩溃后重开，已提交数据不丢失，未提交事务不可见。

## 冲突规则

采用 **first-committer-wins**（先提交者获胜）：

- 事务提交时检查其写集合中的每个键：若该键最新已提交版本的 `commitTS` 大于
  本事务的 `beginTS`，说明存在并发事务先行修改了同一键，提交被拒绝并返回
  `ErrConflict`（可用 `errors.Is` 区分）。
- 冲突事务自动结束，写入全部丢弃；获胜事务的值成为最终值——不存在丢失更新，
  也不存在静默覆盖。
- 写不相交键的并发事务互不冲突，可并行提交。
- WAL 中的记录按键排序写入，提交顺序与日志内容确定、可复现。

## 资源边界

| 配置项 | 默认 | 行为 |
|---|---|---|
| `MaxActiveTxns` | 128 | 活跃事务达到上限时 `Begin` 返回 `ErrTooManyTxns` |
| `MaxTxnDuration` | 60s | 超时事务的任何操作/提交返回 `ErrTxnExpired` 并自动回滚，槽位释放 |
| `MaxVersionsPerKey` | 16 | 每键保留的版本数上限；GC 同时保留最老活跃事务仍可见的版本 |
| `SyncWrites` | false | 是否每次提交 fsync WAL |

所有边界均按既定策略拒绝请求，不做无界增长。

## 崩溃恢复

- WAL 记录格式：`[len][crc32][payload]`，只追加写入。
- 启动时重放全部完好记录；损坏或不完整的尾部记录被截断，之后可继续正常写入。
- 恢复幂等：重复打开得到一致状态（相同 `commitTS` 的版本不会重复应用）。

## API 概览

```go
s, _ := mvcc.Open(mvcc.Config{Dir: "./data", SyncWrites: true})
defer s.Close()

tx, _ := s.Begin()
tx.Put([]byte("k"), []byte("v"))
v, err := tx.Get([]byte("k"))          // 快照读 + 读己之写
it := tx.Scan([]byte("a"), []byte("z")) // 快照范围扫描，结果物化
err = tx.Commit()                       // 冲突时返回 ErrConflict

// 单语句快捷方式（自动包裹事务）
s.Put(k, v); s.Get(k); s.Delete(k)
```

## 验证方式

```sh
go test ./...                 # 全部单元测试
CGO_ENABLED=1 go test -race ./...  # 竞态检测
```

测试覆盖：并发写冲突（`TestWriteConflictDeterministic`、
`TestConcurrentCommitsNoLostUpdate`）、快照一致性（`TestSnapshotIsolation`、
`TestScanSnapshotSemantics`）、异常恢复（`TestRecovery*`、
`TestRecoveryTruncatesCorruptTail`）、资源上限（`TestTooManyTxns`、
`TestTxnExpiry`、`TestVersionCountBounded`）。
