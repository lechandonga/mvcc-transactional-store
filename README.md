# mvcc-transactional-store

一个支持**快照隔离事务**、**写冲突检测**与 **WAL 崩溃恢复**的嵌入式键值存储
（Go，零第三方依赖）。数据保存在单个目录中：`wal.log`（预写日志）与
`snapshot.db`（检查点快照）。

## 快速开始

```go
import "github.com/lechandonga/mvcc-transactional-store"

store, err := mvcc.Open(&mvcc.Options{Dir: "./data"})
if err != nil { /* ... */ }
defer store.Close()

// 自动事务：fn 返回 nil 提交，返回错误或 panic 时回滚
err = store.Update(func(tx *mvcc.Txn) error {
    if err := tx.Put("user:1", []byte("alice")); err != nil {
        return err
    }
    v, err := tx.Get("user:1") // 读己之写（read-your-writes）
    _ = v
    return err
})

// 只读事务
err = store.View(func(tx *mvcc.Txn) error {
    rows, err := tx.Scan("user:", "user0") // [start, end) 字典序范围
    for _, r := range rows { fmt.Println(r.Key, string(r.Value)) }
    return err
})

// 也可以手动控制生命周期
tx, _ := store.Begin(false)
defer tx.Rollback() // 可安全重复调用
_ = tx.Put("k", []byte("v"))
if err := tx.Commit(); errors.Is(err, mvcc.ErrConflict) {
    // 发生写冲突，重试或放弃
}
```

## 事务语义

### 隔离级别：快照隔离（Snapshot Isolation）

- 每个事务在 `Begin` 时获得一个**读快照**（当时的已提交水位）。事务内
  `Get` / `Scan` 只能看到：
  - 快照时刻**已经提交**的版本；
  - 本事务自己的写入（read-your-writes）。
- 并发事务的提交不会影响进行中事务的视图；即使在扫描过程中其他事务
  持续写入，同一事务重复扫描的结果也完全一致（见
  `TestScanSnapshotSemantics`）。
- `Put` / `Delete` 先缓存在事务私有空间，**Commit 成功后才对外可见**。
- 失败、回滚、中止或进程消失的事务不会留下任何状态。

### 冲突规则：首提交者胜（First-Committer-Wins）

- 两个基于同一快照的并发事务写**同一个键**时，**先完成 Commit 的胜出**，
  后提交者整个事务被中止，返回 `*ConflictError`（可用 `errors.Is(err,
  mvcc.ErrConflict)` 判别类别，`errors.As` 取得细节）：

  ```go
  type ConflictError struct {
      Key      string // 冲突的键（写集合按首次写入顺序检查，确定可复现）
      WinnerID uint64 // 胜出（先提交）事务 ID
      LoserID  uint64 // 被中止事务 ID
  }
  ```

- 多键写集合是**原子**的：只要一个键冲突，整个事务不会部分生效。
- 写不相交键集的并发事务互不阻塞、互不冲突。
- 删除与写入同等参与冲突检测（对一个已被并发删除/修改的键提交写入会冲突）。
- 冲突检测、时间戳分配与版本发布在同一把提交锁内串行完成，因此结果
  **确定且可复现**，与 goroutine 调度无关；不会出现丢失更新或静默覆盖。
  被中止的安全做法是由调用方重试整个事务。

> 说明：当前实现采用快照隔离而非可串行化快照隔离（SSI）。SI 只检测
> 写写冲突，不检测读写依赖中的“只读反序异常”（write skew）；业务若需要
> 更强保证，可对共享判据键使用条件写入（把读条件提升为写）。

## 持久性与崩溃恢复

- 每个事务 Commit 时：先把一条带 **CRC32 校验**的记录追加进 `wal.log`
  并 `fsync`，成功后再发布到内存。因此崩溃时一条记录要么完整存在，
  要么不存在。
- `Open` 自动执行恢复：`snapshot.db`（若有）叠加重放 `wal.log` 中
  快照之后的已提交记录。恢复是**幂等**的，对同一目录重复 `Open`
  得到完全相同的状态。
- WAL 尾部因掉电产生的**半条记录**会被忽略并截断；WAL **中部**的
  CRC 损坏是硬错误，返回包裹内部 `corrupt` 判定的错误，绝不静默丢数据。
- WAL 超过 `MaxWALBytes` 时在提交后自动压缩：原子写入新快照（
  `snapshot.db.tmp` → rename → fsync 目录），再用仅含文件头的新 WAL
  原子替换旧 WAL。崩溃发生在压缩的任意阶段都安全：
  - 快照写好、WAL 未替换：重放时快照 + 旧 WAL 仍然得到同一状态；
  - WAL 已替换：新快照已落盘。
- 磁盘格式带 magic 与 `dataFormatVersion = 1`；读取到不支持的更高版本
  会显式报错而不是按旧格式误解析。

帧布局（小端序）：`bodyLen uint32 | crc32 uint32 | body`；
提交记录 body：`type=1 | txnID uint64 | count uint32 | [flags | keyLen | key | valLen? | val?]*`，
`flags` bit0 表示删除墓碑。快照文件以 `MSNP` 开头并以整体 CRC 收尾。

## 可配置的资源边界

`Options`（零值字段在 `Open` 时取默认值；负数表示“不限制”）：

| 字段 | 默认值 | 超限行为 |
| --- | --- | --- |
| `MaxActiveTxns` | 1024 | `Begin` 返回 `ErrTooManyTransactions` |
| `MaxWritesPerTxn` | 100 000（按唯一键计） | 第 N+1 个不同键的 `Put/Delete` 返回 `ErrTxnTooLarge`；同一键反复写只计一次 |
| `MaxTxnLifetime` | 1 分钟 | 超时后该事务的任何操作与提交返回 `ErrTxnTimeout` |
| `MaxLiveBytes` | 1 GiB（按 key+value 字节计） | 超限事务被整体拒绝，返回 `ErrStorageLimit`；删除/缩小可释放预算 |
| `MaxWALBytes` | 64 MiB | 超过后下一次提交触发快照压缩并重写 WAL；负数关闭自动压缩 |

错误均为可 `errors.Is` 判别的哨兵值：`ErrNotFound`、`ErrConflict`、
`ErrTxnClosed`、`ErrReadOnly`、`ErrTooManyTransactions`、`ErrTxnTooLarge`、
`ErrTxnTimeout`、`ErrStorageLimit`、`ErrClosed`。

## 验证方式

```bash
go test ./...            # 全部自动化测试
go test ./... -count=10  # 重复运行验证并发结果的确定性
go vet ./...
```

测试覆盖：

| 场景 | 测试 |
| --- | --- |
| 并发写冲突 / 首提交者胜 / 无丢失更新 | `TestConflictFirstCommitterWins`、`TestConflictOrderReversed`、`TestConflictDeleteVsPut`、`TestConflictMultiKeyWriteSet`、`TestConcurrentConflictsDeterministicCount` |
| 快照一致性 / 扫描快照语义 | `TestSnapshotReadStable`、`TestSnapshotDeleteVisibility`、`TestScanSnapshotSemantics`、`TestScanRanges` |
| 异常恢复 / 半条尾部 / 幂等 / 压缩恢复 / 中部损坏 | `TestRecoveryCommittedData`、`TestRecoveryUncommittedNotPersisted`、`TestRecoveryTornTail`、`TestRecoveryIdempotent`、`TestRecoveryFromSnapshotWithReplayedWAL`、`TestRecoveryCorruptMidWAL` |
| 资源上限 | `TestMaxActiveTransactions`、`TestMaxWritesPerTxn`、`TestMaxLiveBytes`、`TestTxnLifetimeLimit`、`TestLimitsDoNotBreakCompaction` |
| 混合高并发（长读快照 + 高冲突写 + GC） | `TestMixedConcurrentWorkload` |

## 实现要点

- 每个键维护**不可变版本链**（头为最新），读路径取链上第一个
  `commitTS <= 快照时间戳` 的版本，无锁可见（已发布版本不可变）。
- 后台式 GC 不需要：每次提交后，按“所有活跃事务中最老的快照”剪枝，
  回收不可能再被读到的旧版本与墓碑。
- 时间来源抽象为 `clock` 接口，寿命上限可在测试中确定性推进。
