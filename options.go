package mvcc

import "time"

// 资源边界的默认值。它们刻意选择“足够大、不会误伤正常负载，
// 但能防止无界增长”的量级；调用方可通过 Options 逐项覆盖。
const (
	defaultMaxActiveTxns   = 1024
	defaultMaxWritesPerTxn = 100_000
	defaultMaxTxnLifetime  = time.Minute
	defaultMaxLiveBytes    = int64(1) << 30  // 1 GiB 最新版本数据
	defaultMaxWALBytes     = int64(64) << 20 // 64 MiB 后自动快照压缩
)

// Options 定义存储层的可配置资源边界。
// 零值字段会在 Open 中被填充为默认值，因此调用方只需覆盖关心的字段。
// 负数用于表示“不限制”的语义（除 Dir 与 MaxWALBytes 外）。
type Options struct {
	// Dir 是持久化目录（WAL 与快照所在位置）。
	Dir string
	// MaxActiveTxns 是同时存活的事务数上限；0=默认值，负数=不限制。
	MaxActiveTxns int
	// MaxWritesPerTxn 是单个事务允许缓冲的写操作（Put/Delete）条数上限；0=默认值，负数=不限制。
	MaxWritesPerTxn int
	// MaxTxnLifetime 是单个事务允许的最长存活时间；0=默认值，负数=不限制。
	MaxTxnLifetime time.Duration
	// MaxLiveBytes 是已提交数据（最新版本键值）总字节数的软上限；0=默认值，负数=不限制。
	MaxLiveBytes int64
	// MaxWALBytes 是 WAL 自动压缩前的字节数软上限；0=默认值，负数=不压缩。
	MaxWALBytes int64
}

// DefaultOptions 返回带默认资源边界的选项。
func DefaultOptions() *Options {
	return &Options{
		MaxActiveTxns:   defaultMaxActiveTxns,
		MaxWritesPerTxn: defaultMaxWritesPerTxn,
		MaxTxnLifetime:  defaultMaxTxnLifetime,
		MaxLiveBytes:    defaultMaxLiveBytes,
		MaxWALBytes:     defaultMaxWALBytes,
	}
}

// normalize 将零值字段填充为默认值，并把“负数=不限”归一化为内部哨兵 0。
// 返回一个新的 Options，不修改接收者。
func (o *Options) normalize() *Options {
	n := DefaultOptions()
	if o == nil {
		return n
	}
	n.Dir = o.Dir
	if o.MaxActiveTxns != 0 {
		n.MaxActiveTxns = o.MaxActiveTxns
	}
	if o.MaxWritesPerTxn != 0 {
		n.MaxWritesPerTxn = o.MaxWritesPerTxn
	}
	if o.MaxTxnLifetime != 0 {
		n.MaxTxnLifetime = o.MaxTxnLifetime
	}
	if o.MaxLiveBytes != 0 {
		n.MaxLiveBytes = o.MaxLiveBytes
	}
	if o.MaxWALBytes != 0 {
		n.MaxWALBytes = o.MaxWALBytes
	}
	return n
}
