package mvcc

import "time"

// Config 定义存储的资源边界与持久化选项。
type Config struct {
	// Dir 为 WAL 文件所在目录；为空则使用纯内存模式（不持久化）。
	Dir string
	// MaxActiveTxns 限制并发活跃事务数，0 表示默认值。
	MaxActiveTxns int
	// MaxTxnDuration 限制事务最大存活时长，超时事务提交将被拒绝，0 表示默认值。
	MaxTxnDuration time.Duration
	// MaxVersionsPerKey 限制每个键保留的最大历史版本数，0 表示默认值。
	MaxVersionsPerKey int
	// SyncWrites 为 true 时每次提交都 fsync WAL。
	SyncWrites bool
}

func (c *Config) withDefaults() Config {
	out := *c
	if out.MaxActiveTxns <= 0 {
		out.MaxActiveTxns = 128
	}
	if out.MaxTxnDuration <= 0 {
		out.MaxTxnDuration = 60 * time.Second
	}
	if out.MaxVersionsPerKey <= 0 {
		out.MaxVersionsPerKey = 16
	}
	return out
}
