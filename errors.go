package mvcc

import "errors"

var (
	// ErrConflict 表示写-写冲突：并发事务修改了同一键。
	ErrConflict = errors.New("mvcc: write conflict")
	// ErrTxnExpired 表示事务超过配置的最大存活时长，被拒绝提交。
	ErrTxnExpired = errors.New("mvcc: transaction expired")
	// ErrTooManyTxns 表示活跃事务数达到配置上限。
	ErrTooManyTxns = errors.New("mvcc: too many active transactions")
	// ErrTxnDone 表示事务已提交或回滚，不能再使用。
	ErrTxnDone = errors.New("mvcc: transaction already finished")
	// ErrStoreClosed 表示存储已关闭。
	ErrStoreClosed = errors.New("mvcc: store closed")
	// ErrKeyNotFound 表示键不存在。
	ErrKeyNotFound = errors.New("mvcc: key not found")
)
