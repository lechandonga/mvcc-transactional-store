package mvcc

import (
	"errors"
	"strconv"
)

// 事务与存储层可能返回的哨兵错误。调用方可用 errors.Is 判定类别，
// 用 errors.As 取得 *ConflictError 等细节。
var (
	// ErrNotFound 表示键在事务快照中不存在（或已被删除）。
	ErrNotFound = errors.New("mvcc: key not found")
	// ErrConflict 表示写冲突（首提交者胜，后提交者被中止）。
	ErrConflict = errors.New("mvcc: write conflict")
	// ErrTxnClosed 表示事务已经提交或回滚，不能再使用。
	ErrTxnClosed = errors.New("mvcc: transaction already closed")
	// ErrReadOnly 表示在只读事务中执行了写操作。
	ErrReadOnly = errors.New("mvcc: transaction is read-only")
	// ErrTooManyTransactions 表示活跃事务数已达上限。
	ErrTooManyTransactions = errors.New("mvcc: too many concurrent transactions")
	// ErrTxnTooLarge 表示单个事务的写入条数已达上限。
	ErrTxnTooLarge = errors.New("mvcc: transaction write set too large")
	// ErrTxnTimeout 表示事务存活时间超过允许的上限。
	ErrTxnTimeout = errors.New("mvcc: transaction exceeded maximum lifetime")
	// ErrStorageLimit 表示存储占用达到上限，写入被拒绝。
	ErrStorageLimit = errors.New("mvcc: storage size limit exceeded")
)

// ConflictError 携带写冲突的确定性细节：
// 哪个键冲突、谁先提交（胜出）、谁被拒绝（落败）。
type ConflictError struct {
	Key      string
	WinnerID uint64
	LoserID  uint64
}

func (e *ConflictError) Error() string {
	return "mvcc: write conflict on key " + strconvQuote(e.Key) +
		": txn " + itoa(e.LoserID) + " conflicts with committed txn " + itoa(e.WinnerID)
}

// Is 使 errors.Is(err, ErrConflict) 对 *ConflictError 成立。
func (e *ConflictError) Is(target error) bool { return target == ErrConflict }

func strconvQuote(s string) string { return strconv.Quote(s) }
func itoa(u uint64) string         { return strconv.FormatUint(u, 10) }

// ErrClosed 表示存储已关闭，不能再开始或操作事务。
var ErrClosed = errors.New("mvcc: store is closed")
