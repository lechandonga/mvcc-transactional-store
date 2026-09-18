package mvcc

import "time"

// waitInTest 以毫秒为单位忙等 sleep，避免在测试里散落 time.Sleep。
func waitInTest(ms int) { time.Sleep(time.Duration(ms) * time.Millisecond) }
