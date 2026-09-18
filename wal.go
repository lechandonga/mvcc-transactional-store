package mvcc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// dataFormatVersion 是持久化格式的版本号。
// 读取器必须识别无法理解的更高版本并报错，而不是静默破坏数据。
const dataFormatVersion uint16 = 1

const (
	walFileName  = "wal.log"
	snapFileName = "snapshot.db"
	tmpSuffix    = ".tmp"

	walMagic  = "MWAL" // MVCC WAL
	snapMagic = "MSNP" // MVCC Snapshot

	recCommit  uint8 = 1 // 已提交事务记录
	flagDelete uint8 = 1 // 写操作为删除（墓碑）
)

// 帧布局（小端）：
//
//	bodyLen uint32 | crc32(body) uint32 | body ...
//
// WAL 文件头：magic "MWAL" + version uint16。
// 提交记录 body：type=1 uint8 | txnID uint64 | count uint32 |
//
//	[ flags uint8 | keyLen uint32 | key | valLen uint32 | val ]*
//
// 删除记录没有 val（valLen 省略，flags bit0=1）。
var errCorrupt = errors.New("mvcc: persistent data is corrupt")

var crcTable = crc32.MakeTable(crc32.IEEE)

// walWriter 以 append-only 方式写 WAL，每次提交后 fsync。
type walWriter struct {
	dir string
	f   *os.File
	sz  int64
	mu  sync.Mutex
}

// openWAL 打开或创建 WAL；新文件写入文件头，已有文件校验文件头。
func openWAL(dir string) (*walWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mvcc: create data dir: %w", err)
	}
	path := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("mvcc: open wal: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	w := &walWriter{dir: dir, f: f, sz: info.Size()}
	if w.sz == 0 {
		if err := w.writeHeader(); err != nil {
			f.Close()
			return nil, err
		}
	} else if err := w.verifyHeader(); err != nil {
		f.Close()
		return nil, err
	}
	// 崩溃可能在文件尾留下半条记录。把它截断，保证后续追加紧接在
	// 最后一条有效帧之后，而不是接在垃圾字节后面。
	if err := w.truncateToLastValidFrame(); err != nil {
		f.Close()
		return nil, err
	}
	return w, nil
}

// truncateToLastValidFrame 扫描 WAL，丢弃结尾的不完整帧（与 readWAL 的
// 容忍规则一致：帧头/body 不完整，或最后一帧 CRC 不符）。
func (w *walWriter) truncateToLastValidFrame() error {
	data := make([]byte, w.sz)
	if _, err := w.f.ReadAt(data, 0); err != nil {
		return err
	}
	pos := 6
	for pos < len(data) {
		if pos+8 > len(data) {
			break
		}
		bodyLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		wantCRC := binary.LittleEndian.Uint32(data[pos+4 : pos+8])
		end := pos + 8 + bodyLen
		if end > len(data) {
			break
		}
		if crc32.Checksum(data[pos+8:end], crcTable) != wantCRC {
			if end == len(data) {
				break // 容忍最后一帧损坏（疑似崩溃残留）
			}
			return fmt.Errorf("%w: wal crc mismatch", errCorrupt)
		}
		pos = end
	}
	if int64(pos) == w.sz {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Truncate(int64(pos)); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.sz = int64(pos)
	return nil
}

func (w *walWriter) writeHeader() error {
	buf := make([]byte, 0, 6)
	buf = append(buf, walMagic...)
	buf = binary.LittleEndian.AppendUint16(buf, dataFormatVersion)
	if _, err := w.f.Write(buf); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	w.sz += int64(len(buf))
	return nil
}

func (w *walWriter) verifyHeader() error {
	buf := make([]byte, 6)
	if _, err := w.f.ReadAt(buf, 0); err != nil {
		return err
	}
	if string(buf[:4]) != walMagic {
		return fmt.Errorf("%w: bad wal magic", errCorrupt)
	}
	if v := binary.LittleEndian.Uint16(buf[4:6]); v != dataFormatVersion {
		return fmt.Errorf("%w: unsupported wal version %d", errCorrupt, v)
	}
	return nil
}

// appendCommit 以一条 CRC 保护的记录持久化一个已提交事务，并 fsync。
func (w *walWriter) appendCommit(txnID uint64, ops []writeOp) error {
	body := make([]byte, 0, 16)
	body = append(body, recCommit)
	body = binary.LittleEndian.AppendUint64(body, txnID)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(ops)))
	for _, op := range ops {
		var flags uint8
		if op.delete {
			flags |= flagDelete
		}
		body = append(body, flags)
		body = binary.LittleEndian.AppendUint32(body, uint32(len(op.key)))
		body = append(body, op.key...)
		if !op.delete {
			body = binary.LittleEndian.AppendUint32(body, uint32(len(op.value)))
			body = append(body, op.value...)
		}
	}
	frame := make([]byte, 0, 8+len(body))
	frame = binary.LittleEndian.AppendUint32(frame, uint32(len(body)))
	frame = binary.LittleEndian.AppendUint32(frame, crc32.Checksum(body, crcTable))
	frame = append(frame, body...)

	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := w.f.Write(frame); err != nil {
		return fmt.Errorf("mvcc: write wal: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("mvcc: sync wal: %w", err)
	}
	w.sz += int64(len(frame))
	return nil
}

func (w *walWriter) bytes() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.sz
}

// compact 用快照+空 WAL 原子替换当前持久化状态。
// 调用方必须保证期间没有其他 append（持有提交锁）。
func (w *walWriter) compact(live map[string][]byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := writeSnapshotLocked(w.dir, live); err != nil {
		return err
	}
	// 用仅含文件头的新 WAL 原子替换旧 WAL。
	tmp := make([]byte, 0, 6)
	tmp = append(tmp, walMagic...)
	tmp = binary.LittleEndian.AppendUint16(tmp, dataFormatVersion)
	tmpPath := filepath.Join(w.dir, walFileName+tmpSuffix)
	if err := os.WriteFile(tmpPath, tmp, 0o644); err != nil {
		return err
	}
	f, err := os.OpenFile(tmpPath, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, filepath.Join(w.dir, walFileName)); err != nil {
		return err
	}
	if err := syncDir(w.dir); err != nil {
		return err
	}
	if err := w.f.Close(); err != nil {
		return err
	}
	nf, err := os.OpenFile(filepath.Join(w.dir, walFileName),
		os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	w.f = nf
	w.sz = int64(len(tmp))
	return nil
}

func (w *walWriter) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.f.Sync(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}

// committedRecord 是恢复时见到的一条已提交事务记录。
type committedRecord struct {
	txnID uint64
	ops   []writeOp
}

// readWAL 解析并校验 WAL 中的全部完整记录。
// 崩溃可能在文件尾留下半条记录（部分写入或 CRC 不符），一律忽略；
// 文件中部的 CRC 损坏则作为硬错误返回，避免静默丢数据。
func readWAL(dir string) ([]committedRecord, error) {
	data, err := os.ReadFile(filepath.Join(dir, walFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < 6 || string(data[:4]) != walMagic {
		return nil, fmt.Errorf("%w: bad wal magic", errCorrupt)
	}
	if v := binary.LittleEndian.Uint16(data[4:6]); v != dataFormatVersion {
		return nil, fmt.Errorf("%w: unsupported wal version %d", errCorrupt, v)
	}
	pos := 6
	var out []committedRecord
	for pos < len(data) {
		// 帧头不完整：崩溃残留尾部，停止。
		if pos+8 > len(data) {
			break
		}
		bodyLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		wantCRC := binary.LittleEndian.Uint32(data[pos+4 : pos+8])
		end := pos + 8 + bodyLen
		if end > len(data) {
			// body 不完整：崩溃残留尾部，停止。
			break
		}
		body := data[pos+8 : end]
		if crc32.Checksum(body, crcTable) != wantCRC {
			if end == len(data) {
				// 最后一帧 CRC 不符：视为崩溃残留，忽略。
				break
			}
			return nil, fmt.Errorf("%w: wal crc mismatch", errCorrupt)
		}
		rec, err := decodeCommitRecord(body)
		if err != nil {
			if end == len(data) {
				break
			}
			return nil, err
		}
		out = append(out, rec)
		pos = end
	}
	return out, nil
}

func decodeCommitRecord(body []byte) (committedRecord, error) {
	cur := 0
	get := func(n int) ([]byte, error) {
		if cur+n > len(body) {
			return nil, io.ErrUnexpectedEOF
		}
		b := body[cur : cur+n]
		cur += n
		return b, nil
	}
	t, err := get(1)
	if err != nil {
		return committedRecord{}, err
	}
	if t[0] != recCommit {
		return committedRecord{}, fmt.Errorf("%w: unknown record type %d", errCorrupt, t[0])
	}
	b, err := get(8)
	if err != nil {
		return committedRecord{}, err
	}
	txnID := binary.LittleEndian.Uint64(b)
	b, err = get(4)
	if err != nil {
		return committedRecord{}, err
	}
	count := int(binary.LittleEndian.Uint32(b))
	ops := make([]writeOp, 0, count)
	for i := 0; i < count; i++ {
		b, err = get(1)
		if err != nil {
			return committedRecord{}, err
		}
		flags := b[0]
		b, err = get(4)
		if err != nil {
			return committedRecord{}, err
		}
		klen := int(binary.LittleEndian.Uint32(b))
		kb, err := get(klen)
		if err != nil {
			return committedRecord{}, err
		}
		op := writeOp{key: string(kb), delete: flags&flagDelete != 0}
		if !op.delete {
			b, err = get(4)
			if err != nil {
				return committedRecord{}, err
			}
			vlen := int(binary.LittleEndian.Uint32(b))
			vb, err := get(vlen)
			if err != nil {
				return committedRecord{}, err
			}
			op.value = append([]byte(nil), vb...)
		}
		ops = append(ops, op)
	}
	return committedRecord{txnID: txnID, ops: ops}, nil
}

// 快照布局：magic "MSNP" | version uint16 | count uint64 |
//
//	(keyLen uint32 | key | valLen uint32 | val)* | crc32 uint32
//
// 快照只包含每个键的最新值（不含墓碑）。
func writeSnapshotLocked(dir string, live map[string][]byte) error {
	buf := make([]byte, 0, 64)
	buf = append(buf, snapMagic...)
	buf = binary.LittleEndian.AppendUint16(buf, dataFormatVersion)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(len(live)))
	for k, v := range live {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(k)))
		buf = append(buf, k...)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(v)))
		buf = append(buf, v...)
	}
	crc := crc32.Checksum(buf, crcTable)
	buf = binary.LittleEndian.AppendUint32(buf, crc)

	tmpPath := filepath.Join(dir, snapFileName+tmpSuffix)
	finalPath := filepath.Join(dir, snapFileName)
	if err := os.WriteFile(tmpPath, buf, 0o644); err != nil {
		return err
	}
	f, err := os.OpenFile(tmpPath, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return err
	}
	return syncDir(dir)
}

// readSnapshot 读取快照；快照不存在时返回 (nil, nil)。
func readSnapshot(dir string) (map[string][]byte, error) {
	data, err := os.ReadFile(filepath.Join(dir, snapFileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(data) < 10+4 || string(data[:4]) != snapMagic {
		return nil, fmt.Errorf("%w: bad snapshot magic", errCorrupt)
	}
	if v := binary.LittleEndian.Uint16(data[4:6]); v != dataFormatVersion {
		return nil, fmt.Errorf("%w: unsupported snapshot version %d", errCorrupt, v)
	}
	wantCRC := binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.Checksum(data[:len(data)-4], crcTable) != wantCRC {
		return nil, fmt.Errorf("%w: snapshot crc mismatch", errCorrupt)
	}
	pos := 6
	count := binary.LittleEndian.Uint64(data[pos : pos+8])
	pos += 8
	live := make(map[string][]byte, count)
	for i := uint64(0); i < count; i++ {
		if pos+4 > len(data)-4 {
			return nil, io.ErrUnexpectedEOF
		}
		klen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if pos+klen+4 > len(data)-4 {
			return nil, io.ErrUnexpectedEOF
		}
		key := string(data[pos : pos+klen])
		pos += klen
		vlen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if pos+vlen > len(data)-4 {
			return nil, io.ErrUnexpectedEOF
		}
		val := append([]byte(nil), data[pos:pos+vlen]...)
		pos += vlen
		live[key] = val
	}
	return live, nil
}

// recoverState 合并快照与 WAL，重放出一致的逻辑状态。
// 重放是纯函数式的：快照是 WAL 前缀的物化，重复应用同样的记录序列
// 得到同样的结果，因此崩溃在“快照已写、WAL 未重写”之间也安全。
func recoverState(dir string) (live map[string][]byte, lastTxnID uint64, err error) {
	live, err = readSnapshot(dir)
	if err != nil {
		return nil, 0, err
	}
	if live == nil {
		live = make(map[string][]byte)
	}
	recs, err := readWAL(dir)
	if err != nil {
		return nil, 0, err
	}
	for _, rec := range recs {
		if rec.txnID > lastTxnID {
			lastTxnID = rec.txnID
		}
		for _, op := range rec.ops {
			if op.delete {
				delete(live, op.key)
			} else {
				live[op.key] = append([]byte(nil), op.value...)
			}
		}
	}
	return live, lastTxnID, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
