package mvcc

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

// walRecord 是写入 WAL 的一条已提交记录。
type walRecord struct {
	commitTS uint64
	ops      []walOp
}

type walOp struct {
	key     []byte
	value   []byte // nil 表示删除（墓碑）
	deleted bool
}

// WAL 是只追加的预写日志，保证已提交数据在崩溃后可恢复。
// 记录格式：[payloadLen uint32][crc32 uint32][payload]，
// payload = commitTS(8) + opCount(4) + 各 op（keyLen/valueLen/标志/字节）。
type WAL struct {
	path string
	f    *os.File
	w    *bufio.Writer
}

// openWAL 打开（必要时创建）WAL 文件。
func openWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{path: path, f: f, w: bufio.NewWriter(f)}, nil
}

// append 追加一条提交记录并按配置决定是否 fsync。
func (w *WAL) append(rec walRecord, sync bool) error {
	payload := encodeRecord(rec)
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc32.ChecksumIEEE(payload))
	if _, err := w.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.w.Write(payload); err != nil {
		return err
	}
	if err := w.w.Flush(); err != nil {
		return err
	}
	if sync {
		return w.f.Sync()
	}
	return nil
}

// replay 从头读取 WAL，逐条调用 fn。损坏或不完整的尾部记录被截断，
// 使恢复过程可重复执行且结果稳定。
func (w *WAL) replay(fn func(walRecord) error) error {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(w.f)
	var offset int64
	for {
		var hdr [8]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			break // EOF 或半条记录头：截断
		}
		payloadLen := binary.LittleEndian.Uint32(hdr[0:4])
		crc := binary.LittleEndian.Uint32(hdr[4:8])
		if payloadLen > 1<<30 {
			break // 明显损坏
		}
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			break // 半条记录体：截断
		}
		if crc32.ChecksumIEEE(payload) != crc {
			break // 校验失败：截断
		}
		rec, err := decodeRecord(payload)
		if err != nil {
			break
		}
		if err := fn(rec); err != nil {
			return err
		}
		offset += int64(8 + payloadLen)
	}
	// 截断到最后的完好位置，保证重复恢复结果一致。
	if err := w.f.Truncate(offset); err != nil {
		return err
	}
	_, err := w.f.Seek(offset, io.SeekStart)
	return err
}

func (w *WAL) close() error {
	if err := w.w.Flush(); err != nil {
		return err
	}
	return w.f.Close()
}

func encodeRecord(rec walRecord) []byte {
	size := 8 + 4
	for _, op := range rec.ops {
		size += 4 + 4 + 1 + len(op.key) + len(op.value)
	}
	buf := make([]byte, 0, size)
	buf = binary.LittleEndian.AppendUint64(buf, rec.commitTS)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(rec.ops)))
	for _, op := range rec.ops {
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(op.key)))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(op.value)))
		var flag byte
		if op.deleted {
			flag = 1
		}
		buf = append(buf, flag)
		buf = append(buf, op.key...)
		buf = append(buf, op.value...)
	}
	return buf
}

func decodeRecord(payload []byte) (walRecord, error) {
	var rec walRecord
	buf := payload
	if len(buf) < 12 {
		return rec, errors.New("mvcc: wal record too short")
	}
	rec.commitTS = binary.LittleEndian.Uint64(buf[0:8])
	n := binary.LittleEndian.Uint32(buf[8:12])
	buf = buf[12:]
	for i := uint32(0); i < n; i++ {
		if len(buf) < 9 {
			return rec, errors.New("mvcc: wal op truncated")
		}
		klen := binary.LittleEndian.Uint32(buf[0:4])
		vlen := binary.LittleEndian.Uint32(buf[4:8])
		deleted := buf[8] == 1
		buf = buf[9:]
		if uint64(len(buf)) < uint64(klen)+uint64(vlen) {
			return rec, errors.New("mvcc: wal op data truncated")
		}
		op := walOp{deleted: deleted}
		op.key = append([]byte(nil), buf[:klen]...)
		buf = buf[klen:]
		op.value = append([]byte(nil), buf[:vlen]...)
		buf = buf[vlen:]
		rec.ops = append(rec.ops, op)
	}
	return rec, nil
}
