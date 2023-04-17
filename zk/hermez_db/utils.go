package hermez_db

import (
	"encoding/binary"
	"fmt"
)

func BytesToUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(b)
}

func Uint64ToBytes(i uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, i)
	return buf
}

func BytesToUint8(b []byte) uint8 {
	if len(b) < 1 {
		return 0
	}
	return b[0]
}

func Uint8ToBytes(i uint8) []byte {
	buf := make([]byte, 1)
	buf[0] = i
	return buf
}

func SplitKey(data []byte) (uint64, uint64, error) {
	if len(data) != 16 {
		return 0, 0, fmt.Errorf("data length is not 16 bytes")
	}

	l1blockno := BytesToUint64(data[:8])
	batchno := BytesToUint64(data[8:])

	return l1blockno, batchno, nil
}

func ConcatKey(l1BlockNo, batchNo uint64) []byte {
	buf := make([]byte, 16)
	binary.BigEndian.PutUint64(buf[:8], l1BlockNo)
	binary.BigEndian.PutUint64(buf[8:], batchNo)
	return buf
}
