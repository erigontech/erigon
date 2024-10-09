package hermez_db

import (
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
)

var emptyHash = common.Hash{0}

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

func ConcatGerKey(blockNo uint64, l1BlockHash common.Hash) []byte {
	buf := make([]byte, 40)
	binary.BigEndian.PutUint64(buf[:8], blockNo)

	// if the l1blockhash is zero - pre-etrog, don't append
	if l1BlockHash == emptyHash {
		return buf[:8]
	}

	copy(buf[8:], l1BlockHash[:])
	return buf
}

func SplitGerKey(data []byte) (uint64, *common.Hash, error) {
	if len(data) != 40 && len(data) != 8 {
		return 0, nil, fmt.Errorf("data length is not 8 bytes or 40 bytes")
	}

	if len(data) == 8 {
		return BytesToUint64(data), nil, nil
	}

	blockNo := BytesToUint64(data[:8])
	l1BlockHash := common.BytesToHash(data[8:])

	return blockNo, &l1BlockHash, nil
}
