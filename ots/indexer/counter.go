package indexer

import (
	"encoding/binary"

	"github.com/ledgerwatch/erigon-lib/common/length"
)

func OptimizedCounterSerializer(count uint64) []byte {
	v := make([]byte, 1) // 1 byte (counter - 1) [0, 255]
	v[0] = byte(count - 1)
	return v
}

func RegularCounterSerializer(count uint64, chunk []byte) []byte {
	// key == address
	// value (dup) == accumulated counter uint64 + chunk uint64
	v := make([]byte, length.Counter+length.Chunk)
	binary.BigEndian.PutUint64(v, count)
	copy(v[length.Counter:], chunk)
	return v
}

func LastCounterSerializer(count uint64) []byte {
	res := make([]byte, length.Counter+length.Chunk)
	binary.BigEndian.PutUint64(res, count)
	binary.BigEndian.PutUint64(res[length.Counter:], ^uint64(0))

	return res
}
