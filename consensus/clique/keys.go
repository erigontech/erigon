package clique

import (
	"encoding/binary"

	"github.com/ledgerwatch/erigon/common"
)

// SnapshotFullKey = SnapshotBucket + num (uint64 big endian) + hash
func SnapshotFullKey(number uint64, hash common.Hash) []byte {
	return append(EncodeBlockNumber(number), hash.Bytes()...)
}

// SnapshotKey = SnapshotBucket + num (uint64 big endian)
func SnapshotKey(number uint64) []byte {
	return EncodeBlockNumber(number)
}

// SnapshotKey = SnapshotBucket + '0'
func LastSnapshotKey() []byte {
	return []byte{0}
}

const NumberLength = 8

// EncodeBlockNumber encodes a block number as big endian uint64
func EncodeBlockNumber(number uint64) []byte {
	enc := make([]byte, NumberLength)
	binary.BigEndian.PutUint64(enc, number)
	return enc
}
