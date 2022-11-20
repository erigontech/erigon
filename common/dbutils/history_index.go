package dbutils

import (
	"encoding/binary"

	"github.com/ledgerwatch/erigon-lib/common/length"
)

func AccountIndexChunkKey(key []byte, blockNumber uint64) []byte {
	blockNumBytes := make([]byte, length.Addr+8)
	copy(blockNumBytes, key)
	binary.BigEndian.PutUint64(blockNumBytes[length.Addr:], blockNumber)

	return blockNumBytes
}

func StorageIndexChunkKey(key []byte, blockNumber uint64) []byte {
	//remove incarnation and add block number
	blockNumBytes := make([]byte, length.Addr+length.Hash+8)
	copy(blockNumBytes, key[:length.Addr])
	copy(blockNumBytes[length.Addr:], key[length.Addr+length.Incarnation:])
	binary.BigEndian.PutUint64(blockNumBytes[length.Addr+length.Hash:], blockNumber)

	return blockNumBytes
}

func CompositeKeyWithoutIncarnation(key []byte) []byte {
	if len(key) == length.Hash*2+length.Incarnation {
		kk := make([]byte, length.Hash*2)
		copy(kk, key[:length.Hash])
		copy(kk[length.Hash:], key[length.Hash+length.Incarnation:])
		return kk
	}
	if len(key) == length.Addr+length.Hash+length.Incarnation {
		kk := make([]byte, length.Addr+length.Hash)
		copy(kk, key[:length.Addr])
		copy(kk[length.Addr:], key[length.Addr+length.Incarnation:])
		return kk
	}
	return key
}
