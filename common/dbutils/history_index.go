package dbutils

import (
	"encoding/binary"

	"github.com/ledgerwatch/erigon/common"
)

func AccountIndexChunkKey(key []byte, blockNumber uint64) []byte {
	blockNumBytes := make([]byte, common.AddressLength+8)
	copy(blockNumBytes, key)
	binary.BigEndian.PutUint64(blockNumBytes[common.AddressLength:], blockNumber)

	return blockNumBytes
}

func StorageIndexChunkKey(key []byte, blockNumber uint64) []byte {
	//remove incarnation and add block number
	blockNumBytes := make([]byte, common.AddressLength+common.HashLength+8)
	copy(blockNumBytes, key[:common.AddressLength])
	copy(blockNumBytes[common.AddressLength:], key[common.AddressLength+common.IncarnationLength:])
	binary.BigEndian.PutUint64(blockNumBytes[common.AddressLength+common.HashLength:], blockNumber)

	return blockNumBytes
}

func CompositeKeyWithoutIncarnation(key []byte) []byte {
	if len(key) == common.HashLength*2+common.IncarnationLength {
		kk := make([]byte, common.HashLength*2)
		copy(kk, key[:common.HashLength])
		copy(kk[common.HashLength:], key[common.HashLength+common.IncarnationLength:])
		return kk
	}
	if len(key) == common.AddressLength+common.HashLength+common.IncarnationLength {
		kk := make([]byte, common.AddressLength+common.HashLength)
		copy(kk, key[:common.AddressLength])
		copy(kk[common.AddressLength:], key[common.AddressLength+common.IncarnationLength:])
		return kk
	}
	return key
}
