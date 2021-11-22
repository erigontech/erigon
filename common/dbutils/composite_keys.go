package dbutils

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon/common"
)

const NumberLength = 8

// EncodeBlockNumber encodes a block number as big endian uint64
func EncodeBlockNumber(number uint64) []byte {
	enc := make([]byte, NumberLength)
	binary.BigEndian.PutUint64(enc, number)
	return enc
}

var ErrInvalidSize = errors.New("bit endian number has an invalid size")

func DecodeBlockNumber(number []byte) (uint64, error) {
	if len(number) != NumberLength {
		return 0, fmt.Errorf("%w: %d", ErrInvalidSize, len(number))
	}
	return binary.BigEndian.Uint64(number), nil
}

// HeaderKey = num (uint64 big endian) + hash
func HeaderKey(number uint64, hash common.Hash) []byte {
	k := make([]byte, NumberLength+common.HashLength)
	binary.BigEndian.PutUint64(k, number)
	copy(k[NumberLength:], hash[:])
	return k
}

// BlockBodyKey = num (uint64 big endian) + hash
func BlockBodyKey(number uint64, hash common.Hash) []byte {
	k := make([]byte, NumberLength+common.HashLength)
	binary.BigEndian.PutUint64(k, number)
	copy(k[NumberLength:], hash[:])
	return k
}

// LogKey = blockN (uint64 big endian) + txId (uint32 big endian)
func LogKey(blockNumber uint64, txId uint32) []byte {
	newK := make([]byte, 8+4)
	binary.BigEndian.PutUint64(newK, blockNumber)
	binary.BigEndian.PutUint32(newK[8:], txId)
	return newK
}

// bloomBitsKey = bloomBitsPrefix + bit (uint16 big endian) + section (uint64 big endian) + hash
func BloomBitsKey(bit uint, section uint64, hash common.Hash) []byte {
	key := append(make([]byte, 10), hash.Bytes()...)

	binary.BigEndian.PutUint16(key[0:], uint16(bit))
	binary.BigEndian.PutUint64(key[2:], section)

	return key
}

// AddrHash + KeyHash
// Only for trie
func GenerateCompositeTrieKey(addressHash common.Hash, seckey common.Hash) []byte {
	compositeKey := make([]byte, 0, common.HashLength+common.HashLength)
	compositeKey = append(compositeKey, addressHash[:]...)
	compositeKey = append(compositeKey, seckey[:]...)
	return compositeKey
}

// AddrHash + incarnation + KeyHash
// For contract storage
func GenerateCompositeStorageKey(addressHash common.Hash, incarnation uint64, seckey common.Hash) []byte {
	compositeKey := make([]byte, common.HashLength+common.IncarnationLength+common.HashLength)
	copy(compositeKey, addressHash[:])
	binary.BigEndian.PutUint64(compositeKey[common.HashLength:], incarnation)
	copy(compositeKey[common.HashLength+common.IncarnationLength:], seckey[:])
	return compositeKey
}

func ParseCompositeStorageKey(compositeKey []byte) (common.Hash, uint64, common.Hash) {
	prefixLen := common.HashLength + common.IncarnationLength
	addrHash, inc := ParseStoragePrefix(compositeKey[:prefixLen])
	var key common.Hash
	copy(key[:], compositeKey[prefixLen:prefixLen+common.HashLength])
	return addrHash, inc, key
}

// AddrHash + incarnation + KeyHash
// For contract storage (for plain state)
func PlainGenerateCompositeStorageKey(address []byte, incarnation uint64, key []byte) []byte {
	compositeKey := make([]byte, common.AddressLength+common.IncarnationLength+common.HashLength)
	copy(compositeKey, address)
	binary.BigEndian.PutUint64(compositeKey[common.AddressLength:], incarnation)
	copy(compositeKey[common.AddressLength+common.IncarnationLength:], key)
	return compositeKey
}

func PlainParseCompositeStorageKey(compositeKey []byte) (common.Address, uint64, common.Hash) {
	prefixLen := common.AddressLength + common.IncarnationLength
	addr, inc := PlainParseStoragePrefix(compositeKey[:prefixLen])
	var key common.Hash
	copy(key[:], compositeKey[prefixLen:prefixLen+common.HashLength])
	return addr, inc, key
}

// AddrHash + incarnation + StorageHashPrefix
func GenerateCompositeStoragePrefix(addressHash []byte, incarnation uint64, storageHashPrefix []byte) []byte {
	key := make([]byte, common.HashLength+common.IncarnationLength+len(storageHashPrefix))
	copy(key, addressHash)
	binary.BigEndian.PutUint64(key[common.HashLength:], incarnation)
	copy(key[common.HashLength+common.IncarnationLength:], storageHashPrefix)
	return key
}

// address hash + incarnation prefix
func GenerateStoragePrefix(addressHash []byte, incarnation uint64) []byte {
	prefix := make([]byte, common.HashLength+NumberLength)
	copy(prefix, addressHash)
	binary.BigEndian.PutUint64(prefix[common.HashLength:], incarnation)
	return prefix
}

// address hash + incarnation prefix (for plain state)
func PlainGenerateStoragePrefix(address []byte, incarnation uint64) []byte {
	prefix := make([]byte, common.AddressLength+NumberLength)
	copy(prefix, address)
	binary.BigEndian.PutUint64(prefix[common.AddressLength:], incarnation)
	return prefix
}

func PlainParseStoragePrefix(prefix []byte) (common.Address, uint64) {
	var addr common.Address
	copy(addr[:], prefix[:common.AddressLength])
	inc := binary.BigEndian.Uint64(prefix[common.AddressLength : common.AddressLength+common.IncarnationLength])
	return addr, inc
}

func ParseStoragePrefix(prefix []byte) (common.Hash, uint64) {
	var addrHash common.Hash
	copy(addrHash[:], prefix[:common.HashLength])
	inc := binary.BigEndian.Uint64(prefix[common.HashLength : common.HashLength+common.IncarnationLength])
	return addrHash, inc
}

// Key + blockNum
func CompositeKeySuffix(key []byte, timestamp uint64) (composite, encodedTS []byte) {
	encodedTS = EncodeTimestamp(timestamp)
	composite = make([]byte, len(key)+len(encodedTS))
	copy(composite, key)
	copy(composite[len(key):], encodedTS)
	return composite, encodedTS
}
