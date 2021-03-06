package dbutils

import (
	"bytes"
	"encoding/binary"

	"github.com/ledgerwatch/turbo-geth/common"
)

// EncodeBlockNumber encodes a block number as big endian uint64
func EncodeBlockNumber(number uint64) []byte {
	enc := make([]byte, 8)
	binary.BigEndian.PutUint64(enc, number)
	return enc
}

// headerKey = headerPrefix + num (uint64 big endian) + hash
func HeaderKey(number uint64, hash common.Hash) []byte {
	return append(EncodeBlockNumber(number), hash.Bytes()...)
}

func IsHeaderKey(k []byte) bool {
	l := common.BlockNumberLength + common.HashLength
	if len(k) != l {
		return false
	}

	return !IsHeaderHashKey(k) && !IsHeaderTDKey(k)
}

// headerTDKey = headerPrefix + num (uint64 big endian) + hash + headerTDSuffix
func HeaderTDKey(number uint64, hash common.Hash) []byte {
	return append(HeaderKey(number, hash), HeaderTDSuffix...)
}

func IsHeaderTDKey(k []byte) bool {
	l := common.BlockNumberLength + common.HashLength + 1
	return len(k) == l && bytes.Equal(k[l-1:], HeaderTDSuffix)
}

// headerHashKey = headerPrefix + num (uint64 big endian) + headerHashSuffix
func HeaderHashKey(number uint64) []byte {
	return append(EncodeBlockNumber(number), HeaderHashSuffix...)
}

func CheckCanonicalKey(k []byte) bool {
	return len(k) == 8+len(HeaderHashSuffix) && bytes.Equal(k[8:], HeaderHashSuffix)
}

func IsHeaderHashKey(k []byte) bool {
	l := common.BlockNumberLength + 1
	return len(k) == l && bytes.Equal(k[l-1:], HeaderHashSuffix)
}

// blockBodyKey = blockBodyPrefix + num (uint64 big endian) + hash
func BlockBodyKey(number uint64, hash common.Hash) []byte {
	return append(EncodeBlockNumber(number), hash.Bytes()...)
}

// ReceiptsKey = blockN (uint64 big endian)
func ReceiptsKey(blockNumber uint64) []byte {
	newK := make([]byte, 8)
	binary.BigEndian.PutUint64(newK, blockNumber)
	return newK
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
	compositeKey := make([]byte, 0, common.HashLength+common.IncarnationLength+common.HashLength)
	compositeKey = append(compositeKey, GenerateStoragePrefix(addressHash[:], incarnation)...)
	compositeKey = append(compositeKey, seckey[:]...)
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
	prefix := make([]byte, common.HashLength+8)
	copy(prefix, addressHash)
	binary.BigEndian.PutUint64(prefix[common.HashLength:], incarnation)
	return prefix
}

// address hash + incarnation prefix (for plain state)
func PlainGenerateStoragePrefix(address []byte, incarnation uint64) []byte {
	prefix := make([]byte, common.AddressLength+8)
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
