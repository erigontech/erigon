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

func IsHeaderHashKey(k []byte) bool {
	l := common.BlockNumberLength + 1
	return len(k) == l && bytes.Equal(k[l-1:], HeaderHashSuffix)
}

// headerNumberKey = headerNumberPrefix + hash
func HeaderNumberKey(hash common.Hash) []byte {
	return append(HeaderNumberPrefix, hash.Bytes()...)
}

// blockBodyKey = blockBodyPrefix + num (uint64 big endian) + hash
func BlockBodyKey(number uint64, hash common.Hash) []byte {
	return append(EncodeBlockNumber(number), hash.Bytes()...)
}

// blockReceiptsKey = blockReceiptsPrefix + num (uint64 big endian) + hash
func BlockReceiptsKey(number uint64, hash common.Hash) []byte {
	return append(EncodeBlockNumber(number), hash.Bytes()...)
}

// txLookupKey = txLookupPrefix + hash
func TxLookupKey(hash common.Hash) []byte {
	return append(TxLookupPrefix, hash.Bytes()...)
}

// bloomBitsKey = bloomBitsPrefix + bit (uint16 big endian) + section (uint64 big endian) + hash
func BloomBitsKey(bit uint, section uint64, hash common.Hash) []byte {
	key := append(make([]byte, 10), hash.Bytes()...)

	binary.BigEndian.PutUint16(key[0:], uint16(bit))
	binary.BigEndian.PutUint64(key[2:], section)

	return key
}

// preimageKey = preimagePrefix + hash
func PreimageKey(hash common.Hash) []byte {
	return append(PreimagePrefix, hash.Bytes()...)
}

// configKey = configPrefix + hash
func ConfigKey(hash common.Hash) []byte {
	return append(ConfigPrefix, hash.Bytes()...)
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
	compositeKey := make([]byte, 0, common.HashLength+8+common.HashLength)
	compositeKey = append(compositeKey, GenerateStoragePrefix(addressHash, incarnation)...)
	compositeKey = append(compositeKey, seckey[:]...)
	return compositeKey
}

// address hash + incarnation prefix
func GenerateStoragePrefix(addressHash common.Hash, incarnation uint64) []byte {
	prefix := make([]byte, common.HashLength+8)
	copy(prefix, addressHash[:])
	binary.BigEndian.PutUint64(prefix[common.HashLength:], incarnation^^uint64(0))
	return prefix
}

func DecodeIncarnation(buf []byte) uint64 {
	incarnation := binary.BigEndian.Uint64(buf)
	return incarnation ^ ^uint64(0)
}

func RemoveIncarnationFromKey(key []byte, buf *[]byte) {
	tmp := *buf
	if len(key) <= common.HashLength {
		tmp = append(tmp, key...)
	} else {
		tmp = append(tmp, key[:common.HashLength]...)
		tmp = append(tmp, key[common.HashLength+8:]...)
	}
	*buf = tmp
}

// Key + blockNum
func CompositeKeySuffix(key []byte, timestamp uint64) (composite, encodedTS []byte) {
	encodedTS = EncodeTimestamp(timestamp)
	composite = make([]byte, len(key)+len(encodedTS))
	copy(composite, key)
	copy(composite[len(key):], encodedTS)
	return composite, encodedTS
}

// blockNum + history bucket
func CompositeChangeSetKey(encodedTS, hBucket []byte) []byte {
	changeSetKey := make([]byte, len(encodedTS)+len(hBucket))
	copy(changeSetKey, encodedTS)
	copy(changeSetKey[len(encodedTS):], hBucket)
	return changeSetKey
}
