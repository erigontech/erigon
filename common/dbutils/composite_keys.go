package dbutils

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common/length"
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
	k := make([]byte, NumberLength+length.Hash)
	binary.BigEndian.PutUint64(k, number)
	copy(k[NumberLength:], hash[:])
	return k
}

// BlockBodyKey = num (uint64 big endian) + hash
func BlockBodyKey(number uint64, hash common.Hash) []byte {
	k := make([]byte, NumberLength+length.Hash)
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
	compositeKey := make([]byte, 0, length.Hash+length.Hash)
	compositeKey = append(compositeKey, addressHash[:]...)
	compositeKey = append(compositeKey, seckey[:]...)
	return compositeKey
}

// AddrHash + incarnation + KeyHash
// For contract storage
func GenerateCompositeStorageKey(addressHash common.Hash, incarnation uint64, seckey common.Hash) []byte {
	compositeKey := make([]byte, length.Hash+length.Incarnation+length.Hash)
	copy(compositeKey, addressHash[:])
	binary.BigEndian.PutUint64(compositeKey[length.Hash:], incarnation)
	copy(compositeKey[length.Hash+length.Incarnation:], seckey[:])
	return compositeKey
}

func ParseCompositeStorageKey(compositeKey []byte) (common.Hash, uint64, common.Hash) {
	prefixLen := length.Hash + length.Incarnation
	addrHash, inc := ParseStoragePrefix(compositeKey[:prefixLen])
	var key common.Hash
	copy(key[:], compositeKey[prefixLen:prefixLen+length.Hash])
	return addrHash, inc, key
}

// AddrHash + incarnation + KeyHash
// For contract storage (for plain state)
func PlainGenerateCompositeStorageKey(address []byte, incarnation uint64, key []byte) []byte {
	compositeKey := make([]byte, length.Addr+length.Incarnation+length.Hash)
	copy(compositeKey, address)
	binary.BigEndian.PutUint64(compositeKey[length.Addr:], incarnation)
	copy(compositeKey[length.Addr+length.Incarnation:], key)
	return compositeKey
}

func PlainParseCompositeStorageKey(compositeKey []byte) (common.Address, uint64, common.Hash) {
	prefixLen := length.Addr + length.Incarnation
	addr, inc := PlainParseStoragePrefix(compositeKey[:prefixLen])
	var key common.Hash
	copy(key[:], compositeKey[prefixLen:prefixLen+length.Hash])
	return addr, inc, key
}

// AddrHash + incarnation + StorageHashPrefix
func GenerateCompositeStoragePrefix(addressHash []byte, incarnation uint64, storageHashPrefix []byte) []byte {
	key := make([]byte, length.Hash+length.Incarnation+len(storageHashPrefix))
	copy(key, addressHash)
	binary.BigEndian.PutUint64(key[length.Hash:], incarnation)
	copy(key[length.Hash+length.Incarnation:], storageHashPrefix)
	return key
}

// address hash + incarnation prefix
func GenerateStoragePrefix(addressHash []byte, incarnation uint64) []byte {
	prefix := make([]byte, length.Hash+NumberLength)
	copy(prefix, addressHash)
	binary.BigEndian.PutUint64(prefix[length.Hash:], incarnation)
	return prefix
}

// address hash + incarnation prefix (for plain state)
func PlainGenerateStoragePrefix(address []byte, incarnation uint64) []byte {
	prefix := make([]byte, length.Addr+NumberLength)
	copy(prefix, address)
	binary.BigEndian.PutUint64(prefix[length.Addr:], incarnation)
	return prefix
}

func PlainParseStoragePrefix(prefix []byte) (common.Address, uint64) {
	var addr common.Address
	copy(addr[:], prefix[:length.Addr])
	inc := binary.BigEndian.Uint64(prefix[length.Addr : length.Addr+length.Incarnation])
	return addr, inc
}

func ParseStoragePrefix(prefix []byte) (common.Hash, uint64) {
	var addrHash common.Hash
	copy(addrHash[:], prefix[:length.Hash])
	inc := binary.BigEndian.Uint64(prefix[length.Hash : length.Hash+length.Incarnation])
	return addrHash, inc
}

// Key + blockNum
func CompositeKeySuffix(key []byte, timestamp uint64) (composite, encodedTS []byte) {
	encodedTS = encodeTimestamp(timestamp)
	composite = make([]byte, len(key)+len(encodedTS))
	copy(composite, key)
	copy(composite[len(key):], encodedTS)
	return composite, encodedTS
}

// encodeTimestamp has the property: if a < b, then Encoding(a) < Encoding(b) lexicographically
func encodeTimestamp(timestamp uint64) []byte {
	var suffix []byte
	var limit uint64 = 32

	for bytecount := 1; bytecount <= 8; bytecount++ {
		if timestamp < limit {
			suffix = make([]byte, bytecount)
			b := timestamp
			for i := bytecount - 1; i > 0; i-- {
				suffix[i] = byte(b & 0xff)
				b >>= 8
			}
			suffix[0] = byte(b) | (byte(bytecount) << 5) // 3 most significant bits of the first byte are bytecount
			break
		}
		limit <<= 8
	}
	return suffix
}

func decodeTimestamp(suffix []byte) (uint64, []byte) {
	bytecount := int(suffix[0] >> 5)
	timestamp := uint64(suffix[0] & 0x1f)
	for i := 1; i < bytecount; i++ {
		timestamp = (timestamp << 8) | uint64(suffix[i])
	}
	return timestamp, suffix[bytecount:]
}
