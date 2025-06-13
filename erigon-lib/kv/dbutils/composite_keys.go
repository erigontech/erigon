// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package dbutils

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
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

func ReceiptCacheKey(blockNumber uint64, blockHash common.Hash, txnIndex uint32) []byte {
	newK := make([]byte, 8+32+4)
	binary.BigEndian.PutUint64(newK, blockNumber)
	copy(newK[8:], blockHash[:])
	binary.BigEndian.PutUint32(newK[8+32:], txnIndex)
	return newK
}

// AddrHash + KeyHash
// Only for trie
func GenerateCompositeTrieKey(addressHash common.Hash, seckey common.Hash) []byte {
	compositeKey := make([]byte, 0, length.Hash+length.Hash)
	compositeKey = append(compositeKey, addressHash[:]...)
	compositeKey = append(compositeKey, seckey[:]...)
	return compositeKey
}

// Address + storageLocationHash
func GenerateStoragePlainKey(address common.Address, storageKey common.Hash) []byte {
	storagePlainKey := make([]byte, 0, length.Addr+length.Hash)
	storagePlainKey = append(storagePlainKey, address.Bytes()...)
	storagePlainKey = append(storagePlainKey, storageKey.Bytes()...)
	return storagePlainKey
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
