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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
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

// AddrHash + KeyHash. Compact storage-key format used by the legacy MPT
// path (eth_getProof / witness generation in execution/commitment/trie)
// and by the modern E3 storage domain (kv.StorageDomain). The
// per-account incarnation counter used to sit between the two hashes in
// the MPT format; it has been dropped now that IntraBlockState no
// longer tracks it.
func GenerateCompositeStorageKey(addressHash common.Hash, seckey common.Hash) []byte {
	compositeKey := make([]byte, length.Hash+length.Hash)
	copy(compositeKey, addressHash[:])
	copy(compositeKey[length.Hash:], seckey[:])
	return compositeKey
}

func ParseCompositeStorageKey(compositeKey []byte) (common.Hash, common.Hash) {
	var addrHash common.Hash
	copy(addrHash[:], compositeKey[:length.Hash])
	var key common.Hash
	copy(key[:], compositeKey[length.Hash:length.Hash+length.Hash])
	return addrHash, key
}
