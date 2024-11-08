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

package jsonrpc

import (
	"github.com/holiman/uint256"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/order"
)

// StorageRangeResult is the result of a debug_storageRangeAt API call.
type StorageRangeResult struct {
	Storage storageMap      `json:"storage"`
	NextKey *libcommon.Hash `json:"nextKey"` // nil if Storage includes the last key in the trie.
}

// storageMap a map from storage locations to StorageEntry items
type storageMap map[libcommon.Hash]StorageEntry

// StorageEntry an entry in storage of the account
type StorageEntry struct {
	Key   *libcommon.Hash `json:"key"`
	Value libcommon.Hash  `json:"value"`
}

func storageRangeAt(ttx kv.TemporalTx, contractAddress libcommon.Address, start []byte, txNum uint64, maxResult int) (StorageRangeResult, error) {
	result := StorageRangeResult{Storage: storageMap{}}

	fromKey := append(libcommon.Copy(contractAddress.Bytes()), start...)
	toKey, _ := kv.NextSubtree(contractAddress.Bytes())

	r, err := ttx.DomainRange(kv.StorageDomain, fromKey, toKey, txNum, order.Asc, kv.Unlim) //no limit because need skip empty records
	if err != nil {
		return StorageRangeResult{}, err
	}
	defer r.Close()
	for i := 0; i < maxResult && r.HasNext(); i++ {
		k, v, err := r.Next()
		if err != nil {
			return StorageRangeResult{}, err
		}
		if len(v) == 0 {
			continue // Skip deleted entries
		}
		key := libcommon.BytesToHash(k[20:])
		seckey, err := libcommon.HashData(k[20:])
		if err != nil {
			return StorageRangeResult{}, err
		}
		var value uint256.Int
		value.SetBytes(v)
		result.Storage[seckey] = StorageEntry{Key: &key, Value: value.Bytes32()}
	}

	for r.HasNext() { // not `if` because need skip empty vals
		k, v, err := r.Next()
		if err != nil {
			return StorageRangeResult{}, err
		}
		if len(v) == 0 {
			continue
		}
		key := libcommon.BytesToHash(k[20:])
		result.NextKey = &key
		break
	}
	return result, nil
}
