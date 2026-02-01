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
	"bytes"
	"sort"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
)

// StorageRangeResult is the result of a debug_storageRangeAt API call.
type StorageRangeResult struct {
	Storage storageMap   `json:"storage"`
	NextKey *common.Hash `json:"nextKey"` // nil if Storage includes the last key in the trie.
}

// storageMap a map from storage locations to StorageEntry items
type storageMap map[common.Hash]StorageEntry

// StorageEntry an entry in storage of the account
type StorageEntry struct {
	Key   *common.Hash `json:"key"`
	Value common.Hash  `json:"value"`
}

type tempEntry struct {
	SecKey common.Hash // Keccak256 of key
	Key    common.Hash // clear key
	Value  common.Hash
}

func storageRangeAt(ttx kv.TemporalTx, contractAddress common.Address, start []byte, txNum uint64, maxResult int) (StorageRangeResult, error) {
	result := StorageRangeResult{Storage: storageMap{}}

	// We use only the contract address to fetch ALL the storage
	fromKey := common.Copy(contractAddress.Bytes())
	toKey, _ := kv.NextSubtree(contractAddress.Bytes())

	r, err := ttx.RangeAsOf(kv.StorageDomain, fromKey, toKey, txNum, order.Asc, kv.Unlim) //no limit because need skip empty records
	if err != nil {
		return StorageRangeResult{}, err
	}
	defer r.Close()

	var entries []tempEntry

	for r.HasNext() {
		k, v, err := r.Next()
		if err != nil {
			return StorageRangeResult{}, err
		}
		if len(v) == 0 {
			continue // Skip deleted entries
		}

		key := common.BytesToHash(k[20:])
		secKey, err := common.HashData(k[20:])
		if err != nil {
			return StorageRangeResult{}, err
		}

		entries = append(entries, tempEntry{
			SecKey: secKey,
			Key:    key,
			Value:  common.BytesToHash(v),
		})
	}

	// sorted the list according secKey
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].SecKey.Bytes(), entries[j].SecKey.Bytes()) < 0
	})

	keyStartHash := common.BytesToHash(start)

	count := 0
	for _, e := range entries {

		// fetch SecKey >= keyStart requested by user
		if bytes.Compare(e.SecKey.Bytes(), keyStartHash.Bytes()) < 0 {
			continue
		}

		if count < maxResult {
			k := e.Key
			result.Storage[e.SecKey] = StorageEntry{
				Key:   &k,
				Value: e.Value,
			}
			count++
		} else {
			// get next Key
			next := e.SecKey
			result.NextKey = &next
			break
		}
	}
	return result, nil
}
