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
	"container/heap"
	"slices"

	"github.com/holiman/uint256"

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

// hashedStorageEntry is an intermediate type used for sorting storage entries by their hashed key.
type hashedStorageEntry struct {
	seckey common.Hash // keccak256 of the raw storage key
	key    common.Hash // raw storage key (preimage)
	value  common.Hash
}

// storageEntryMaxHeap is a max-heap of hashedStorageEntry ordered by seckey.
// Used to efficiently find the smallest maxResult+1 entries from a full storage scan.
type storageEntryMaxHeap []hashedStorageEntry

func (h storageEntryMaxHeap) Len() int { return len(h) }
func (h storageEntryMaxHeap) Less(i, j int) bool {
	return bytes.Compare(h[i].seckey[:], h[j].seckey[:]) > 0 // max-heap: largest first
}
func (h storageEntryMaxHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *storageEntryMaxHeap) Push(x any) {
	*h = append(*h, x.(hashedStorageEntry))
}

func (h *storageEntryMaxHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func storageRangeAt(ttx kv.TemporalTx, contractAddress common.Address, start []byte, txNum uint64, maxResult int, gethCompatibility bool) (StorageRangeResult, error) {
	if gethCompatibility {
		return storageRangeAtGethCompat(ttx, contractAddress, start, txNum, maxResult)
	}
	return storageRangeAtErigon(ttx, contractAddress, start, txNum, maxResult)
}

// storageRangeAtErigon iterates storage in raw key order (Erigon's flat KV order).
// This is the default behavior — fast, but produces different iteration order than Geth.
func storageRangeAtErigon(ttx kv.TemporalTx, contractAddress common.Address, start []byte, txNum uint64, maxResult int) (StorageRangeResult, error) {
	result := StorageRangeResult{Storage: storageMap{}}

	fromKey := append(common.Copy(contractAddress.Bytes()), start...)
	toKey, _ := kv.NextSubtree(contractAddress.Bytes())

	r, err := ttx.RangeAsOf(kv.StorageDomain, fromKey, toKey, txNum, order.Asc, kv.Unlim) //no limit because need skip empty records
	if err != nil {
		return StorageRangeResult{}, err
	}
	defer r.Close()
	for len(result.Storage) < maxResult && r.HasNext() {
		k, v, err := r.Next()
		if err != nil {
			return StorageRangeResult{}, err
		}
		if len(v) == 0 {
			continue // Skip deleted entries
		}
		key := common.BytesToHash(k[20:])
		seckey, err := common.HashData(k[20:])
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
		key := common.BytesToHash(k[20:])
		result.NextKey = &key
		break
	}
	return result, nil
}

// storageRangeAtGethCompat iterates storage in keccak256 hash order to match Geth's
// trie-based iteration. This requires scanning all storage entries and sorting by hash,
// which is significantly slower than the default Erigon approach.
func storageRangeAtGethCompat(ttx kv.TemporalTx, contractAddress common.Address, start []byte, txNum uint64, maxResult int) (StorageRangeResult, error) {
	result := StorageRangeResult{Storage: storageMap{}}

	// Always scan all storage for this contract — we need to sort by hashed key
	// to match Geth's trie-based iteration order.
	fromKey := common.Copy(contractAddress.Bytes())
	toKey, _ := kv.NextSubtree(fromKey)

	r, err := ttx.RangeAsOf(kv.StorageDomain, fromKey, toKey, txNum, order.Asc, kv.Unlim)
	if err != nil {
		return StorageRangeResult{}, err
	}
	defer r.Close()

	// We need the smallest maxResult+1 entries (by hashed key) that are >= start.
	// Use a bounded max-heap of size maxResult+1: we keep pushing eligible entries
	// and evict the largest whenever the heap exceeds the limit. This gives O(n log k)
	// time and O(k) memory, where k = maxResult+1 and n = total storage entries.
	startHash := common.BytesToHash(start)
	hasStart := len(start) > 0
	limit := maxResult + 1 // +1 to determine nextKey
	h := make(storageEntryMaxHeap, 0, limit+1)

	for r.HasNext() {
		k, v, err := r.Next()
		if err != nil {
			return StorageRangeResult{}, err
		}
		if len(v) == 0 {
			continue
		}
		rawKey := k[20:]
		seckey, err := common.HashData(rawKey)
		if err != nil {
			return StorageRangeResult{}, err
		}

		// Skip entries with hashed key < start.
		if hasStart && bytes.Compare(seckey[:], startHash[:]) < 0 {
			continue
		}

		// If the heap is full and this entry is larger than the current max, skip it.
		if h.Len() == limit && bytes.Compare(seckey[:], h[0].seckey[:]) >= 0 {
			continue
		}

		var value uint256.Int
		value.SetBytes(v)
		heap.Push(&h, hashedStorageEntry{
			seckey: seckey,
			key:    common.BytesToHash(rawKey),
			value:  value.Bytes32(),
		})
		if h.Len() > limit {
			heap.Pop(&h)
		}
	}

	// Sort the collected entries by hashed key (ascending).
	entries := []hashedStorageEntry(h)
	slices.SortFunc(entries, func(a, b hashedStorageEntry) int {
		return bytes.Compare(a.seckey[:], b.seckey[:])
	})

	// Fill result with up to maxResult entries; the extra one (if present) becomes nextKey.
	for i := range entries {
		if i >= maxResult {
			result.NextKey = &entries[i].seckey
			break
		}
		key := entries[i].key
		result.Storage[entries[i].seckey] = StorageEntry{Key: &key, Value: entries[i].value}
	}

	return result, nil
}
