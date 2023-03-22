package commands

import (
	"fmt"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/order"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/state/temporal"
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

type walker interface {
	ForEachStorage(addr libcommon.Address, startLocation libcommon.Hash, cb func(key, seckey libcommon.Hash, value uint256.Int) bool, maxResults int) error
}

func storageRangeAt(stateReader walker, contractAddress libcommon.Address, start []byte, maxResult int) (StorageRangeResult, error) {
	result := StorageRangeResult{Storage: storageMap{}}
	resultCount := 0

	if err := stateReader.ForEachStorage(contractAddress, libcommon.BytesToHash(start), func(key, seckey libcommon.Hash, value uint256.Int) bool {
		if resultCount < maxResult {
			result.Storage[seckey] = StorageEntry{Key: &key, Value: value.Bytes32()}
		} else {
			result.NextKey = &key
		}
		resultCount++
		return resultCount < maxResult
	}, maxResult+1); err != nil {
		return StorageRangeResult{}, fmt.Errorf("error walking over storage: %w", err)
	}
	return result, nil
}

func storageRangeAtV3(ttx kv.TemporalTx, contractAddress libcommon.Address, start []byte, txNum uint64, maxResult int) (StorageRangeResult, error) {
	result := StorageRangeResult{Storage: storageMap{}}

	fromKey := append(libcommon.Copy(contractAddress.Bytes()), start...)
	toKey, _ := kv.NextSubtree(contractAddress.Bytes())

	r, err := ttx.DomainRange(temporal.StorageDomain, fromKey, toKey, txNum, order.Asc, maxResult+1)
	if err != nil {
		return StorageRangeResult{}, err
	}
	for i := 0; i < maxResult && r.HasNext(); i++ {
		k, v, err := r.Next()
		if err != nil {
			return StorageRangeResult{}, err
		}
		if len(v) == 0 {
			continue // Skip deleted entries
		}
		key := libcommon.BytesToHash(k[20:])
		seckey, err := common.HashData(k[20:])
		if err != nil {
			return StorageRangeResult{}, err
		}
		var value uint256.Int
		value.SetBytes(v)
		result.Storage[seckey] = StorageEntry{Key: &key, Value: value.Bytes32()}
	}

	if r.HasNext() {
		k, v, err := r.Next()
		if err != nil {
			return StorageRangeResult{}, err
		}
		if len(v) > 0 {
			key := libcommon.BytesToHash(k[20:])
			result.NextKey = &key
		}
	}
	return result, nil
}
