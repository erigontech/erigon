package commands

import (
	"fmt"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
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

type walker interface {
	ForEachStorage(addr common.Address, startLocation common.Hash, cb func(key, seckey common.Hash, value uint256.Int) bool, maxResults int) error
}

func storageRangeAt(stateReader walker, contractAddress common.Address, start []byte, maxResult int) (StorageRangeResult, error) {
	result := StorageRangeResult{Storage: storageMap{}}
	resultCount := 0

	if err := stateReader.ForEachStorage(contractAddress, common.BytesToHash(start), func(key, seckey common.Hash, value uint256.Int) bool {
		if resultCount < maxResult {
			result.Storage[seckey] = StorageEntry{Key: &key, Value: value.Bytes32()}
		} else {
			result.NextKey = &key
		}
		resultCount++
		return resultCount <= maxResult
	}, maxResult+1); err != nil {
		return StorageRangeResult{}, fmt.Errorf("error walking over storage: %w", err)
	}
	return result, nil
}
