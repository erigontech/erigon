package commands

import (
	"fmt"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
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
		return resultCount <= maxResult
	}, maxResult+1); err != nil {
		return StorageRangeResult{}, fmt.Errorf("error walking over storage: %w", err)
	}
	return result, nil
}
