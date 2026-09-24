package state

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

type storageBaselineKey struct {
	addr accounts.Address
	key  accounts.StorageKey
}

// SetStorageBaseline overrides the committed value of one storage slot for the
// current transaction, so a replay can reproduce storage a canonical chain
// committed through a cache bug in the client that sealed it. The transaction
// reads the baseline and prices SSTORE against it; its own writes and the
// resulting write set are untouched. Cleared by SetTxContext.
func (sdb *IntraBlockState) SetStorageBaseline(addr accounts.Address, key accounts.StorageKey, value uint256.Int) {
	if sdb.storageBaselines == nil {
		sdb.storageBaselines = map[storageBaselineKey]uint256.Int{}
	}
	sdb.storageBaselines[storageBaselineKey{addr, key}] = value
}

func (sdb *IntraBlockState) storageBaseline(addr accounts.Address, key accounts.StorageKey) (uint256.Int, bool) {
	if len(sdb.storageBaselines) == 0 {
		return uint256.Int{}, false
	}
	value, ok := sdb.storageBaselines[storageBaselineKey{addr, key}]
	return value, ok
}

// wroteStorage reports whether the current transaction has written the slot, in
// which case a baseline must not shadow that write.
func (sdb *IntraBlockState) wroteStorage(addr accounts.Address, key accounts.StorageKey) bool {
	if sdb.versionMap != nil {
		if _, ok := sdb.versionedWrites.GetStorage(addr, key); ok {
			return true
		}
	}
	stateObject, ok := sdb.stateObjects[addr]
	if !ok {
		return false
	}
	_, dirty := stateObject.dirtyStorage[key]
	return dirty
}
