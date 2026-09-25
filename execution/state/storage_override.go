package state

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

type storageOverrideKey struct {
	addr accounts.Address
	key  accounts.StorageKey
}

// SetStorageOverride overrides the committed value of one storage slot for the
// current transaction, so a replay can reproduce storage a canonical chain
// committed through a cache bug in the client that sealed it. The transaction
// reads the override, prices SSTORE against it and skips writes equal to it;
// its own writes shadow it. Cleared at the transaction boundary: SetTxContext,
// FinalizeTx and Reset.
func (sdb *IntraBlockState) SetStorageOverride(addr accounts.Address, key accounts.StorageKey, value uint256.Int) {
	if sdb.storageOverrides == nil {
		sdb.storageOverrides = map[storageOverrideKey]uint256.Int{}
	}
	sdb.storageOverrides[storageOverrideKey{addr, key}] = value
}

func (sdb *IntraBlockState) storageOverride(addr accounts.Address, key accounts.StorageKey) (uint256.Int, bool) {
	if len(sdb.storageOverrides) == 0 {
		return uint256.Int{}, false
	}
	value, ok := sdb.storageOverrides[storageOverrideKey{addr, key}]
	return value, ok
}

// wroteStorage reports whether the current transaction has written the slot, in
// which case an override must not shadow that write.
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
