package state

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/types/accounts"
)

type storageOverrideKey struct {
	addr accounts.Address
	key  accounts.StorageKey
}

// StorageOverride replaces the committed (tx-start) value of one storage slot
// for one transaction. It changes what SSTORE prices against, unlike an eth_call
// state override, which replaces the current value.
type StorageOverride struct {
	Address accounts.Address
	Key     accounts.StorageKey
	Value   uint256.Int
}

// StorageOverrider supplies the overrides for the transaction at
// (blockNum, txIndex). rules.EngineReader implements it.
type StorageOverrider interface {
	StorageOverrides(blockNum uint64, txIndex int) []StorageOverride
}

// SetStorageOverrides attaches the overrider SetTxContext consults for every
// transaction; nil detaches it. It matches on block and tx index only, so attach
// it only to an IBS that executes canonical transactions: a user call run at the
// same position would pick the overrides up too. Survives Reset.
func (sdb *IntraBlockState) SetStorageOverrides(o StorageOverrider) {
	sdb.storageOverrider = o
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

func (sdb *IntraBlockState) installStorageOverrides() {
	sdb.storageOverrides = nil
	if sdb.storageOverrider == nil {
		return
	}
	for _, override := range sdb.storageOverrider.StorageOverrides(sdb.blockNum, sdb.txIndex) {
		sdb.SetStorageOverride(override.Address, override.Key, override.Value)
	}
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
