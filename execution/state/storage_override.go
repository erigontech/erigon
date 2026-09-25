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

// StorageOverridePosition names one canonical transaction.
type StorageOverridePosition struct {
	BlockNum uint64
	TxIndex  int
}

// StorageOverrideTable holds the overrides of every patched transaction of a
// chain. IBSs share it read-only.
type StorageOverrideTable map[StorageOverridePosition][]StorageOverride

// StorageOverrider supplies a chain's override table. rules.EngineReader
// implements it.
type StorageOverrider interface {
	StorageOverrides() StorageOverrideTable
}

// Option configures an IntraBlockState at construction.
type Option func(*IntraBlockState)

// WithStorageOverrides attaches o's table at construction; see
// SetStorageOverrides.
func WithStorageOverrides(o StorageOverrider) Option {
	return func(sdb *IntraBlockState) { sdb.storageOverrideTable = storageOverrideTable(o) }
}

// SetStorageOverrides attaches o's table, which SetTxContext consults for every
// transaction; nil detaches it. The table is keyed on block and tx index only, so
// attach it only to an IBS that executes canonical transactions: a user call run
// at the same position would pick the overrides up too. Changing it drops the
// overrides already installed for the current transaction. Survives Reset.
func (sdb *IntraBlockState) SetStorageOverrides(o StorageOverrider) {
	sdb.storageOverrideTable = storageOverrideTable(o)
	sdb.storageOverrides = nil
}

func storageOverrideTable(o StorageOverrider) StorageOverrideTable {
	if o == nil {
		return nil
	}
	return o.StorageOverrides()
}

// SetStorageOverride overrides the committed value of one storage slot for the
// current transaction, so a replay can reproduce storage a canonical chain
// committed through a cache bug in the client that sealed it. The transaction
// reads the override, prices SSTORE against it and skips writes equal to it;
// its own writes shadow it. Replaced at the next SetTxContext, cleared by Reset.
func (sdb *IntraBlockState) SetStorageOverride(addr accounts.Address, key accounts.StorageKey, value uint256.Int) {
	if sdb.storageOverrides == nil {
		sdb.storageOverrides = map[storageOverrideKey]uint256.Int{}
	}
	sdb.storageOverrides[storageOverrideKey{addr, key}] = value
}

func (sdb *IntraBlockState) installStorageOverrides() {
	sdb.storageOverrides = nil
	for _, override := range sdb.storageOverrideTable[StorageOverridePosition{sdb.blockNum, sdb.txIndex}] {
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
