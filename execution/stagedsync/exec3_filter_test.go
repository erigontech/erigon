package stagedsync

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestFilterWritesByVersionMap_RemovesUnmodifiedFields verifies that fields
// not in the versionMap WriteSet are filtered out. This prevents stale
// CollectorWrites values from overwriting correct sd.mem state.
func TestFilterWritesByVersionMap_RemovesUnmodifiedFields(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x01})

	// CollectorWrites has all 4 fields (LightCollector always emits all)
	collectorWrites := state.VersionedWrites{
		&state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr, Path: state.BalancePath}, Val: *uint256.NewInt(1000)},
		&state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: addr, Path: state.NoncePath}, Val: uint64(5)},
		&state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: addr, Path: state.IncarnationPath}, Val: uint64(0)},
		&state.VersionedWrite[accounts.CodeHash]{WriteHeader: state.WriteHeader{Address: addr, Path: state.CodeHashPath}, Val: accounts.EmptyCodeHash},
	}

	// versionMap WriteSet only has BalancePath and NoncePath
	// (the TX modified balance and nonce but not incarnation/codeHash)
	vmWrites := state.VersionedWrites{
		&state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr, Path: state.BalancePath}, Val: *uint256.NewInt(1000)},
		&state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: addr, Path: state.NoncePath}, Val: uint64(5)},
	}

	filtered := filterWritesByVersionMap(collectorWrites, vmWrites)

	assert.Len(t, filtered, 2, "Should keep only BalancePath and NoncePath")
	assert.Equal(t, state.BalancePath, filtered[0].Header().Path)
	assert.Equal(t, state.NoncePath, filtered[1].Header().Path)
}

// TestFilterWritesByVersionMap_KeepsStorageWrites verifies that storage
// entries present in the versionMap are kept.
func TestFilterWritesByVersionMap_KeepsStorageWrites(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x02})
	slot := accounts.InternKey([32]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04})

	collectorWrites := state.VersionedWrites{
		&state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr, Path: state.BalancePath}, Val: *uint256.NewInt(500)},
		&state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: addr, Path: state.NoncePath}, Val: uint64(1)},
		&state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr, Path: state.StoragePath, Key: slot}, Val: *uint256.NewInt(42)},
	}

	vmWrites := state.VersionedWrites{
		&state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr, Path: state.StoragePath, Key: slot}, Val: *uint256.NewInt(42)},
	}

	filtered := filterWritesByVersionMap(collectorWrites, vmWrites)

	assert.Len(t, filtered, 1, "Should keep only StoragePath")
	assert.Equal(t, state.StoragePath, filtered[0].Header().Path)
}

// TestFilterWritesByVersionMap_EmptyVMWrites returns all writes when
// versionMap WriteSet is empty (no filtering needed).
func TestFilterWritesByVersionMap_EmptyVMWrites(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x03})

	collectorWrites := state.VersionedWrites{
		&state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr, Path: state.BalancePath}, Val: *uint256.NewInt(100)},
		&state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: addr, Path: state.NoncePath}, Val: uint64(1)},
	}

	filtered := filterWritesByVersionMap(collectorWrites, nil)

	assert.Len(t, filtered, 2, "Empty vmWrites should return all")
}

// TestFilterWritesByVersionMap_MultipleAddresses verifies filtering works
// correctly when multiple addresses are involved.
func TestFilterWritesByVersionMap_MultipleAddresses(t *testing.T) {
	addr1 := accounts.InternAddress([20]byte{0x01})
	addr2 := accounts.InternAddress([20]byte{0x02})

	collectorWrites := state.VersionedWrites{
		// addr1: balance + nonce (TX modified balance only)
		&state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr1, Path: state.BalancePath}, Val: *uint256.NewInt(1000)},
		&state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: addr1, Path: state.NoncePath}, Val: uint64(5)},
		// addr2: balance + nonce (TX modified both)
		&state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr2, Path: state.BalancePath}, Val: *uint256.NewInt(2000)},
		&state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: addr2, Path: state.NoncePath}, Val: uint64(10)},
	}

	vmWrites := state.VersionedWrites{
		&state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr1, Path: state.BalancePath}, Val: *uint256.NewInt(1000)},
		&state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr2, Path: state.BalancePath}, Val: *uint256.NewInt(2000)},
		&state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: addr2, Path: state.NoncePath}, Val: uint64(10)},
	}

	filtered := filterWritesByVersionMap(collectorWrites, vmWrites)

	assert.Len(t, filtered, 3, "Should keep addr1.Balance + addr2.Balance + addr2.Nonce")

	// Verify the correct entries
	paths := make(map[[20]byte][]state.AccountPath)
	for _, w := range filtered {
		paths[w.Header().Address.Value()] = append(paths[w.Header().Address.Value()], w.Header().Path)
	}
	assert.Equal(t, []state.AccountPath{state.BalancePath}, paths[addr1.Value()])
	assert.Equal(t, []state.AccountPath{state.BalancePath, state.NoncePath}, paths[addr2.Value()])
}
