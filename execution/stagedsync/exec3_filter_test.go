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
	collectorWrites := &state.WriteSet{}
	collectorWrites.SetBalance(addr, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr, Path: state.BalancePath}, Val: *uint256.NewInt(1000)})
	collectorWrites.SetNonce(addr, &state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: addr, Path: state.NoncePath}, Val: uint64(5)})
	collectorWrites.SetIncarnation(addr, &state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: addr, Path: state.IncarnationPath}, Val: uint64(0)})
	collectorWrites.SetCodeHash(addr, &state.VersionedWrite[accounts.CodeHash]{WriteHeader: state.WriteHeader{Address: addr, Path: state.CodeHashPath}, Val: accounts.EmptyCodeHash})

	// versionMap WriteSet only has BalancePath and NoncePath
	// (the TX modified balance and nonce but not incarnation/codeHash)
	vmWrites := &state.WriteSet{}
	vmWrites.SetBalance(addr, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr, Path: state.BalancePath}, Val: *uint256.NewInt(1000)})
	vmWrites.SetNonce(addr, &state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: addr, Path: state.NoncePath}, Val: uint64(5)})

	filtered := filterWritesByVersionMap(collectorWrites, vmWrites)

	assert.Equal(t, 2, filtered.Count(), "Should keep only BalancePath and NoncePath")
	_, hasBal := filtered.GetBalance(addr)
	_, hasNonce := filtered.GetNonce(addr)
	assert.True(t, hasBal, "BalancePath must be kept")
	assert.True(t, hasNonce, "NoncePath must be kept")
}

// TestFilterWritesByVersionMap_KeepsStorageWrites verifies that storage
// entries present in the versionMap are kept.
func TestFilterWritesByVersionMap_KeepsStorageWrites(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x02})
	slot := accounts.InternKey([32]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04})

	collectorWrites := &state.WriteSet{}
	collectorWrites.SetBalance(addr, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr, Path: state.BalancePath}, Val: *uint256.NewInt(500)})
	collectorWrites.SetNonce(addr, &state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: addr, Path: state.NoncePath}, Val: uint64(1)})
	collectorWrites.SetStorage(addr, slot, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr, Path: state.StoragePath, Key: slot}, Val: *uint256.NewInt(42)})

	vmWrites := &state.WriteSet{}
	vmWrites.SetStorage(addr, slot, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr, Path: state.StoragePath, Key: slot}, Val: *uint256.NewInt(42)})

	filtered := filterWritesByVersionMap(collectorWrites, vmWrites)

	assert.Equal(t, 1, filtered.Count(), "Should keep only StoragePath")
	_, hasStorage := filtered.GetStorage(addr, slot)
	assert.True(t, hasStorage, "StoragePath must be kept")
}

// TestFilterWritesByVersionMap_EmptyVMWrites returns all writes when
// versionMap WriteSet is empty (no filtering needed).
func TestFilterWritesByVersionMap_EmptyVMWrites(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x03})

	collectorWrites := &state.WriteSet{}
	collectorWrites.SetBalance(addr, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr, Path: state.BalancePath}, Val: *uint256.NewInt(100)})
	collectorWrites.SetNonce(addr, &state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: addr, Path: state.NoncePath}, Val: uint64(1)})

	filtered := filterWritesByVersionMap(collectorWrites, nil)

	assert.Equal(t, 2, filtered.Count(), "Empty vmWrites should return all")
}

// TestFilterWritesByVersionMap_MultipleAddresses verifies filtering works
// correctly when multiple addresses are involved.
func TestFilterWritesByVersionMap_MultipleAddresses(t *testing.T) {
	addr1 := accounts.InternAddress([20]byte{0x01})
	addr2 := accounts.InternAddress([20]byte{0x02})

	collectorWrites := &state.WriteSet{}
	// addr1: balance + nonce (TX modified balance only)
	collectorWrites.SetBalance(addr1, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr1, Path: state.BalancePath}, Val: *uint256.NewInt(1000)})
	collectorWrites.SetNonce(addr1, &state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: addr1, Path: state.NoncePath}, Val: uint64(5)})
	// addr2: balance + nonce (TX modified both)
	collectorWrites.SetBalance(addr2, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr2, Path: state.BalancePath}, Val: *uint256.NewInt(2000)})
	collectorWrites.SetNonce(addr2, &state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: addr2, Path: state.NoncePath}, Val: uint64(10)})

	vmWrites := &state.WriteSet{}
	vmWrites.SetBalance(addr1, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr1, Path: state.BalancePath}, Val: *uint256.NewInt(1000)})
	vmWrites.SetBalance(addr2, &state.VersionedWrite[uint256.Int]{WriteHeader: state.WriteHeader{Address: addr2, Path: state.BalancePath}, Val: *uint256.NewInt(2000)})
	vmWrites.SetNonce(addr2, &state.VersionedWrite[uint64]{WriteHeader: state.WriteHeader{Address: addr2, Path: state.NoncePath}, Val: uint64(10)})

	filtered := filterWritesByVersionMap(collectorWrites, vmWrites)

	assert.Equal(t, 3, filtered.Count(), "Should keep addr1.Balance + addr2.Balance + addr2.Nonce")

	// addr1: balance kept, nonce dropped (only balance was in vmWrites).
	_, hasBal1 := filtered.GetBalance(addr1)
	_, hasNonce1 := filtered.GetNonce(addr1)
	assert.True(t, hasBal1, "addr1 balance must be kept")
	assert.False(t, hasNonce1, "addr1 nonce must be dropped")

	// addr2: both balance and nonce kept.
	_, hasBal2 := filtered.GetBalance(addr2)
	_, hasNonce2 := filtered.GetNonce(addr2)
	assert.True(t, hasBal2, "addr2 balance must be kept")
	assert.True(t, hasNonce2, "addr2 nonce must be kept")
}
