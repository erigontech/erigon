package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// emptyReader is a stub StateReader that returns empty/nil for all reads.
type emptyReader struct{}

func (r *emptyReader) ReadAccountData(accounts.Address) (*accounts.Account, error) { return nil, nil }
func (r *emptyReader) ReadAccountDataForDebug(accounts.Address) (*accounts.Account, error) {
	return nil, nil
}
func (r *emptyReader) ReadAccountStorage(accounts.Address, accounts.StorageKey) (uint256.Int, bool, error) {
	return uint256.Int{}, false, nil
}
func (r *emptyReader) HasStorage(accounts.Address) (bool, error)               { return false, nil }
func (r *emptyReader) ReadAccountCode(accounts.Address) ([]byte, error)        { return nil, nil }
func (r *emptyReader) ReadAccountCodeSize(accounts.Address) (int, error)       { return 0, nil }
func (r *emptyReader) ReadAccountIncarnation(accounts.Address) (uint64, error) { return 0, nil }
func (r *emptyReader) SetTrace(bool, string)                                   {}
func (r *emptyReader) Trace() bool                                             { return false }
func (r *emptyReader) TracePrefix() string                                     { return "" }

// TestValueTiebreaker_BalancePath verifies that validation does not
// invalidate a StorageRead when the versionMap Done value matches the
// read value. This prevents unnecessary re-executions that cause
// cascading state errors.
func TestValueTiebreaker_BalancePath(t *testing.T) {
	vm := NewVersionMap(nil)

	addr := accounts.InternAddress([20]byte{0x01})
	balance := uint256.NewInt(1000)

	// Write a balance to the versionMap at txIndex=5
	vm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 5, Incarnation: 0}, *balance, true)

	// Validate a read from txIndex=10 that read the SAME value from storage
	readVal := *balance // Same value

	valid := vm.validateRead(10, addr, BalancePath, accounts.NilKey, StorageRead, Version{TxIndex: UnknownDep},
		readVal, // value tiebreaker
		func(rv, wv Version) VersionValidity { return VersionValid },
		false, "")

	assert.Equal(t, VersionValid, valid, "Should be valid when StorageRead value matches versionMap Done value")
}

// TestValueTiebreaker_DifferentBalance verifies that validation DOES
// invalidate when the StorageRead value differs from the versionMap value.
func TestValueTiebreaker_DifferentBalance(t *testing.T) {
	vm := NewVersionMap(nil)

	addr := accounts.InternAddress([20]byte{0x01})

	// Write balance=1000 to versionMap
	vm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 5, Incarnation: 0}, *uint256.NewInt(1000), true)

	// Validate a read that got balance=500 from storage (stale)
	readVal := *uint256.NewInt(500)

	valid := vm.validateRead(10, addr, BalancePath, accounts.NilKey, StorageRead, Version{TxIndex: UnknownDep},
		readVal,
		func(rv, wv Version) VersionValidity { return VersionValid },
		false, "")

	assert.Equal(t, VersionInvalid, valid, "Should be invalid when StorageRead value differs from versionMap Done value")
}

// TestValueTiebreaker_NoncePath verifies nonce comparison works.
func TestValueTiebreaker_NoncePath(t *testing.T) {
	vm := NewVersionMap(nil)

	addr := accounts.InternAddress([20]byte{0x02})

	// Write nonce=42 to versionMap
	vm.Write(addr, NoncePath, accounts.NilKey, Version{TxIndex: 5, Incarnation: 0}, uint64(42), true)

	// Same nonce from storage → valid
	valid := vm.validateRead(10, addr, NoncePath, accounts.NilKey, StorageRead, Version{TxIndex: UnknownDep},
		uint64(42),
		func(rv, wv Version) VersionValidity { return VersionValid },
		false, "")
	assert.Equal(t, VersionValid, valid, "Same nonce should be valid")

	// Different nonce → invalid
	valid = vm.validateRead(10, addr, NoncePath, accounts.NilKey, StorageRead, Version{TxIndex: UnknownDep},
		uint64(41),
		func(rv, wv Version) VersionValidity { return VersionValid },
		false, "")
	assert.Equal(t, VersionInvalid, valid, "Different nonce should be invalid")
}

// TestValuesEqual verifies the valuesEqual helper for all path types.
func TestValuesEqual(t *testing.T) {
	// BalancePath
	b1 := uint256.NewInt(100)
	b2 := uint256.NewInt(100)
	b3 := uint256.NewInt(200)
	assert.True(t, valuesEqual(BalancePath, *b1, *b2), "Same balance should be equal")
	assert.False(t, valuesEqual(BalancePath, *b1, *b3), "Different balance should not be equal")

	// NoncePath
	assert.True(t, valuesEqual(NoncePath, uint64(5), uint64(5)), "Same nonce")
	assert.False(t, valuesEqual(NoncePath, uint64(5), uint64(6)), "Different nonce")

	// IncarnationPath
	assert.True(t, valuesEqual(IncarnationPath, uint64(1), uint64(1)), "Same incarnation")
	assert.False(t, valuesEqual(IncarnationPath, uint64(1), uint64(2)), "Different incarnation")

	// Nil values
	assert.True(t, valuesEqual(BalancePath, nil, nil), "Both nil should be equal")
	assert.False(t, valuesEqual(BalancePath, *b1, nil), "One nil should not be equal")
	assert.False(t, valuesEqual(BalancePath, nil, *b1), "One nil should not be equal")
}

// TestVersionedWriteVersion verifies that VersionedWrite entries at
// txIndex=0 are still reachable. The bug was that finalizeTx appended
// writes without Version (zero value = txIndex=0), making them only
// visible via floor(0) but invisible to floor(N-1) for N > 1.
func TestVersionedWriteVersion(t *testing.T) {
	vm := NewVersionMap(nil)

	addr := accounts.InternAddress([20]byte{0x03})

	// Write at txIndex=10 with correct Version
	vm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 10, Incarnation: 1}, *uint256.NewInt(500), true)

	// Read at txIndex=11 should find txIndex=10
	rr := vm.Read(addr, BalancePath, accounts.NilKey, 11)
	assert.Equal(t, MVReadResultDone, rr.Status(), "Should find entry at floor(10)")
	assert.Equal(t, 10, rr.DepIdx(), "Should be from txIndex 10")

	// Now also write at txIndex=0 (simulates the zero-Version bug)
	vm.Write(addr, BalancePath, accounts.NilKey, Version{TxIndex: 0, Incarnation: 0}, *uint256.NewInt(999), true)

	// Read at txIndex=11 should STILL find txIndex=10 (not 0)
	rr = vm.Read(addr, BalancePath, accounts.NilKey, 11)
	assert.Equal(t, MVReadResultDone, rr.Status())
	assert.Equal(t, 10, rr.DepIdx(), "Should find txIndex=10, not txIndex=0")

	// But read at txIndex=1 should find txIndex=0
	rr = vm.Read(addr, BalancePath, accounts.NilKey, 1)
	assert.Equal(t, MVReadResultDone, rr.Status())
	assert.Equal(t, 0, rr.DepIdx(), "Should find txIndex=0 for floor(0)")
}

// TestAccessListResetInIBSReset verifies that IBS.Reset() clears the
// access list, preventing stale warm addresses from leaking between
// TX executions on the same worker.
func TestAccessListResetInIBSReset(t *testing.T) {
	ibs := New(nil)

	// Add an address to the access list
	testAddr := accounts.InternAddress([20]byte{0x42})
	ibs.AddAddressToAccessList(testAddr)
	assert.True(t, ibs.AddressInAccessList(testAddr), "Address should be warm")

	// Reset
	ibs.Reset()

	// Address should be cold after reset
	assert.False(t, ibs.AddressInAccessList(testAddr), "Address should be cold after Reset")
}

// TestTransientStorageResetInIBSReset verifies that IBS.Reset() clears
// transient storage (EIP-1153).
func TestTransientStorageResetInIBSReset(t *testing.T) {
	ibs := New(nil)

	testAddr := accounts.InternAddress([20]byte{0x42})
	testKey := accounts.InternKey([32]byte{0x01})

	// Set transient storage
	ibs.SetTransientState(testAddr, testKey, *uint256.NewInt(42))
	val := ibs.GetTransientState(testAddr, testKey)
	assert.False(t, val.IsZero(), "Transient storage should be set")

	// Reset
	ibs.Reset()

	// Transient storage should be cleared
	val = ibs.GetTransientState(testAddr, testKey)
	assert.True(t, val.IsZero(), "Transient storage should be zero after Reset")
}

// TestCodeReadFromVersionMap verifies that the versionMap CodePath
// entries are accessible. This ensures EIP-7702 synthetic code
// (delegation prefix) written by a prior TX is visible to subsequent
// TXs via the versionMap.
func TestCodeReadFromVersionMap(t *testing.T) {
	vm := NewVersionMap(nil)

	addr := accounts.InternAddress([20]byte{0x55})

	// Write EIP-7702 delegation code to versionMap at txIndex=5
	delegationCode := []byte{0xef, 0x01, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05,
		0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
		0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14}
	vm.Write(addr, CodePath, accounts.NilKey, Version{TxIndex: 5, Incarnation: 0}, delegationCode, true)

	// Read at txIndex=10 should find the code
	rr := vm.Read(addr, CodePath, accounts.NilKey, 10)
	require.Equal(t, MVReadResultDone, rr.Status(), "Should find CodePath entry")

	code, ok := rr.Value().([]byte)
	require.True(t, ok, "Value should be []byte")
	assert.Equal(t, delegationCode, code, "Code should match")
	assert.Equal(t, byte(0xef), code[0], "Should have EIP-7702 prefix")

	// Read at txIndex=3 should NOT find it (before the write)
	rr = vm.Read(addr, CodePath, accounts.NilKey, 3)
	assert.NotEqual(t, MVReadResultDone, rr.Status(), "Should not find code before write txIndex")
}

// TestTouchUpdates_Account verifies that TouchUpdates feeds account field
// writes directly to commitment.Updates via TouchPlainKeyDirect, producing
// merged Updates with correct key count.
func TestTouchUpdates_Account(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x42})

	writes := VersionedWrites{
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath}, Val: *uint256.NewInt(1000)},
		&VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath}, Val: uint64(5)},
		&VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: IncarnationPath}, Val: uint64(1)},
		&VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath}, Val: accounts.InternCodeHash([32]byte{0xaa, 0xbb})},
	}

	updates := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), func(k []byte) []byte { return k })
	writes.TouchUpdates(updates)

	// All 4 fields merge into 1 key (same address)
	assert.Equal(t, uint64(1), updates.Size(), "Should have 1 merged key for same address")
}

// TestToTouchKeys_Storage verifies storage entries use correct composite keys.
func TestToTouchKeys_Storage(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x55})
	slot1 := accounts.InternKey([32]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04})
	slot2 := accounts.InternKey([32]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05})
	val1 := *uint256.NewInt(42)
	val2 := *uint256.NewInt(0) // zero = delete

	writes := VersionedWrites{
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: slot1}, Val: val1},
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: slot2}, Val: val2},
	}

	updates := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), func(k []byte) []byte { return k })
	writes.TouchUpdates(updates)

	// Should have 2 unique keys (different slots)
	assert.Equal(t, uint64(2), updates.Size(), "Should have 2 storage keys")
}

// TestTouchUpdates_Code verifies code writes feed through TouchUpdates.
func TestTouchUpdates_Code(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xd2})
	code := []byte{0xef, 0x01, 0x00, 0x01, 0x02, 0x03}

	writes := VersionedWrites{
		&VersionedWrite[accounts.Code]{WriteHeader: WriteHeader{Address: addr, Path: CodePath}, Val: accounts.Code{Hash: accounts.InternCodeHash(crypto.Keccak256Hash(code)), Bytes: code}},
	}

	updates := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), func(k []byte) []byte { return k })
	writes.TouchUpdates(updates)

	assert.Equal(t, uint64(1), updates.Size(), "Should have 1 code key")
}

// TestTouchUpdates_MixedBatch verifies that a mixed batch of writes
// (accounts + storage + code) feeds correctly through TouchUpdates.
func TestTouchUpdates_MixedBatch(t *testing.T) {
	addr1 := accounts.InternAddress([20]byte{0x01})
	addr2 := accounts.InternAddress([20]byte{0x02})
	slot := accounts.InternKey([32]byte{0x04})

	writes := VersionedWrites{
		// Account 1: balance + nonce + code
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr1, Path: BalancePath}, Val: *uint256.NewInt(100)},
		&VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr1, Path: NoncePath}, Val: uint64(1)},
		&VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr1, Path: IncarnationPath}, Val: uint64(0)},
		&VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr1, Path: CodeHashPath}, Val: accounts.InternCodeHash([32]byte{})},
		&VersionedWrite[accounts.Code]{WriteHeader: WriteHeader{Address: addr1, Path: CodePath}, Val: accounts.Code{Hash: accounts.InternCodeHash(crypto.Keccak256Hash([]byte{0x60, 0x00})), Bytes: []byte{0x60, 0x00}}},
		// Account 2: storage write
		&VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr2, Path: StoragePath, Key: slot}, Val: *uint256.NewInt(999)},
	}

	updates := commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), func(k []byte) []byte { return k })
	writes.TouchUpdates(updates)

	// addr1: account fields + code all merge into 1 key
	// addr2+slot: 1 storage key
	// Total: 2 unique keys
	assert.Equal(t, uint64(2), updates.Size(), "2 unique keys (addr1 merged, addr2 storage)")
}

// TestBlockStateCacheWriteAccount_NilCommitted verifies that WriteAccount
// doesn't panic when the committed cache has a nil account entry.
// This can happen when PutCommittedAccount stores nil (account doesn't exist).
func TestBlockStateCacheWriteAccount_NilCommitted(t *testing.T) {
	cache := NewBlockStateCache()

	addr := accounts.InternAddress([20]byte{0x42})

	// Put a nil committed account (account doesn't exist in pre-block state)
	cache.PutCommittedAccount(addr, nil)

	// Write a new account — should not panic
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(1000)
	acc.Nonce = 1
	enc := accounts.SerialiseV3(&acc)

	assert.NotPanics(t, func() {
		cache.WriteAccount(addr, enc, 1)
	}, "WriteAccount should not panic with nil committed account")

	// Verify the write is recorded.
	current, ok := cache.GetCurrentAccount(addr)
	assert.True(t, ok, "Should have current account")
	assert.Equal(t, enc, current, "Current account should match written value")
}

// TestBlockStateCacheWriteAccountUpdatesCurrent verifies that successive
// writes update the current view to the latest value (last write wins
// for read access via GetCurrentAccount). The full per-tx history is
// preserved in writeLog for Flush.
func TestBlockStateCacheWriteAccountUpdatesCurrent(t *testing.T) {
	cache := NewBlockStateCache()

	addr := accounts.InternAddress([20]byte{0x55})

	// Set up committed account
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(500)
	acc.Nonce = 3
	cache.PutCommittedAccount(addr, &acc)

	enc := accounts.SerialiseV3(&acc)
	cache.WriteAccount(addr, enc, 3)

	acc2 := accounts.NewAccount()
	acc2.Balance = *uint256.NewInt(600)
	acc2.Nonce = 3
	enc2 := accounts.SerialiseV3(&acc2)
	cache.WriteAccount(addr, enc2, 5)

	current, ok := cache.GetCurrentAccount(addr)
	assert.True(t, ok)
	assert.Equal(t, enc2, current, "GetCurrentAccount should return the latest write")
}

// TestSelfDestructKeepsDirtyStorageReadableSameTx verifies that after an
// account self-destructs (versionMap active), a subsequent same-tx GetState
// still returns the dirty value written before the SELFDESTRUCT. Pre-Cancun
// (and for CALL-based SELFDESTRUCT generally) the account stays alive until
// end-of-tx, so re-entered code must see the real storage — not zero.
//
// Regression: IBS.Selfdestruct used to versionWritten(StoragePath, key, 0)
// for every dirty slot to feed the parallel commitment calculator. Because
// versionedRead consults versionedWrites before the stateObject, those
// spurious zero writes made same-tx re-reads return 0 — wrong gas
// (SSTORE_SET vs dirty-update, +19900) and a wrong written value
// (EEST cancun/eip6780_selfdestruct/* under EXEC3_PARALLEL). The calc now
// gets per-slot DELETEs from normalizeWriteSet's SD cascade instead.
func TestSelfDestructKeepsDirtyStorageReadableSameTx(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xAA})
	slot0 := accounts.InternKey([32]byte{0x00})
	slot1 := accounts.InternKey([32]byte{0x01})

	vm := NewVersionMap(nil)

	ibs := New(&emptyReader{})
	ibs.SetVersionMap(vm)
	ibs.SetTxContext(100, 0)
	ibs.SetVersion(0)

	ibs.CreateAccount(addr, true)
	require.NoError(t, ibs.SetState(addr, slot0, *uint256.NewInt(42)))
	require.NoError(t, ibs.SetState(addr, slot1, *uint256.NewInt(99)))

	_, err := ibs.Selfdestruct(addr)
	require.NoError(t, err)

	// After SELFDESTRUCT, same-tx reads must still see the dirty values.
	got0, err := ibs.GetState(addr, slot0)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), got0.Uint64(), "slot0 must read back as 42, not 0, after SELFDESTRUCT")
	got1, err := ibs.GetState(addr, slot1)
	require.NoError(t, err)
	assert.Equal(t, uint64(99), got1.Uint64(), "slot1 must read back as 99, not 0, after SELFDESTRUCT")

	// SelfDestructPath is still recorded.
	destructed, err := ibs.HasSelfdestructed(addr)
	require.NoError(t, err)
	assert.True(t, destructed)

	// And it must NOT have emitted spurious StoragePath=0 writes.
	for _, w := range ibs.VersionedWrites(false) {
		if w.Header().Address == addr && w.Header().Path == StoragePath {
			v := w.ValAny().(uint256.Int)
			assert.False(t, v.IsZero(), "Selfdestruct must not emit StoragePath=0 for slot %x", w.Header().Key.Value())
		}
	}
}
