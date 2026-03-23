package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types/accounts"
)

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

// TestToTouchKeys_AccountSerialization verifies that ToTouchKeys produces
// correctly serialized account entries from individual field writes.
func TestToTouchKeys_AccountSerialization(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x42})
	balance := uint256.NewInt(1000)
	nonce := uint64(5)
	incarnation := uint64(1)
	codeHash := accounts.InternCodeHash([32]byte{0xaa, 0xbb})

	writes := VersionedWrites{
		{Address: addr, Path: BalancePath, Val: *balance},
		{Address: addr, Path: NoncePath, Val: nonce},
		{Address: addr, Path: IncarnationPath, Val: incarnation},
		{Address: addr, Path: CodeHashPath, Val: codeHash},
	}

	entries := writes.ToTouchKeys()

	// Should produce exactly 1 AccountsDomain entry
	var accountEntries []TouchKeyEntry
	for _, e := range entries {
		if e.Domain == kv.AccountsDomain {
			accountEntries = append(accountEntries, e)
		}
	}
	require.Equal(t, 1, len(accountEntries), "Should have 1 account entry")

	// Deserialize and verify
	var acc accounts.Account
	err := accounts.DeserialiseV3(&acc, accountEntries[0].Val)
	require.NoError(t, err)
	assert.Equal(t, *balance, acc.Balance)
	assert.Equal(t, nonce, acc.Nonce)
	assert.Equal(t, incarnation, acc.Incarnation)
}

// TestToTouchKeys_Storage verifies storage entries use correct composite keys.
func TestToTouchKeys_Storage(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x55})
	slot1 := accounts.InternKey([32]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04})
	slot2 := accounts.InternKey([32]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05})
	val1 := *uint256.NewInt(42)
	val2 := *uint256.NewInt(0) // zero = delete

	writes := VersionedWrites{
		{Address: addr, Path: StoragePath, Key: slot1, Val: val1},
		{Address: addr, Path: StoragePath, Key: slot2, Val: val2},
	}

	entries := writes.ToTouchKeys()

	var storageEntries []TouchKeyEntry
	for _, e := range entries {
		if e.Domain == kv.StorageDomain {
			storageEntries = append(storageEntries, e)
		}
	}
	require.Equal(t, 2, len(storageEntries), "Should have 2 storage entries")

	// First entry: non-zero value
	assert.Equal(t, 52, len(storageEntries[0].Key), "Composite key = 20 addr + 32 slot")

	// One should have val, one should be nil (delete)
	hasVal := false
	hasNil := false
	for _, e := range storageEntries {
		if e.Val == nil {
			hasNil = true
		} else {
			hasVal = true
		}
	}
	assert.True(t, hasVal, "Should have non-nil storage value")
	assert.True(t, hasNil, "Should have nil storage value (delete)")
}

// TestToTouchKeys_Code verifies code writes produce CodeDomain entries.
func TestToTouchKeys_Code(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xd2})
	code := []byte{0xef, 0x01, 0x00, 0x01, 0x02, 0x03}

	writes := VersionedWrites{
		{Address: addr, Path: CodePath, Val: code},
	}

	entries := writes.ToTouchKeys()

	var codeEntries []TouchKeyEntry
	for _, e := range entries {
		if e.Domain == kv.CodeDomain {
			codeEntries = append(codeEntries, e)
		}
	}
	require.Equal(t, 1, len(codeEntries))
	assert.Equal(t, code, codeEntries[0].Val)
}

// TestToTouchKeys_MixedWritesBatch verifies that a mixed batch of writes
// (accounts + storage + code) produces correct entries for all domains.
func TestToTouchKeys_MixedWritesBatch(t *testing.T) {
	addr1 := accounts.InternAddress([20]byte{0x01})
	addr2 := accounts.InternAddress([20]byte{0x02})
	slot := accounts.InternKey([32]byte{0x04})

	writes := VersionedWrites{
		// Account 1: balance + nonce
		{Address: addr1, Path: BalancePath, Val: *uint256.NewInt(100)},
		{Address: addr1, Path: NoncePath, Val: uint64(1)},
		{Address: addr1, Path: IncarnationPath, Val: uint64(0)},
		{Address: addr1, Path: CodeHashPath, Val: accounts.InternCodeHash([32]byte{})},
		// Account 2: storage write
		{Address: addr2, Path: StoragePath, Key: slot, Val: *uint256.NewInt(999)},
		// Account 1: code write
		{Address: addr1, Path: CodePath, Val: []byte{0x60, 0x00}},
	}

	entries := writes.ToTouchKeys()

	accountCount := 0
	storageCount := 0
	codeCount := 0
	for _, e := range entries {
		switch e.Domain {
		case kv.AccountsDomain:
			accountCount++
		case kv.StorageDomain:
			storageCount++
		case kv.CodeDomain:
			codeCount++
		}
	}

	assert.Equal(t, 1, accountCount, "1 account entry (addr1 fields grouped)")
	assert.Equal(t, 1, storageCount, "1 storage entry (addr2 slot)")
	assert.Equal(t, 1, codeCount, "1 code entry (addr1 code)")
}
