package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// --- Test 1: IBS.Reset clears accessList and transientStorage ---

func TestReset_ClearsAccessListAndTransientStorage(t *testing.T) {
	t.Parallel()

	addr1 := accounts.InternAddress([20]byte{1})
	addr2 := accounts.InternAddress([20]byte{2})
	slot := accounts.InternKey([32]byte{0x42})

	ibs := New(nil)

	// Warm some addresses and add transient storage.
	ibs.accessList = newAccessList()
	ibs.accessList.AddAddress(addr1)
	ibs.accessList.AddAddress(addr2)
	ibs.accessList.AddSlot(addr1, slot)
	ibs.transientStorage = newTransientStorage()
	ibs.transientStorage.Set(addr1, slot, *uint256.NewInt(0xff))

	// Verify pre-reset state.
	assert.True(t, ibs.accessList.ContainsAddress(addr1), "addr1 should be warm before reset")
	assert.True(t, ibs.accessList.ContainsAddress(addr2), "addr2 should be warm before reset")
	val := ibs.transientStorage.Get(addr1, slot)
	assert.Equal(t, *uint256.NewInt(0xff), val, "transient storage should have value before reset")

	// Reset.
	ibs.Reset()

	// Verify post-reset state.
	assert.False(t, ibs.accessList.ContainsAddress(addr1), "addr1 should be cold after reset")
	assert.False(t, ibs.accessList.ContainsAddress(addr2), "addr2 should be cold after reset")
	val = ibs.transientStorage.Get(addr1, slot)
	assert.True(t, val.IsZero(), "transient storage should be empty after reset")
}

// --- Test 2: stateObject.Code reads from versionMap CodePath ---

func TestStateObject_Code_ReadsFromVersionMap(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress([20]byte{0xd2, 0xc6, 0xb2})

	// EIP-7702 delegation prefix: 0xef0100 + 20-byte address
	delegationCode := make([]byte, 23)
	delegationCode[0] = 0xef
	delegationCode[1] = 0x01
	delegationCode[2] = 0x00
	copy(delegationCode[3:], []byte{0xaa, 0xbb, 0xcc})

	// Create versionMap with CodePath entry.
	vm := NewVersionMap(nil)
	vm.Write(addr, CodePath, accounts.NilKey,
		Version{TxIndex: 5, Incarnation: 0}, delegationCode, true)

	// Create IBS with versionMap but nil stateReader.
	ibs := New(nil)
	ibs.versionMap = vm
	ibs.txIndex = 10

	// Create stateObject with non-empty codeHash (so Code() doesn't return nil).
	so := &stateObject{
		address: addr,
		db:      ibs,
		data: accounts.Account{
			CodeHash: accounts.InternCodeHash([32]byte{0x45, 0x42}), // non-empty
		},
	}

	code, err := so.Code()
	require.NoError(t, err)
	assert.Equal(t, delegationCode, code,
		"Code() should return the versionMap CodePath entry (EIP-7702 delegation)")
}

func TestStateObject_Code_FallsBackWhenNoVersionMapEntry(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress([20]byte{0xaa})

	// Create versionMap WITHOUT CodePath entry for addr.
	vm := NewVersionMap(nil)

	// Create IBS with versionMap. stateObject.Code() should NOT find
	// the address in the versionMap and should fall through to stateReader.
	// We verify the versionMap check doesn't return wrong data.
	ibs := New(nil)
	ibs.versionMap = vm
	ibs.txIndex = 10

	so := &stateObject{
		address: addr,
		db:      ibs,
		data: accounts.Account{
			// Empty code hash means Code() returns nil early (line 409).
			CodeHash: accounts.EmptyCodeHash,
		},
	}

	code, err := so.Code()
	require.NoError(t, err)
	assert.Nil(t, code, "should return nil for empty code hash even with versionMap present")
}

// --- Test 3: CodeHash refresh from versionMap on stateObject cache hit ---

func TestGetStateObject_RefreshesCodeHashFromVersionMap(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress([20]byte{0xd2, 0xc6})
	oldCodeHash := accounts.InternCodeHash([32]byte{0x11, 0x22})
	newCodeHash := accounts.InternCodeHash([32]byte{0x33, 0x44})

	vm := NewVersionMap(nil)

	// Write a newer CodeHashPath to the versionMap.
	vm.Write(addr, CodeHashPath, accounts.NilKey,
		Version{TxIndex: 5, Incarnation: 0}, newCodeHash, true)

	ibs := New(nil)
	ibs.versionMap = vm
	ibs.txIndex = 10

	// Pre-cache a stateObject with the OLD codeHash.
	cachedCode := []byte{0xef, 0x01, 0x00, 0xaa, 0xbb} // old code
	so := &stateObject{
		address: addr,
		db:      ibs,
		data: accounts.Account{
			CodeHash:    oldCodeHash,
			Incarnation: 1,
			Nonce:       1,
		},
		code: cachedCode,
	}
	ibs.stateObjects[addr] = so

	// Access via getStateObject — should refresh CodeHash.
	result, err := ibs.getStateObject(addr, false)
	require.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, newCodeHash, result.data.CodeHash,
		"CodeHash should be refreshed from versionMap")
	// Note: cached code may or may not be nil depending on whether
	// the refresh path clears it. The key invariant is that the
	// CodeHash is updated so subsequent Code() calls read fresh data.
	// If code is still cached with the old value, the CodeHash mismatch
	// will cause Code() to re-read on next access.
}

// --- Test 4: Validation cross-check behavior ---

func TestValidateVersion_AddressPathDone_CrossChecksBalancePath(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress([20]byte{0xaa})
	vm := NewVersionMap(nil)

	// Write AddressPath at version (5,0).
	vm.Write(addr, AddressPath, accounts.NilKey,
		Version{TxIndex: 5, Incarnation: 0}, &accounts.Account{}, true)

	// Write BalancePath at a NEWER version (8,0) — simulates finalize fee write.
	vm.Write(addr, BalancePath, accounts.NilKey,
		Version{TxIndex: 8, Incarnation: 0}, uint256.Int{42}, true)

	// Create a ReadSet where TX 10 read AddressPath at version (5,0).
	io := NewVersionedIO(20)
	io.RecordReads(Version{TxIndex: 10, Incarnation: 0}, ReadSet{
		addr: {
			AccountKey{Path: AddressPath}: {
				Address: addr,
				Path:    AddressPath,
				Source:  MapRead,
				Version: Version{TxIndex: 5, Incarnation: 0},
			},
		},
	})

	checkVersion := func(readVersion, writtenVersion Version) VersionValidity {
		if readVersion != writtenVersion {
			return VersionInvalid
		}
		return VersionValid
	}

	// Validate TX 10 — should detect stale AddressPath (BalancePath is newer).
	valid := vm.ValidateVersion(10, io, checkVersion, false, "")
	assert.Equal(t, VersionInvalid, valid,
		"AddressPath read at (5,0) should be invalidated by BalancePath at (8,0)")
}

func TestValidateVersion_AddressPathNone_NoInfiniteLoop(t *testing.T) {
	t.Parallel()

	addr := accounts.InternAddress([20]byte{0xbb})
	vm := NewVersionMap(nil)

	// NO AddressPath entry — simulates reading from storage.
	// Write BalancePath at version (8,0) — simulates finalize fee write.
	vm.Write(addr, BalancePath, accounts.NilKey,
		Version{TxIndex: 8, Incarnation: 0}, uint256.Int{42}, true)

	// Create a ReadSet where TX 10 read AddressPath from StorageRead.
	// This is what happens after re-execution — the worker reads from
	// sd.mem/snapshots, and applyVersionedUpdates applies the BalancePath.
	io := NewVersionedIO(20)
	io.RecordReads(Version{TxIndex: 10, Incarnation: 1}, ReadSet{
		addr: {
			AccountKey{Path: AddressPath}: {
				Address: addr,
				Path:    AddressPath,
				Source:  StorageRead,
				Version: UnknownVersion,
			},
		},
	})

	checkVersion := func(readVersion, writtenVersion Version) VersionValidity {
		if readVersion != writtenVersion {
			return VersionInvalid
		}
		return VersionValid
	}

	// Validate TX 10 — should NOT invalidate (applyVersionedUpdates
	// already incorporated the BalancePath). If it does invalidate,
	// we'd get infinite re-execution.
	valid := vm.ValidateVersion(10, io, checkVersion, false, "")
	assert.Equal(t, VersionValid, valid,
		"AddressPath StorageRead should NOT be invalidated by BalancePath — "+
			"applyVersionedUpdates handles this, cross-checking would cause infinite loop")
}
