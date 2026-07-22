package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestSetStateParallel_NoMaterialize verifies that an SSTORE on the parallel
// (versionMap) path records the storage write through versioned-write cells
// without materializing/caching a stateObject.
func TestSetStateParallel_NoMaterialize(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xC0, 0xDE})
	key := accounts.InternKey([32]byte{0x01})
	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.Incarnation = 1

	reader := &storageReader{
		addr:    addr,
		account: &acc,
		storage: map[accounts.StorageKey]uint256.Int{key: *uint256.NewInt(5)},
	}
	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	require.NoError(t, ibs.SetState(addr, key, *uint256.NewInt(42)))

	writes := ibs.VersionedWrites()
	vw, ok := writes.GetStorage(addr, key)
	require.True(t, ok, "StoragePath write expected")
	assert.Equal(t, uint256.NewInt(42), &vw.Val)
	assert.Empty(t, ibs.stateObjects, "parallel SSTORE must not materialize a stateObject")

	// Reading the slot back within the tx returns the written value.
	got, err := ibs.GetState(addr, key)
	require.NoError(t, err)
	assert.Equal(t, uint256.NewInt(42), &got)
	assert.Empty(t, ibs.stateObjects)
}

// TestSetStateParallel_NoOpToCommitted verifies that writing the current
// committed value records no versioned write (matches stateObject.SetState's
// set decision).
func TestSetStateParallel_NoOpToCommitted(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xC0, 0xDE})
	key := accounts.InternKey([32]byte{0x02})
	acc := accounts.NewAccount()
	acc.Nonce = 1

	reader := &storageReader{
		addr:    addr,
		account: &acc,
		storage: map[accounts.StorageKey]uint256.Int{key: *uint256.NewInt(9)},
	}
	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	ibs.SetTxContext(100, 5)
	ibs.SetVersion(0)

	require.NoError(t, ibs.SetState(addr, key, *uint256.NewInt(9)))

	writes := ibs.VersionedWrites()
	_, ok := writes.GetStorage(addr, key)
	assert.False(t, ok, "writing the committed value should record no storage write")
	assert.Empty(t, ibs.stateObjects)
}
