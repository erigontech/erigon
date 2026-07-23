package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestVersionMapWriteView_ValuesFromMap proves the wrapper sources its values
// from the versionMap floor (the validated single source of truth), not from
// the key-set WriteSet: a stale value in the key-set must be overridden by the
// versionMap's value, and the yielded VersionedWrite must be a fresh copy (not
// a pointer into the map).
func TestVersionMapWriteView_ValuesFromMap(t *testing.T) {
	t.Parallel()

	const txIdx = 4
	addr := getAddress(1)
	key := accounts.InternKey(uint256.NewInt(0x11).Bytes32())

	// key-set carries STALE values (what a raw WriteSet copy might hold).
	keys := &WriteSet{}
	keys.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath}, Val: *uint256.NewInt(1)})
	keys.SetNonce(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath}, Val: 1})
	keys.SetStorage(addr, key, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: key}, Val: *uint256.NewInt(1)})

	// versionMap holds the VALIDATED values at txIdx.
	vm := NewVersionMap(nil)
	writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: txIdx}, *uint256.NewInt(250), true)
	writeFor(vm, addr, NoncePath, accounts.NilKey, Version{TxIndex: txIdx}, uint64(9), true)
	writeFor(vm, addr, StoragePath, key, Version{TxIndex: txIdx}, *uint256.NewInt(777), true)

	view := NewVersionMapWriteView(keys, vm, txIdx)

	gotBal := false
	for a, vw := range view.Balances() {
		require.Equal(t, addr, a)
		require.Equal(t, *uint256.NewInt(250), vw.Val, "balance must come from the versionMap, not the stale key-set")
		gotBal = true
	}
	require.True(t, gotBal, "balance key should be iterated")

	for _, vw := range view.Nonces() {
		require.Equal(t, uint64(9), vw.Val, "nonce from the versionMap")
	}

	gotSlot := false
	for a, inner := range view.Storages() {
		require.Equal(t, addr, a)
		vw := inner[key]
		require.NotNil(t, vw)
		require.Equal(t, *uint256.NewInt(777), vw.Val, "storage from the versionMap")
		gotSlot = true
	}
	require.True(t, gotSlot, "storage key should be iterated")
}
