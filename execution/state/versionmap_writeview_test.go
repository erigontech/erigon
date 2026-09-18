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

// TestVersionMapWriteView_FallsBackToKeySetOnMapMiss proves the wrapper is
// base-complete: when the versionMap has no cell for a key at txIdx (e.g. a
// normalize-filled field, or the 7702 SetCode short-circuit whose codeHash/code
// were resolved from committed state into the writeset, not the map), the view
// must yield the key-set's resolved value — not a zero. Reading vm-only and
// dropping the key-set value would persist an empty codeHash/code (wrong leaf).
func TestVersionMapWriteView_FallsBackToKeySetOnMapMiss(t *testing.T) {
	t.Parallel()

	const txIdx = 4
	addr := getAddress(2)
	designator := accounts.NewCode([]byte{0xef, 0x01, 0x00, 0x11, 0x22})

	// key-set carries the normalize-resolved codeHash + code; the versionMap has
	// NO cell for either at txIdx (the short-circuit / fill case).
	keys := &WriteSet{}
	keys.SetCodeHash(addr, &VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath}, Val: designator.Hash})
	keys.SetCode(addr, &VersionedWrite[accounts.Code]{WriteHeader: WriteHeader{Address: addr, Path: CodePath}, Val: designator})

	vm := NewVersionMap(nil)
	view := NewVersionMapWriteView(keys, vm, txIdx)

	gotHash := false
	for _, vw := range view.CodeHashes() {
		require.Equal(t, designator.Hash, vw.Val, "codeHash must fall back to the resolved key-set value on a versionMap miss")
		gotHash = true
	}
	require.True(t, gotHash, "codeHash key should be iterated")

	gotCode := false
	for _, vw := range view.Codes() {
		require.Equal(t, designator.Bytes, vw.Val.Bytes, "code must fall back to the resolved key-set value on a versionMap miss")
		gotCode = true
	}
	require.True(t, gotCode, "code key should be iterated")
}
