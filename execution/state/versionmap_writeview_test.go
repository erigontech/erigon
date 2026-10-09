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

// A self-destruct present in the key-set but absent from the versionMap (a
// Normalize-synthesized SD, or a not-yet-resolved cell) must still be published
// as true — the view reproduces its backing WriteSet. Every other value iterator
// falls back to the key-set value on a map miss; SelfDestructs must too, else
// ApplyWrites never deletes the account.
func TestVersionMapWriteView_SelfDestructFallsBackToKeySetOnMapMiss(t *testing.T) {
	t.Parallel()

	const txIdx = 4
	addr := getAddress(3)

	keys := &WriteSet{}
	keys.SetSelfDestruct(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath}, Val: true})

	vm := NewVersionMap(nil) // no SelfDestruct cell for addr
	view := NewVersionMapWriteView(keys, vm, txIdx)

	got := false
	seen := false
	for _, vw := range view.SelfDestructs() {
		seen = true
		got = vw.Val
	}
	require.True(t, seen, "the self-destruct key should be iterated")
	require.True(t, got, "SelfDestructs must fall back to the key-set value on a versionMap miss, not drop the delete")
}

// StoragesChanged drops a write whose final value equals what the tx would have
// read, and keeps one that only looks equal: after a destruct the slot baseline
// is zero, so writing the pre-destruct value back is a real change.
func TestVersionMapWriteView_StoragesChanged(t *testing.T) {
	t.Parallel()

	const priorTx, myTx = 2, 5
	addr := getAddress(1)
	unchanged := accounts.InternKey(uint256.NewInt(0x11).Bytes32())
	changed := accounts.InternKey(uint256.NewInt(0x22).Bytes32())
	val100, val200 := *uint256.NewInt(100), *uint256.NewInt(200)

	// Real execution stamps each write from the tx's own prior read: a write-back of
	// the value the tx read is ValueUnchanged; a genuine update is ValueChanged.
	keys := &WriteSet{}
	keys.SetStorage(addr, unchanged, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: unchanged, valStatus: ValueUnchanged}, Val: val100})
	keys.SetStorage(addr, changed, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: changed, valStatus: ValueChanged}, Val: val200})

	vm := NewVersionMap(nil)
	writeFor(vm, addr, StoragePath, unchanged, Version{TxIndex: priorTx}, val100, true)
	writeFor(vm, addr, StoragePath, changed, Version{TxIndex: priorTx}, val100, true)
	writeFor(vm, addr, StoragePath, unchanged, Version{TxIndex: myTx}, val100, true)
	writeFor(vm, addr, StoragePath, changed, Version{TxIndex: myTx}, val200, true)

	view := NewVersionMapWriteView(keys, vm, myTx)

	all := map[accounts.StorageKey]bool{}
	for _, inner := range view.Storages() {
		for k := range inner {
			all[k] = true
		}
	}
	require.True(t, all[unchanged] && all[changed], "Storages must still yield every write")

	got := map[accounts.StorageKey]uint256.Int{}
	for _, inner := range view.StoragesChanged() {
		for k, vw := range inner {
			got[k] = vw.Val
		}
	}
	require.Equal(t, map[accounts.StorageKey]uint256.Int{changed: val200}, got,
		"only the slot stamped changed survives; the ValueUnchanged no-op is dropped")

	// After a destruct the tx reads zero, so writing the pre-destruct value back is
	// stamped ValueCreated at write time (not Unchanged) — a real change that survives.
	// The status rides on the write, so no vm re-derivation or destruct guard is needed.
	revivedKeys := &WriteSet{}
	revivedKeys.SetStorage(addr, unchanged, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: unchanged, valStatus: ValueCreated}, Val: val100})
	revived := map[accounts.StorageKey]uint256.Int{}
	for _, inner := range NewVersionMapWriteView(revivedKeys, vm, myTx).StoragesChanged() {
		for k, vw := range inner {
			revived[k] = vw.Val
		}
	}
	require.Equal(t, val100, revived[unchanged],
		"a post-destruct write-back is stamped Created, so it survives")
}

func TestStorageValueStatus(t *testing.T) {
	t.Parallel()
	zero := uint256.Int{}
	a, b := *uint256.NewInt(100), *uint256.NewInt(200)
	require.Equal(t, ValueUnchanged, storageValueStatus(a, a), "value == prev")
	require.Equal(t, ValueUnchanged, storageValueStatus(zero, zero), "absent stays absent")
	require.Equal(t, ValueCreated, storageValueStatus(zero, a), "absent -> value")
	require.Equal(t, ValueDeleted, storageValueStatus(a, zero), "value -> absent")
	require.Equal(t, ValueChanged, storageValueStatus(a, b), "value -> different value")
}
