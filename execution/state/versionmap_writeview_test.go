package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestVersionMapWriteView_ValuesFromMap proves the wrapper sources its account
// values from the versionMap floor (the validated single source of truth), not
// from the key-set WriteSet: a stale value in the key-set must be overridden by
// the versionMap's value, and the yielded VersionedWrite must not be a pointer
// into the map.
func TestVersionMapWriteView_ValuesFromMap(t *testing.T) {
	t.Parallel()

	const txIdx = 4
	addr := getAddress(1)

	// key-set carries STALE values (what a raw WriteSet copy might hold).
	keys := &WriteSet{}
	keys.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath}, Val: *uint256.NewInt(1)})
	keys.SetNonce(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath}, Val: 1})

	// versionMap holds the VALIDATED values at txIdx.
	vm := NewVersionMap(nil)
	writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: txIdx}, *uint256.NewInt(250), true)
	writeFor(vm, addr, NoncePath, accounts.NilKey, Version{TxIndex: txIdx}, uint64(9), true)

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

// StorageChanges drops a write whose final value equals what the tx would have
// read, and keeps one that only looks equal: after a destruct the slot baseline
// is zero, so writing the pre-destruct value back is a real change.
func TestVersionMapWriteView_StorageChanges(t *testing.T) {
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
	for _, inner := range view.StorageChanges() {
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
	for _, inner := range NewVersionMapWriteView(revivedKeys, vm, myTx).StorageChanges() {
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

// TestVersionMapWriteView_BalanceKeepsDeleteStamp pins that the view carries the
// writer's delete stamp on a balance write, both when the value is re-read from
// the versionMap and when it falls back to the key-set: the commitment projection
// decides delete-vs-revive from that stamp.
func TestVersionMapWriteView_BalanceKeepsDeleteStamp(t *testing.T) {
	t.Parallel()

	const txIdx = 2
	burned := getAddress(1)
	missed := getAddress(2)

	keys := &WriteSet{}
	for _, addr := range []accounts.Address{burned, missed} {
		vw := &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: txIdx}}}
		vw.valStatus = ValueDeleted
		keys.SetBalance(addr, vw)
	}

	flushed := &WriteSet{}
	vw := &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: burned, Path: BalancePath, Version: Version{TxIndex: txIdx}}}
	vw.valStatus = ValueDeleted
	flushed.SetBalance(burned, vw)
	vm := NewVersionMap(nil)
	vm.FlushVersionedWrites(flushed, true)

	seen := 0
	for addr, vw := range NewVersionMapWriteView(keys, vm, txIdx).Balances() {
		require.True(t, vw.Deletes(), "balance write for %x lost its delete stamp", addr)
		seen++
	}
	require.Equal(t, 2, seen)
}

// An account the tx creates and self-destructs folds to the uniform balance-deleted
// removal verdict — whether it was absent before the tx, funded at base, funded by an
// earlier tx of the block, or left empty by an earlier tx. The removal is encoded as
// data (so commitment reads it absent without lifecycle re-derivation), not dropped;
// WithoutVanished leaves that verdict in place.
func TestWithoutVanished_CreatedAndDestroyedInTx(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0xC4EA7ED"))
	run := func(t *testing.T, reader StateReader, vm *VersionMap) *WriteSet {
		t.Helper()
		ibs := NewWithVersionMap(reader, vm)
		t.Cleanup(ibs.Close)
		ibs.SetTxContext(1, 1)
		require.NoError(t, ibs.CreateAccount(addr, true))
		require.NoError(t, ibs.SetNonce(addr, 1, tracing.NonceChangeNewContract))
		require.NoError(t, ibs.SetCode(addr, []byte{0x33, 0xff}, tracing.CodeChangeContractCreation))
		_, err := ibs.Selfdestruct(addr, false)
		require.NoError(t, err)
		ws := finalizedWrites(t, ibs, &chain.Rules{IsSpuriousDragon: true, IsCancun: true})
		vm.FlushVersionedWrites(ws, true)
		out, err := WithoutVanished(ws, vm, 1, false, reader)
		require.NoError(t, err)
		return out
	}
	removed := func(ws *WriteSet) bool {
		bal, ok := ws.GetBalance(addr)
		return ok && bal.Deletes()
	}
	require.True(t, removed(run(t, &minimalStateReader{}, NewVersionMap(nil))), "absent before the tx: folds to the removal verdict")
	require.True(t, removed(run(t, newAccountStateReader(addr), NewVersionMap(nil))), "a CREATE onto a pre-funded address deletes it")

	funded := NewVersionMap(nil)
	w := &WriteSet{}
	w.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath, Version: Version{TxIndex: 0}}, Val: *uint256.NewInt(5)})
	funded.FlushVersionedWrites(w, true)
	require.True(t, removed(run(t, &minimalStateReader{}, funded)), "an earlier tx of the block funded the address")

	// Before EIP-161 an earlier tx can leave an empty account that only its AddressPath cell records.
	empty := NewVersionMap(nil)
	w = &WriteSet{}
	w.SetAddress(addr, &VersionedWrite[*accounts.Account]{WriteHeader: WriteHeader{Address: addr, Path: AddressPath, Version: Version{TxIndex: 0}}, Val: &accounts.Account{CodeHash: accounts.EmptyCodeHash}})
	empty.FlushVersionedWrites(w, true)
	require.True(t, removed(run(t, &minimalStateReader{}, empty)), "an earlier tx of the block left an empty account")
}

// The view's value iterators reuse one VersionedWrite per call, so ranging one costs
// no allocation per yielded element.
func TestVersionMapWriteView_IteratorsDoNotAllocatePerElement(t *testing.T) {
	code := accounts.NewCode([]byte{0x60, 0x00})
	viewOf := func(n int) WriteSetView {
		keys := &WriteSet{}
		for i := range n {
			addr := getAddress(100 + i)
			keys.SetBalance(addr, &VersionedWrite[uint256.Int]{WriteHeader: WriteHeader{Address: addr, Path: BalancePath}, Val: *uint256.NewInt(uint64(i))})
			keys.SetNonce(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: NoncePath}, Val: uint64(i)})
			keys.SetIncarnation(addr, &VersionedWrite[uint64]{WriteHeader: WriteHeader{Address: addr, Path: IncarnationPath}, Val: 1})
			keys.SetCodeHash(addr, &VersionedWrite[accounts.CodeHash]{WriteHeader: WriteHeader{Address: addr, Path: CodeHashPath}, Val: code.Hash})
			keys.SetCode(addr, &VersionedWrite[accounts.Code]{WriteHeader: WriteHeader{Address: addr, Path: CodePath}, Val: code})
			keys.SetSelfDestruct(addr, &VersionedWrite[bool]{WriteHeader: WriteHeader{Address: addr, Path: SelfDestructPath}, Val: true})
		}
		return NewVersionMapWriteView(keys, NewVersionMap(nil), 4)
	}
	small, large := viewOf(2), viewOf(16)
	for name, rangeAll := range map[string]func(WriteSetView){
		"Balances": func(v WriteSetView) {
			for range v.Balances() {
			}
		},
		"Nonces": func(v WriteSetView) {
			for range v.Nonces() {
			}
		},
		"Incarnations": func(v WriteSetView) {
			for range v.Incarnations() {
			}
		},
		"CodeHashes": func(v WriteSetView) {
			for range v.CodeHashes() {
			}
		},
		"Codes": func(v WriteSetView) {
			for range v.Codes() {
			}
		},
		"SelfDestructs": func(v WriteSetView) {
			for range v.SelfDestructs() {
			}
		},
	} {
		perElement := (testing.AllocsPerRun(20, func() { rangeAll(large) }) - testing.AllocsPerRun(20, func() { rangeAll(small) })) / 14
		assert.Less(t, perElement, 0.5, "%s allocates %v per yielded element", name, perElement)
	}
}

// On a BAL block a net-zero storage write is not materialized in the version map, so the cell at the
// writer's index is the access list's, which can disagree with the tx. The view must publish the
// value the tx wrote.
func TestVersionMapWriteView_NetZeroStorageWritePublishesItsValue(t *testing.T) {
	t.Parallel()

	const txIdx = 1
	addr := getAddress(3)
	key := accounts.InternKey(common.Hash{0x04})
	vm := NewVersionMap(types.BlockAccessList{{Address: addr.Value(), StorageChanges: []types.SlotChanges{{Slot: key, Changes: []*types.StorageChange{{Index: txIdx + 1, Value: *uint256.NewInt(42)}}}}}})
	keys := &WriteSet{}
	keys.SetStorage(addr, key, &VersionedWrite[uint256.Int]{
		WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: key, Version: Version{TxIndex: txIdx}, valStatus: ValueUnchanged},
		Val:         *uint256.NewInt(5),
	})

	vm.FlushVersionedWrites(keys, false)

	published := 0
	for _, inner := range NewVersionMapWriteView(keys, vm, txIdx).Storages() {
		for _, vw := range inner {
			require.Equal(t, *uint256.NewInt(5), vw.Val)
			published++
		}
	}
	require.Equal(t, 1, published)
}
