package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// The read rule-set for a destroyed/removed account, tested against a real committed
// base (a prior block's state) so the base-resolution path is exercised, not a genesis
// stub. The write side encodes the destruct as either a SelfDestruct cell (burn /
// EIP-8246 no-burn) or a balance ValueDeleted (EIP-161 touched-empty removal, burn); the
// read must suppress the committed base for such an account's storage and surface only
// writes made at or above the destruct (a revival's own data).

// committedBase serves a prior block's committed account + storage as the version map's
// base cascade tail.
func committedBase(addr accounts.Address, acc *accounts.Account, storage map[accounts.StorageKey]uint256.Int) *storageReader {
	return &storageReader{addr: addr, account: acc, storage: storage}
}

// XB1: an account committed in block N with storage S=V is touched-empty-removed
// (EIP-161) in block N+1. The read of S in N+1 must wipe to 0 (committed base
// suppressed), and the account must read absent.
func TestReadRule_XB1_RemovedAccountWipesCommittedStorage(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0x58})
	slot := accounts.InternKey([32]byte{0x01})
	committed := accounts.NewAccount() // empty (bal 0, nonce 0, no code) -> EIP-161-removable
	base := committedBase(addr, &committed, map[accounts.StorageKey]uint256.Int{slot: *uint256.NewInt(9)})

	vm := NewVersionMap(nil)
	ws := &WriteSet{}
	ws.SetEmptyRemoval(addr, Version{TxIndex: 0}) // block N+1 removes it (EIP-161, no SelfDestruct cell)
	vm.FlushVersionedWrites(ws, true)

	vr := NewVersionedStateReader(1, ReadSet{}, vm, base)
	val, _, err := vr.ReadAccountStorage(addr, slot)
	require.NoError(t, err)
	require.True(t, val.IsZero(), "a removed account's committed storage slot must read 0 (base suppressed), got %s", val.String())

	// The commitment read must agree: account absent, slot deleted.
	view := vm.View(1, base)
	_, ok := view.GetAccount(addr)
	require.False(t, ok, "an EIP-161-removed account reads absent in the commitment view")
	sv, _ := view.GetStorage(addr, slot)
	require.True(t, sv.IsZero(), "committed slot wiped in the commitment view, got %s", sv.String())
}

// XB2 (guard): a non-removal touch of a committed account must NOT suppress its committed
// storage — the base-resolution rule only fires for a destroyed/removed account.
func TestReadRule_XB2_NonRemovalTouchKeepsCommittedStorage(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0x59})
	slot := accounts.InternKey([32]byte{0x01})
	committed := accounts.NewAccount()
	committed.Balance = *uint256.NewInt(100) // non-empty: not removable
	base := committedBase(addr, &committed, map[accounts.StorageKey]uint256.Int{slot: *uint256.NewInt(9)})

	vm := NewVersionMap(nil)
	vm.WriteBalance(addr, Version{TxIndex: 0}, *uint256.NewInt(101), true) // touched, not removed

	vr := NewVersionedStateReader(1, ReadSet{}, vm, base)
	val, _, err := vr.ReadAccountStorage(addr, slot)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(9), val, "a non-removal touch must leave the committed slot readable")
}

// XB3: an account committed in block N is removed then revived (value transfer) in N+1.
// The revived account is present but inherits no committed storage.
func TestReadRule_XB3_RemovedThenRevivedWipesCommittedStorage(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0x5a})
	slot := accounts.InternKey([32]byte{0x01})
	committed := accounts.NewAccount()
	base := committedBase(addr, &committed, map[accounts.StorageKey]uint256.Int{slot: *uint256.NewInt(9)})

	vm := NewVersionMap(nil)
	ws := &WriteSet{}
	ws.SetEmptyRemoval(addr, Version{TxIndex: 0}) // removed in N+1 @0
	vm.FlushVersionedWrites(ws, true)
	vm.WriteBalance(addr, Version{TxIndex: 1}, *uint256.NewInt(5), true) // revived by a credit @1

	vr := NewVersionedStateReader(2, ReadSet{}, vm, base)
	val, _, err := vr.ReadAccountStorage(addr, slot)
	require.NoError(t, err)
	require.True(t, val.IsZero(), "a revived account inherits no committed storage, got %s", val.String())

	view := vm.View(2, base)
	acc, ok := view.GetAccount(addr)
	require.True(t, ok, "the revived account is present")
	require.Equal(t, *uint256.NewInt(5), acc.Balance, "revived balance")
}

// A1 (guard on the SD leg): a burning self-destruct already wipes committed storage via
// the SelfDestruct cell. Pins that the SD path keeps working (the rule must subsume it).
func TestReadRule_A1_BurnSelfDestructWipesCommittedStorage(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0x5b})
	slot := accounts.InternKey([32]byte{0x01})
	committed := accounts.NewAccount()
	committed.Balance = *uint256.NewInt(50)
	base := committedBase(addr, &committed, map[accounts.StorageKey]uint256.Int{slot: *uint256.NewInt(9)})

	vm := NewVersionMap(nil)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 0}, true, true)

	vr := NewVersionedStateReader(1, ReadSet{}, vm, base)
	val, _, err := vr.ReadAccountStorage(addr, slot)
	require.NoError(t, err)
	require.True(t, val.IsZero(), "a self-destructed account's committed slot reads 0, got %s", val.String())
}

// B3: a removed account revived with a NEW slot write above the destruct — the new slot is
// visible (recreated account's own data), while a committed slot it did NOT rewrite stays 0.
func TestReadRule_B3_PostRemovalWriteVisibleBaseSuppressed(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0x5c})
	slotS := accounts.InternKey([32]byte{0x01}) // committed, not rewritten
	slotT := accounts.InternKey([32]byte{0x02}) // written by the revival
	committed := accounts.NewAccount()
	base := committedBase(addr, &committed, map[accounts.StorageKey]uint256.Int{slotS: *uint256.NewInt(9)})

	vm := NewVersionMap(nil)
	ws := &WriteSet{}
	ws.SetEmptyRemoval(addr, Version{TxIndex: 0})
	vm.FlushVersionedWrites(ws, true)
	vm.WriteBalance(addr, Version{TxIndex: 1}, *uint256.NewInt(5), true)        // revive @1
	vm.WriteStorage(addr, slotT, Version{TxIndex: 1}, *uint256.NewInt(7), true) // revival-era slot write @1

	vr := NewVersionedStateReader(2, ReadSet{}, vm, base)
	valT, _, err := vr.ReadAccountStorage(addr, slotT)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(7), valT, "a post-removal slot write is visible")
	valS, _, err := vr.ReadAccountStorage(addr, slotS)
	require.NoError(t, err)
	require.True(t, valS.IsZero(), "a committed slot the revival did not rewrite stays 0, got %s", valS.String())
}

// XB4: the prevBlocks cascade — a removal in a sealed prior-block layer must suppress the
// committed base storage beneath it when read through the layered reader.
func TestReadRule_XB4_PrevBlockCascadeRemovalSuppressesStorage(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0x5d})
	slot := accounts.InternKey([32]byte{0x01})
	base := &fakeBaseReader{
		accts:   map[accounts.Address]*accounts.Account{addr: acctBal(0)},
		storage: map[accounts.Address]map[accounts.StorageKey]uint256.Int{addr: {slot: *uint256.NewInt(9)}},
	}
	removal := NewVersionMap(nil)
	ws := &WriteSet{}
	ws.SetEmptyRemoval(addr, Version{TxIndex: 0})
	removal.FlushVersionedWrites(ws, true)
	removal.SealUpTo(0)

	r := layerVersionMaps(base, []*VersionMap{removal})
	val, _, err := r.ReadAccountStorage(addr, slot)
	require.NoError(t, err)
	require.True(t, val.IsZero(), "a prior-block removal must suppress the committed slot across the cascade, got %s", val.String())
}

// A3 (EIP-8246 field leg): a no-burn self-destruct of a previously-committed contract keeps
// the balance but resets the account to balance-only — so against a committed base that still
// holds the old nonce/code/storage, the read must surface preserved balance, empty nonce,
// empty codeHash, and wiped storage (base suppressed for every field except balance).
func TestReadRule_A3_EIP8246PreserveSuppressesCommittedFieldsAndStorage(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0x5e})
	slot := accounts.InternKey([32]byte{0x01})
	committed := accounts.NewAccount()
	committed.Balance = *uint256.NewInt(100)
	committed.Nonce = 3
	committed.CodeHash = accounts.InternCodeHash(common.HexToHash("0x31537ad3f3619e1f93aac0ddfdb0d8a0013bd170b427d81dd5abbee4f3f5248e"))
	base := committedBase(addr, &committed, map[accounts.StorageKey]uint256.Int{slot: *uint256.NewInt(9)})

	vm := NewVersionMap(nil)
	sd := Version{TxIndex: 0}
	vm.WriteSelfDestruct(addr, sd, true, true)
	vm.WriteBalance(addr, sd, *uint256.NewInt(100), true) // preserved (ValueChanged, not ValueDeleted)
	vm.WriteIncarnation(addr, sd, 0, true)

	vr := NewVersionedStateReader(1, ReadSet{}, vm, base)
	acc, err := vr.ReadAccountData(addr)
	require.NoError(t, err)
	require.NotNil(t, acc, "a preserved self-destruct still exists")
	require.Equal(t, *uint256.NewInt(100), acc.Balance, "balance preserved")
	require.Equal(t, uint64(0), acc.Nonce, "nonce reset, not the committed 3")
	require.NotEqual(t, committed.CodeHash, acc.CodeHash, "codeHash must not leak the committed code")
	require.True(t, acc.CodeHash == accounts.EmptyCodeHash || acc.CodeHash == (accounts.CodeHash{}), "codeHash reset to empty, got %x", acc.CodeHash.Value())

	val, _, err := vr.ReadAccountStorage(addr, slot)
	require.NoError(t, err)
	require.True(t, val.IsZero(), "a preserved account's committed storage is wiped, got %s", val.String())
}
