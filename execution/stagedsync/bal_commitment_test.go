package stagedsync

import (
	"context"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type mockBALReader struct {
	accts   map[common.Address]*accounts.Account
	storage map[string]uint256.Int
}

func (m *mockBALReader) Read(d kv.Domain, plainKey []byte, _ uint64) ([]byte, kv.Step, error) {
	switch d {
	case kv.AccountsDomain:
		var a common.Address
		copy(a[:], plainKey)
		if acc, ok := m.accts[a]; ok {
			return accounts.SerialiseV3(acc), 0, nil
		}
	case kv.StorageDomain:
		if v, ok := m.storage[string(plainKey)]; ok {
			return v.Bytes(), 0, nil
		}
	}
	return nil, 0, nil
}

func balTestAddr(b byte) accounts.Address {
	var a common.Address
	a[19] = b
	return accounts.InternAddress(a)
}

func balTestSlot(b byte) accounts.StorageKey {
	var h common.Hash
	h[31] = b
	return accounts.InternKey(h)
}

func newBALUpdatesBuf(t *testing.T) *commitment.Updates {
	t.Helper()
	return commitment.NewUpdates(commitment.ModeUpdate, t.TempDir(), func(k []byte) []byte { return crypto.Keccak256(k) })
}

func dumpBALUpdates(t *testing.T, u *commitment.Updates) map[string]commitment.Update {
	t.Helper()
	out := map[string]commitment.Update{}
	err := u.HashSort(context.Background(), nil, func(_, pk []byte, upd *commitment.Update) error {
		out[string(pk)] = *upd
		return nil
	})
	require.NoError(t, err)
	return out
}

func acctKey(a accounts.Address) string {
	v := a.Value()
	return string(v[:])
}

func slotKey(a accounts.Address, s accounts.StorageKey) string {
	av := a.Value()
	sv := s.Value()
	return string(av[:]) + string(sv[:])
}

func TestBalToUpdates_HighestIndexAndFullAccount(t *testing.T) {
	baseCodeHash := crypto.Keccak256Hash([]byte("some-code"))
	a := balTestAddr(1)
	reader := &mockBALReader{accts: map[common.Address]*accounts.Account{
		a.Value(): {Nonce: 7, Balance: *uint256.NewInt(100), CodeHash: accounts.InternCodeHash(baseCodeHash)},
	}}
	bal := types.BlockAccessList{
		&types.AccountChanges{
			Address: a,
			BalanceChanges: []*types.BalanceChange{
				{Index: 1, Value: *uint256.NewInt(200)},
				{Index: 3, Value: *uint256.NewInt(500)},
			},
		},
	}
	dst := newBALUpdatesBuf(t)
	fallback, err := balToUpdates(bal, reader, dst, log.Root(), "test")
	require.NoError(t, err)
	require.False(t, fallback)

	got := dumpBALUpdates(t, dst)
	require.Len(t, got, 1)
	u, ok := got[acctKey(a)]
	require.True(t, ok)
	require.Equal(t, commitment.BalanceUpdate|commitment.NonceUpdate|commitment.CodeUpdate, u.Flags)
	require.Equal(t, uint256.NewInt(500), &u.Balance, "highest-Index balance wins")
	require.Equal(t, uint64(7), u.Nonce, "unchanged nonce loaded from pre-state baseline")
	require.Equal(t, baseCodeHash, u.CodeHash, "unchanged codeHash loaded from pre-state baseline")
}

func TestBalToUpdates_StorageSetAndDelete(t *testing.T) {
	a := balTestAddr(2)
	reader := &mockBALReader{}
	bal := types.BlockAccessList{
		&types.AccountChanges{
			Address: a,
			StorageChanges: []*types.SlotChanges{
				{Slot: balTestSlot(1), Changes: []*types.StorageChange{
					{Index: 1, Value: *uint256.NewInt(0)},
					{Index: 2, Value: *uint256.NewInt(42)},
				}},
				{Slot: balTestSlot(2), Changes: []*types.StorageChange{
					{Index: 1, Value: *uint256.NewInt(9)},
					{Index: 4, Value: *uint256.NewInt(0)},
				}},
			},
		},
	}
	dst := newBALUpdatesBuf(t)
	fallback, err := balToUpdates(bal, reader, dst, log.Root(), "test")
	require.NoError(t, err)
	require.False(t, fallback)

	got := dumpBALUpdates(t, dst)
	require.NotContains(t, got, acctKey(a), "storage-only account gets no account leaf touch")
	require.Equal(t, commitment.StorageUpdate, got[slotKey(a, balTestSlot(1))].Flags, "final non-zero slot is a StorageUpdate")
	require.Equal(t, commitment.DeleteUpdate, got[slotKey(a, balTestSlot(2))].Flags, "final-zero slot is a DeleteUpdate")
}

func TestBalToUpdates_SyntheticEmptyGuardFallback(t *testing.T) {
	a := balTestAddr(3)
	reader := &mockBALReader{accts: map[common.Address]*accounts.Account{
		a.Value(): {Nonce: 0, Balance: *uint256.NewInt(50), CodeHash: accounts.EmptyCodeHash},
	}}
	bal := types.BlockAccessList{
		&types.AccountChanges{
			Address:        a,
			BalanceChanges: []*types.BalanceChange{{Index: 1, Value: *uint256.NewInt(0)}},
		},
	}
	dst := newBALUpdatesBuf(t)
	fallback, err := balToUpdates(bal, reader, dst, log.Root(), "test")
	require.NoError(t, err)
	require.True(t, fallback, "balance->0 on a codeless, nonce-0 account is a potential EIP-161 empty -> fallback")
}

func TestBalToUpdates_ContractToZeroBalanceStaysFastPath(t *testing.T) {
	codeHash := crypto.Keccak256Hash([]byte("contract"))
	a := balTestAddr(5)
	reader := &mockBALReader{accts: map[common.Address]*accounts.Account{
		a.Value(): {Nonce: 1, Balance: *uint256.NewInt(50), CodeHash: accounts.InternCodeHash(codeHash)},
	}}
	bal := types.BlockAccessList{
		&types.AccountChanges{
			Address:        a,
			BalanceChanges: []*types.BalanceChange{{Index: 1, Value: *uint256.NewInt(0)}},
		},
	}
	dst := newBALUpdatesBuf(t)
	fallback, err := balToUpdates(bal, reader, dst, log.Root(), "test")
	require.NoError(t, err)
	require.False(t, fallback, "a contract spending to zero keeps its code, so it is not empty")
	got := dumpBALUpdates(t, dst)
	u := got[acctKey(a)]
	require.True(t, u.Balance.IsZero())
	require.Equal(t, codeHash, u.CodeHash)
}

func TestBalToUpdates_CodeAndNonceChange(t *testing.T) {
	a := balTestAddr(4)
	code := []byte{0x60, 0x00, 0x60, 0x00}
	reader := &mockBALReader{}
	bal := types.BlockAccessList{
		&types.AccountChanges{
			Address:      a,
			NonceChanges: []*types.NonceChange{{Index: 1, Value: 1}},
			CodeChanges:  []*types.CodeChange{{Index: 1, Bytecode: code}},
		},
	}
	dst := newBALUpdatesBuf(t)
	fallback, err := balToUpdates(bal, reader, dst, log.Root(), "test")
	require.NoError(t, err)
	require.False(t, fallback)
	got := dumpBALUpdates(t, dst)
	u := got[acctKey(a)]
	require.Equal(t, crypto.Keccak256Hash(code), u.CodeHash)
	require.Equal(t, uint64(1), u.Nonce)
}

// TestBalToUpdates_DifferentialVsTxResults asserts the BAL reducer produces the
// same Updates as accumulating the equivalent per-tx writes through calcState
// (the post-execution path) — the keystone equality, with no net-zero churn.
func TestBalToUpdates_DifferentialVsTxResults(t *testing.T) {
	baseCodeHash := crypto.Keccak256Hash([]byte("base"))
	a1 := balTestAddr(10)
	a2 := balTestAddr(11)
	reader := &mockBALReader{accts: map[common.Address]*accounts.Account{
		a1.Value(): {Nonce: 2, Balance: *uint256.NewInt(1000), CodeHash: accounts.InternCodeHash(baseCodeHash)},
		a2.Value(): {Nonce: 0, Balance: *uint256.NewInt(0), CodeHash: accounts.EmptyCodeHash},
	}}

	// Post-execution path: accumulate per-tx writes then flush.
	cs := newCalcState(reader, log.Root(), "test")
	cs.ApplyWrites(state.VersionedWrites{
		{Path: state.BalancePath, Address: a1, Val: *uint256.NewInt(900)},
		{Path: state.StoragePath, Address: a1, Key: balTestSlot(7), Val: *uint256.NewInt(0)},
	})
	cs.ApplyWrites(state.VersionedWrites{
		{Path: state.BalancePath, Address: a1, Val: *uint256.NewInt(1200)},
		{Path: state.NoncePath, Address: a1, Val: uint64(3)},
		{Path: state.BalancePath, Address: a2, Val: *uint256.NewInt(5)},
		{Path: state.StoragePath, Address: a1, Key: balTestSlot(7), Val: *uint256.NewInt(88)},
	})
	wantBuf := newBALUpdatesBuf(t)
	cs.FlushToUpdates(wantBuf)
	want := dumpBALUpdates(t, wantBuf)

	// BAL path: same net effect, highest-Index per key.
	bal := types.BlockAccessList{
		&types.AccountChanges{
			Address: a1,
			BalanceChanges: []*types.BalanceChange{
				{Index: 1, Value: *uint256.NewInt(900)},
				{Index: 2, Value: *uint256.NewInt(1200)},
			},
			NonceChanges: []*types.NonceChange{{Index: 2, Value: 3}},
			StorageChanges: []*types.SlotChanges{
				{Slot: balTestSlot(7), Changes: []*types.StorageChange{
					{Index: 1, Value: *uint256.NewInt(0)},
					{Index: 2, Value: *uint256.NewInt(88)},
				}},
			},
		},
		&types.AccountChanges{
			Address:        a2,
			BalanceChanges: []*types.BalanceChange{{Index: 2, Value: *uint256.NewInt(5)}},
		},
	}
	gotBuf := newBALUpdatesBuf(t)
	fallback, err := balToUpdates(bal, reader, gotBuf, log.Root(), "test")
	require.NoError(t, err)
	require.False(t, fallback)
	got := dumpBALUpdates(t, gotBuf)

	require.Equal(t, want, got, "BAL-seeded updates must equal txResult-accumulated updates")
}

// fakeStateReader returns preset pre-state encodings; it stands in for the
// pre-state domain reader behind balFoldReader.
type fakeStateReader struct {
	reads map[string][]byte
}

func (f *fakeStateReader) WithHistory() bool                            { return false }
func (f *fakeStateReader) CheckDataAvailable(kv.Domain, kv.Step) error  { return nil }
func (f *fakeStateReader) Clone(kv.TemporalTx) commitmentdb.StateReader { return f }
func (f *fakeStateReader) Read(d kv.Domain, k []byte, _ uint64) ([]byte, kv.Step, error) {
	return f.reads[fmt.Sprintf("%d:%x", d, k)], 0, nil
}

// TestBalFoldReader_OverlaysFinalValues asserts the fold reader returns the
// BAL's block-final values for changed account/storage keys (so the trie's
// storage-root recomputation loads final, not stale, fields) and falls through
// to the pre-state base reader for unchanged keys.
func TestBalFoldReader_OverlaysFinalValues(t *testing.T) {
	changed := balTestAddr(1)
	unchanged := balTestAddr(2)
	codeHash := crypto.Keccak256Hash([]byte("contract"))
	changedAddr := changed.Value()
	unchangedAddr := unchanged.Value()
	// Pre-state: changed has OLD balance 100; unchanged has balance 7.
	base := &fakeStateReader{reads: map[string][]byte{
		fmt.Sprintf("%d:%x", kv.AccountsDomain, changedAddr[:]): accounts.SerialiseV3(&accounts.Account{
			Nonce: 5, Balance: *uint256.NewInt(100), CodeHash: accounts.InternCodeHash(codeHash), Root: empty.RootHash,
		}),
		fmt.Sprintf("%d:%x", kv.AccountsDomain, unchangedAddr[:]): accounts.SerialiseV3(&accounts.Account{
			Nonce: 1, Balance: *uint256.NewInt(7), CodeHash: accounts.EmptyCodeHash, Root: empty.RootHash,
		}),
	}}

	cs := newCalcState(base, log.Root(), "test")
	acc := cs.ensureAccount(changed) // loads baseline (nonce 5, bal 100, code)
	acc.Balance = *uint256.NewInt(500)
	acc.dirty = true
	cs.setBALStorage(changed, balTestSlot(7), *uint256.NewInt(99))

	r := &balFoldReader{base: base, cs: cs, txNum: 0}

	enc, _, err := r.Read(kv.AccountsDomain, changedAddr[:], 0)
	require.NoError(t, err)
	var got accounts.Account
	require.NoError(t, accounts.DeserialiseV3(&got, enc))
	require.Equal(t, uint256.NewInt(500), &got.Balance, "changed account returns block-final balance, not pre-state")
	require.Equal(t, uint64(5), got.Nonce, "unchanged nonce preserved from pre-state baseline")

	slotKeyBytes := func(a accounts.Address, s accounts.StorageKey) []byte {
		av := a.Value()
		sv := s.Value()
		return append(append([]byte{}, av[:]...), sv[:]...)
	}
	senc, _, err := r.Read(kv.StorageDomain, slotKeyBytes(changed, balTestSlot(7)), 0)
	require.NoError(t, err)
	require.Equal(t, uint256.NewInt(99).Bytes(), senc, "changed slot returns block-final value")

	uenc, _, err := r.Read(kv.AccountsDomain, unchangedAddr[:], 0)
	require.NoError(t, err)
	var ugot accounts.Account
	require.NoError(t, accounts.DeserialiseV3(&ugot, uenc))
	require.Equal(t, uint256.NewInt(7), &ugot.Balance, "unchanged account falls through to pre-state base reader")
}

func TestBalFoldReader_EmptyAccountReturnsNil(t *testing.T) {
	a := balTestAddr(3)
	cs := newCalcState(&fakeStateReader{reads: map[string][]byte{}}, log.Root(), "test")
	acc := cs.ensureAccount(a)
	acc.Deleted = true
	acc.dirty = true
	enc := cs.encodeFinalAccount(acc)
	require.Nil(t, enc, "an emptied account encodes to nil (removed from the trie)")
}
