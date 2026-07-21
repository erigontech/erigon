package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// EIP-8246 removes the SELFDESTRUCT balance burn, leaving a destroyed account
// alive as a balance-only account. These tests cover that behavior inside
// IntraBlockState: reconstructing a preserved account on the versioned-read
// path, resetting the in-memory object at end-of-tx so the block assembler's
// shared IBS carries the preserved balance into a later tx, and keeping the
// re-created incarnation aligned across execution modes.

// A destroyed-preserved contract's deployed code hash and nonce must not reach
// a later tx. Extraction drops nonce/code/codeHash for self-destructed
// accounts, so nothing in the version map carries them: GetCodeHash serves
// EmptyCodeHash through the preserved-account reconstruction fallback.
func TestEIP8246_PreservedSD_ReadsAsEmptyCodeAccountInLaterTx(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246B"))
	preserved := *uint256.NewInt(1)
	vm := NewVersionMap(nil)
	reader := newAccountStateReader()
	tx0 := New(reader)
	defer tx0.Release(false)
	tx0.SetTxContext(0, 0)
	tx0.SetVersion(0)
	tx0.SetVersionMap(vm)
	tx0.eip8246 = true
	require.NoError(t, tx0.CreateAccount(addr, true))
	require.NoError(t, tx0.SetBalance(addr, preserved, tracing.BalanceChangeUnspecified))
	require.NoError(t, tx0.SetCode(addr, []byte("deployed runtime code"), tracing.CodeChangeUnspecified))
	_, err := tx0.Selfdestruct(addr, true)
	require.NoError(t, err)
	require.NoError(t, tx0.MakeWriteSet(&chain.Rules{IsAmsterdam: true}, NewNoopWriter()))
	vm.FlushVersionedWrites(tx0.VersionedWrites(false), true, "")
	tx1 := New(reader)
	defer tx1.Release(false)
	tx1.SetTxContext(0, 1)
	tx1.SetVersion(0)
	tx1.SetVersionMap(vm)
	tx1.eip8246 = true
	ch, err := tx1.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, accounts.EmptyCodeHash, ch, "EXTCODEHASH of a preserved account must read empty, not the pre-destruct deployed hash")
	nonce, err := tx1.GetNonce(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(0), nonce, "the preserved account's nonce reads as cleared")
	exists, err := tx1.Exist(addr)
	require.NoError(t, err)
	require.True(t, exists, "the preserved account still exists")
}

// The block assembler runs every tx on one shared IBS (no per-tx Reset).
// After a balance-preserving SELFDESTRUCT is finalized, a later tx's CREATE2 at
// the same address must carry the preserved balance — i.e. FinalizeTx must not
// leave a stale selfdestructed marker on the in-memory object.
func TestEIP8246_FinalizeTx_PreservedBalanceCarriesToLaterTxCreate(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246C"))
	realCodeHash := accounts.InternCodeHash(common.HexToHash("0x31537ad3f3619e1f93aac0ddfdb0d8a0013bd170b427d81dd5abbee4f3f5248e"))
	reader := newAccountStateReader(addr)
	reader.accounts[addr].Nonce = 3
	reader.accounts[addr].CodeHash = realCodeHash
	reader.accounts[addr].Balance = *uint256.NewInt(100)

	rules := &chain.Rules{IsAmsterdam: true}
	ibs := New(reader)
	defer ibs.Release(false)

	ibs.SetTxContext(1, 0)
	_, err := ibs.Selfdestruct(addr, true /* preserveBalance */)
	require.NoError(t, err)
	require.NoError(t, ibs.FinalizeTx(rules, NewNoopWriter()))

	ibs.SetTxContext(1, 1)
	require.NoError(t, ibs.CreateAccount(addr, true))
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(100), bal, "EIP-8246: preserved balance must carry into a later-tx CREATE2 on the assembler's shared IBS")
}

// getVersionedAccount must not treat a later Balance/Nonce/CodeHash write
// as a revival. An in-block-created account that self-destructs (preserving
// balance) and is then funded by a later tx must still be reconstructed — with
// the later funding overlaid — so a concurrent reader sees it exist, matching
// serial. Only a genuine re-creation (a later AddressPath) skips reconstruction.
func TestEIP8246_VersionedAccount_LaterBalanceWriteDoesNotSkipReconstruction(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246D"))
	reader := newAccountStateReader() // in-block-created: no pre-block record

	vm := NewVersionMap(nil)
	sdVer := Version{TxIndex: 0}
	vm.WriteSelfDestruct(addr, sdVer, true, true)
	vm.WriteBalance(addr, sdVer, *uint256.NewInt(1), true)
	vm.WriteIncarnation(addr, sdVer, 1, true)
	// A later tx funds the preserved account — a balance write only, no re-creation.
	vm.WriteBalance(addr, Version{TxIndex: 1}, *uint256.NewInt(2), true)

	ibs := New(reader)
	defer ibs.Release(false)
	ibs.SetTxContext(0, 2)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)
	ibs.eip8246 = true

	exists, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.True(t, exists, "EIP-8246: a preserved account funded by a later tx must still exist for a concurrent reader")
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(2), bal, "reader must see the latest funded balance of the preserved account")
}

// The persisted balance-only record has incarnation 0, so a later CREATE2 over
// a preserved account must compute incarnation 1 in every execution mode.
// Selfdestruct publishes the pre-destruct IncarnationPath and extraction keeps
// it even for self-destructed accounts; without a superseding write the
// parallel worker's version map serves the stale value and the recreated
// contract's domain record diverges from serial and from the next block.
func TestEIP8246_CreateAfterPreservedSD_IncarnationAndBalanceAcrossModes(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246E1"))
	preserved := *uint256.NewInt(100)
	rules := &chain.Rules{IsAmsterdam: true}
	t.Run("worker-map-shaped", func(t *testing.T) {
		t.Parallel()
		reader := newAccountStateReader()
		vm := NewVersionMap(nil)
		tx0 := New(reader)
		defer tx0.Release(false)
		tx0.SetTxContext(0, 0)
		tx0.SetVersion(0)
		tx0.SetVersionMap(vm)
		tx0.eip8246 = true
		require.NoError(t, tx0.CreateAccount(addr, true))
		require.NoError(t, tx0.SetBalance(addr, preserved, tracing.BalanceChangeUnspecified))
		_, err := tx0.Selfdestruct(addr, true)
		require.NoError(t, err)
		require.NoError(t, tx0.MakeWriteSet(rules, NewNoopWriter()))
		vm.FlushVersionedWrites(tx0.VersionedWrites(false), true, "")
		tx1 := New(reader)
		defer tx1.Release(false)
		tx1.SetTxContext(0, 1)
		tx1.SetVersion(0)
		tx1.SetVersionMap(vm)
		tx1.eip8246 = true
		bal, err := tx1.GetBalance(addr)
		require.NoError(t, err)
		require.Equal(t, preserved, bal, "concurrent reader must see the preserved balance before re-creation")
		require.NoError(t, tx1.CreateAccount(addr, true))
		inc, err := tx1.GetIncarnation(addr)
		require.NoError(t, err)
		require.Equal(t, uint64(1), inc, "map-shaped re-creation must match serial and next-block incarnation")
		balAfter, err := tx1.GetBalance(addr)
		require.NoError(t, err)
		require.Equal(t, preserved, balAfter, "re-creation must carry the preserved balance")
	})
	t.Run("assembler-shared-ibs", func(t *testing.T) {
		t.Parallel()
		reader := newAccountStateReader()
		ibs := New(reader)
		defer ibs.Release(false)
		ibs.eip8246 = true
		ibs.SetTxContext(1, 0)
		require.NoError(t, ibs.CreateAccount(addr, true))
		require.NoError(t, ibs.SetBalance(addr, preserved, tracing.BalanceChangeUnspecified))
		_, err := ibs.Selfdestruct(addr, true)
		require.NoError(t, err)
		require.NoError(t, ibs.FinalizeTx(rules, NewNoopWriter()))
		ibs.SetTxContext(1, 1)
		require.NoError(t, ibs.CreateAccount(addr, true))
		inc, err := ibs.GetIncarnation(addr)
		require.NoError(t, err)
		require.Equal(t, uint64(1), inc, "assembler re-creation incarnation")
		bal, err := ibs.GetBalance(addr)
		require.NoError(t, err)
		require.Equal(t, preserved, bal, "assembler re-creation must carry the preserved balance")
	})
}

// Account-level reconstruction of a preserved account must agree with the
// field-level reads: a later tx's Nonce/CodeHash map entries overlay the
// reconstructed account exactly like a later balance write does, so a caller
// materializing the account through getStateObject sees the same fields a
// per-path read returns.
func TestEIP8246_PreservedAccount_OverlaysLaterFieldWrites(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246F"))
	laterCodeHash := accounts.InternCodeHash(common.HexToHash("0x31537ad3f3619e1f93aac0ddfdb0d8a0013bd170b427d81dd5abbee4f3f5248e"))
	reader := newAccountStateReader()
	vm := NewVersionMap(nil)
	sdVer := Version{TxIndex: 0}
	vm.WriteSelfDestruct(addr, sdVer, true, true)
	vm.WriteBalance(addr, sdVer, *uint256.NewInt(1), true)
	vm.WriteIncarnation(addr, sdVer, 0, true)
	laterVer := Version{TxIndex: 1}
	vm.WriteNonce(addr, laterVer, 7, true)
	vm.WriteCodeHash(addr, laterVer, laterCodeHash, true)
	ibs := New(reader)
	defer ibs.Release(false)
	ibs.SetTxContext(0, 2)
	ibs.SetVersion(0)
	ibs.SetVersionMap(vm)
	ibs.eip8246 = true
	fieldNonce, err := ibs.GetNonce(addr)
	require.NoError(t, err)
	fieldCodeHash, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	obj, err := ibs.getStateObject(addr, false)
	require.NoError(t, err)
	require.NotNil(t, obj, "the preserved account must materialize")
	require.Equal(t, fieldNonce, obj.data.Nonce, "account-level nonce must agree with the field-level read")
	require.Equal(t, fieldCodeHash, obj.data.CodeHash, "account-level code hash must agree with the field-level read")
	require.Equal(t, uint64(7), obj.data.Nonce)
	require.Equal(t, laterCodeHash, obj.data.CodeHash)
}
