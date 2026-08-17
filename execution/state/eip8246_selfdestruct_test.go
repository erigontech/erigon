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

// EIP-8246 removes SELFDESTRUCT's balance burn; these tests cover the resulting balance-only preserved account in IntraBlockState.

// A destroyed-preserved account's nonce and code hash must not reach a later tx: extraction drops both, so GetCodeHash falls back to EmptyCodeHash.
func TestEIP8246_PreservedSD_ReadsAsEmptyCodeAccountInLaterTx(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246B"))
	preserved := *uint256.NewInt(1)
	vm := NewVersionMap(nil)
	reader := newAccountStateReader()
	tx0 := New(reader)
	defer tx0.Close()
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
	vm.FlushVersionedWrites(tx0.VersionedWrites(), true, "")
	tx1 := New(reader)
	defer tx1.Close()
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

// The block assembler shares one IBS across all txs (no per-tx Reset), so FinalizeTx must not leave a stale selfdestruct marker after a preserving SD.
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
	defer ibs.Close()

	ibs.SetTxContext(1, 0)
	_, err := ibs.Selfdestruct(addr, true)
	require.NoError(t, err)
	require.NoError(t, ibs.FinalizeTx(rules, NewNoopWriter()))

	ibs.SetTxContext(1, 1)
	require.NoError(t, ibs.CreateAccount(addr, true))
	bal, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(100), bal, "EIP-8246: preserved balance must carry into a later-tx CREATE2 on the assembler's shared IBS")
}

// getVersionedAccount must not mistake a later Balance/Nonce/CodeHash write for
// a revival — only a later AddressPath write re-creates the account.
func TestEIP8246_VersionedAccount_LaterBalanceWriteDoesNotSkipReconstruction(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress(common.HexToAddress("0x8246D"))
	reader := newAccountStateReader()

	vm := NewVersionMap(nil)
	sdVer := Version{TxIndex: 0}
	vm.WriteSelfDestruct(addr, sdVer, true, true)
	vm.WriteBalance(addr, sdVer, *uint256.NewInt(1), true)
	vm.WriteIncarnation(addr, sdVer, 1, true)
	vm.WriteBalance(addr, Version{TxIndex: 1}, *uint256.NewInt(2), true)

	ibs := New(reader)
	defer ibs.Close()
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

// The persisted preserved-balance record has incarnation 0, so a later CREATE2 must compute incarnation 1 consistently across execution modes.
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
		defer tx0.Close()
		tx0.SetTxContext(0, 0)
		tx0.SetVersion(0)
		tx0.SetVersionMap(vm)
		tx0.eip8246 = true
		require.NoError(t, tx0.CreateAccount(addr, true))
		require.NoError(t, tx0.SetBalance(addr, preserved, tracing.BalanceChangeUnspecified))
		_, err := tx0.Selfdestruct(addr, true)
		require.NoError(t, err)
		require.NoError(t, tx0.MakeWriteSet(rules, NewNoopWriter()))
		vm.FlushVersionedWrites(tx0.VersionedWrites(), true, "")
		tx1 := New(reader)
		defer tx1.Close()
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
		defer ibs.Close()
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

// Account-level reconstruction of a preserved account must agree with field-level reads: later Nonce/CodeHash writes overlay both the same way.
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
	defer ibs.Close()
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
