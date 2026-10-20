package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// createObjectChange.revert must drop the account-record cells that createObject
// emitted (AddressPath/CodeHashPath) so a reverted creation leaves no versioned
// write behind on the noMaterialize path — the journal keeps the write-set in
// step on its own, without dirties re-processing.
func TestNoMaterialize_CreateRevertDropsCells(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc0, 0x11})
	ibs := NewWithVersionMap(&emptyReader{}, NewVersionMap(nil))
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(0, 0)
	ibs.SetVersion(0)

	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.CreateAccount(addr, true))
	_, created := ibs.VersionedWrites().GetAddress(addr)
	require.True(t, created, "create records an AddressPath write")

	ibs.RevertToSnapshot(snap, nil)

	_, ok := ibs.VersionedWrites().GetAddress(addr)
	require.False(t, ok, "reverting the creation must drop the AddressPath write")
	exist, err := ibs.Exist(addr)
	require.NoError(t, err)
	require.False(t, exist, "the account must not exist after the creation is reverted")
}

// resetObjectChange.revert must restore, via restoreCreateFields, the
// account-record cells the recreation overwrote. createObject stamps the
// CodeHashPath to EmptyCodeHash, so a CreateAccount over a live contract clears
// its code hash; reverting must bring the prior hash back on the noMaterialize
// path (the journal keeps the write-set in step on its own).
func TestNoMaterialize_RecreateRevertRestoresPrior(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc0, 0x22})
	code := []byte("live-contract-code")
	committed := accounts.NewAccount()
	committed.Nonce = 5
	committed.Balance = *uint256.NewInt(1000)
	committed.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	reader := &fieldReader{addr: addr, account: &committed, code: code}
	ibs := NewWithVersionMap(reader, NewVersionMap(nil))
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(0, 0)
	ibs.SetVersion(0)

	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.CreateAccount(addr, true)) // stamps CodeHashPath -> empty
	ch, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, accounts.EmptyCodeHash, ch, "recreation clears the code hash")

	ibs.RevertToSnapshot(snap, nil)

	ch, err = ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, committed.CodeHash, ch, "reverting the recreation restores the prior code hash")
}

// A touch creates the account without a balance read, so the creation records the
// first one. After the revert the address must read as absent.
func TestNoMaterialize_TouchFundCreateRevertReadsZeroBalance(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc0, 0x33})
	ibs := NewWithVersionMap(&emptyReader{}, NewVersionMap(nil))
	ibs.SetNoMaterialize(true)

	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.TouchAccount(addr))
	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(91), tracing.BalanceChangeTransfer))
	require.NoError(t, ibs.CreateAccount(addr, true))
	ibs.RevertToSnapshot(snap, nil)

	balance, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.True(t, balance.IsZero(), "balance after revert: %s", balance.String())
}

// A credit in an earlier tx revives a destroyed account, and the creation records the
// first balance read. After the revert the address must keep the credit.
func TestNoMaterialize_RevivedCreateRevertReadsCellBalance(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc0, 0x44})
	vm := NewVersionMap(nil)
	ibs := newNoMaterializeTx(vm, 0)
	require.NoError(t, ibs.CreateAccount(addr, true))
	_, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	vm.FlushVersionedWrites(ibs.VersionedWrites(), true)
	ibs = newNoMaterializeTx(vm, 1)
	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(5), tracing.BalanceChangeTransfer))
	vm.FlushVersionedWrites(ibs.VersionedWrites(), true)

	ibs = newNoMaterializeTx(vm, 2)
	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.CreateAccount(addr, true))
	ibs.RevertToSnapshot(snap, nil)

	balance, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(5), balance.Uint64())
}

// On a versioned IBS that caches state objects, a credit creates the account
// with a zero balance in its record. A later tx credits it again and reverts a
// creation at it: the next tx must still read both credits. The EIP-161 rules
// matter: FinalizeTx deletes the stale empty object, which is what makes it
// observable.
func TestMaterialized_CreditedCreateRevertKeepsBalance(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc0, 0x55})
	rules := &chain.Rules{IsSpuriousDragon: true}
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(&emptyReader{}, vm)
	finalizeTx := func() {
		require.NoError(t, ibs.FinalizeTx(rules, NewNoopWriter()))
		vm.FlushVersionedWrites(ibs.FinalizedWrites(rules), true)
		ibs.ResetVersionedIO()
	}

	ibs.SetTxContext(1, 0)
	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(5), tracing.BalanceChangeTransfer))
	finalizeTx()

	ibs.SetTxContext(1, 1)
	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(3), tracing.BalanceChangeTransfer))
	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.CreateAccount(addr, true))
	ibs.RevertToSnapshot(snap, nil)
	finalizeTx()

	ibs.SetTxContext(1, 2)
	balance, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(8), balance.Uint64())
}

// An IBS that has not cached the account journals no object for a creation
// at it, and the revert must leave the cache without an entry.
func TestMaterialized_UncachedCreateRevertKeepsBalance(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc0, 0x66})
	rules := &chain.Rules{IsSpuriousDragon: true}
	vm := NewVersionMap(nil)
	funder := NewWithVersionMap(&emptyReader{}, vm)
	funder.SetTxContext(1, 0)
	require.NoError(t, funder.AddBalance(addr, *uint256.NewInt(5), tracing.BalanceChangeTransfer))
	vm.FlushVersionedWrites(funder.FinalizedWrites(rules), true)

	ibs := NewWithVersionMap(&emptyReader{}, vm)
	ibs.SetTxContext(1, 1)
	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(3), tracing.BalanceChangeTransfer))
	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.CreateAccount(addr, true))
	ibs.RevertToSnapshot(snap, nil)
	require.NoError(t, ibs.FinalizeTx(rules, NewNoopWriter()))
	vm.FlushVersionedWrites(ibs.FinalizedWrites(rules), true)
	ibs.ResetVersionedIO()

	ibs.SetTxContext(1, 2)
	balance, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.Equal(t, uint64(8), balance.Uint64())
}

// Without EIP-8246 a credit received after SELFDESTRUCT in the same tx burns with
// the account. A later re-creation must not bring it back after a revert.
func TestNoMaterialize_BurnedCreditCreateRevertReadsZeroBalance(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc0, 0x55})
	vm := NewVersionMap(nil)
	ibs := newNoMaterializeTx(vm, 0)
	require.NoError(t, ibs.CreateAccount(addr, true))
	_, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(9), tracing.BalanceChangeTransfer))
	vm.FlushVersionedWrites(ibs.VersionedWrites(), true)

	ibs = newNoMaterializeTx(vm, 1)
	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.CreateAccount(addr, false))
	ibs.RevertToSnapshot(snap, nil)

	balance, err := ibs.GetBalance(addr)
	require.NoError(t, err)
	require.True(t, balance.IsZero(), "balance after revert: %s", balance.String())
}

// A destruct-only cell above a burned credit (a fee touch of an absent coinbase)
// must not invalidate the creation's balance read on every validation.
func TestNoMaterialize_CreateAfterBurnedCreditAndDestructValidates(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc0, 0x66})
	vm := NewVersionMap(nil)
	ibs := newNoMaterializeTx(vm, 0)
	require.NoError(t, ibs.CreateAccount(addr, true))
	_, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(9), tracing.BalanceChangeTransfer))
	vm.FlushVersionedWrites(ibs.VersionedWrites(), true)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 1}, true, true)

	ibs = newNoMaterializeTx(vm, 2)
	require.NoError(t, ibs.CreateAccount(addr, false))

	io := NewVersionedIO(3)
	io.RecordReads(Version{TxIndex: 2}, ibs.VersionedReads())
	require.Equal(t, VersionValid, vm.ValidateVersion(2, io, validateEqualVersion, true, false, false, ""))
}

// The zero balance read by a creation over a burned credit depends on the
// destruct; when that destruct goes away, the read must be invalidated.
func TestNoMaterialize_CreateAfterBurnedCreditDependsOnDestruct(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc0, 0x77})
	vm := NewVersionMap(nil)
	vm.WriteBalance(addr, Version{TxIndex: 0}, *uint256.NewInt(9), true)
	vm.WriteSelfDestruct(addr, Version{TxIndex: 1}, true, true)

	ibs := newNoMaterializeTx(vm, 2)
	require.NoError(t, ibs.CreateAccount(addr, false))
	io := NewVersionedIO(3)
	io.RecordReads(Version{TxIndex: 2}, ibs.VersionedReads())
	require.Equal(t, VersionValid, vm.ValidateVersion(2, io, validateEqualVersion, true, false, false, ""))

	vm.Delete(addr, SelfDestructPath, accounts.NilKey, 1, true)
	require.Equal(t, VersionInvalid, vm.ValidateVersion(2, io, validateEqualVersion, true, false, false, ""))
}

func newNoMaterializeTx(vm *VersionMap, txIndex int) *IntraBlockState {
	ibs := NewWithVersionMap(&emptyReader{}, vm)
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(1, txIndex)
	return ibs
}

// SetCode of an account the tx created skips the transient stateObject on the
// noMaterialize path; the code must still read back and land in the write set.
func TestNoMaterialize_CreatedAccountSetCode(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc0, 0x44})
	code := []byte("created-contract-code")
	ibs := NewWithVersionMap(&emptyReader{}, NewVersionMap(nil))
	ibs.SetNoMaterialize(true)
	ibs.SetTxContext(0, 0)
	ibs.SetVersion(0)

	require.NoError(t, ibs.CreateAccount(addr, true))
	require.NoError(t, ibs.SetCode(addr, code, tracing.CodeChangeContractCreation))

	got, err := ibs.GetCode(addr)
	require.NoError(t, err)
	require.Equal(t, code, got)
	hash, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, accounts.InternCodeHash(crypto.Keccak256Hash(code)), hash)
	cw, ok := ibs.VersionedWrites().GetCode(addr)
	require.True(t, ok, "the code is this tx's write")
	require.Equal(t, code, cw.Val.Bytes)
}
