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
		vm.FlushVersionedWrites(finalizedWrites(t, ibs, rules), true)
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
	vm.FlushVersionedWrites(finalizedWrites(t, funder, rules), true)

	ibs := NewWithVersionMap(&emptyReader{}, vm)
	ibs.SetTxContext(1, 1)
	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(3), tracing.BalanceChangeTransfer))
	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.CreateAccount(addr, true))
	ibs.RevertToSnapshot(snap, nil)
	require.NoError(t, ibs.FinalizeTx(rules, NewNoopWriter()))
	vm.FlushVersionedWrites(finalizedWrites(t, ibs, rules), true)
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
	require.Equal(t, VersionValid, vm.ValidateVersion(2, io, validateEqualVersion, true, ""))
}

// The zero balance read by a creation over a burned credit depends on the
// destruct; when that destruct goes away, the read must be invalidated.
func TestNoMaterialize_CreateAfterBurnedCreditDependsOnDestruct(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc0, 0x77})
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
	require.Equal(t, VersionValid, vm.ValidateVersion(2, io, validateEqualVersion, true, ""))

	vm.Delete(addr, SelfDestructPath, accounts.NilKey, 1, true)
	require.Equal(t, VersionInvalid, vm.ValidateVersion(2, io, validateEqualVersion, true, ""))
}

// A tx that re-creates an account and sets its code back to the pre-block original
// must still record the code write. The net-zero "matches original" skip applies
// only to accounts the tx did NOT create; keying it off base==nil wrongly treated a
// re-created account (whose own AddressPath write makes base non-nil) as not-created,
// dropping the Code/CodeHash writes and leaving it code-less after the recreation
// cleared the code hash.
func TestNoMaterialize_RecreateWithOriginalCodeKeepsCodeWrite(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc0, 0xcc})
	code := []byte("recreated-same-code")
	codeHash := accounts.InternCodeHash(crypto.Keccak256Hash(code))
	committed := accounts.NewAccount()
	committed.Nonce = 1
	committed.Balance = *uint256.NewInt(1000)
	committed.CodeHash = codeHash
	ibs := NewWithVersionMap(&fieldReader{addr: addr, account: &committed, code: code}, NewVersionMap(nil))
	ibs.SetTxContext(1, 0)

	// Create the account and set code X, then self-destruct it, then (post-snapshot)
	// clear the code and revert. The revert must restore code X. The net-zero skip
	// must not fire for this tx-created account against its pre-block original hash.
	require.NoError(t, ibs.CreateAccount(addr, true))
	require.NoError(t, ibs.SetCode(addr, code, tracing.CodeChangeContractCreation))
	_, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.SetCode(addr, nil, tracing.CodeChangeUnspecified))
	ibs.RevertToSnapshot(snap, nil)

	got, err := ibs.GetCode(addr)
	require.NoError(t, err)
	require.Equal(t, code, got, "reverting the code clear must restore code X, not net-zero it away")
	ch, err := ibs.GetCodeHash(addr)
	require.NoError(t, err)
	require.Equal(t, codeHash, ch)
}

// codeSeed must resolve the previous code bytes from the version map when an
// earlier tx in the block installed the code (its bytes are not in the pre-block
// origin). Otherwise re-setting the identical code emits a spurious code-change
// hook, and replacing it reports empty previous bytes to tracers.
func TestNoMaterialize_SetCodeUsesEarlierTxCodeAsSeed(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc0, 0xdd})
	codeX := []byte("earlier-tx-code-x")
	vm := NewVersionMap(nil)
	ibs0 := newNoMaterializeTx(vm, 0)
	require.NoError(t, ibs0.CreateAccount(addr, true))
	require.NoError(t, ibs0.SetCode(addr, codeX, tracing.CodeChangeContractCreation))
	vm.FlushVersionedWrites(ibs0.VersionedWrites(), true)

	type change struct{ prev, next []byte }
	codeHook := func(rec *[]change) *tracing.Hooks {
		return &tracing.Hooks{OnCodeChange: func(_ accounts.Address, _ accounts.CodeHash, prev []byte, _ accounts.CodeHash, code []byte) {
			*rec = append(*rec, change{prev, code})
		}}
	}

	// Re-setting the identical code installed by the earlier tx is a no-op.
	var ch1 []change
	ibs1 := newNoMaterializeTx(vm, 1)
	ibs1.SetHooks(codeHook(&ch1))
	require.NoError(t, ibs1.SetCode(addr, codeX, tracing.CodeChangeUnspecified))
	require.Empty(t, ch1, "re-setting the earlier tx's code must emit no code change")

	// Replacing it reports the earlier tx's code bytes as the previous value.
	codeY := []byte("later-code-y")
	var ch2 []change
	ibs2 := newNoMaterializeTx(vm, 2)
	ibs2.SetHooks(codeHook(&ch2))
	require.NoError(t, ibs2.SetCode(addr, codeY, tracing.CodeChangeUnspecified))
	require.Len(t, ch2, 1, "replacing the code emits exactly one change")
	require.Equal(t, codeX, ch2[0].prev, "the previous code must be the earlier tx's bytes, not empty")
	require.Equal(t, codeY, ch2[0].next)
}

// SetStorage (a debug-only full-storage replace) records the account's current
// incarnation as read through the version map, not a transient object's field.
// Pins the stateObject-free behaviour: for a freshly created contract the recorded
// incarnation equals GetIncarnation (the materialized path recorded the object's 0).
func TestNoMaterialize_SetStorageRecordsCurrentIncarnation(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc0, 0xee})
	ibs := NewWithVersionMap(&emptyReader{}, NewVersionMap(nil))
	ibs.SetTxContext(1, 0)
	require.NoError(t, ibs.CreateAccount(addr, true))

	require.NoError(t, ibs.SetStorage(addr, Storage{accounts.InternKey([32]byte{0x01}): *uint256.NewInt(7)}))

	inc, err := ibs.GetIncarnation(addr)
	require.NoError(t, err)
	wInc, ok := ibs.VersionedWrites().GetIncarnation(addr)
	require.True(t, ok, "SetStorage records an incarnation write")
	require.Equal(t, inc, wInc.Val, "SetStorage records the account's current incarnation")
}

func newNoMaterializeTx(vm *VersionMap, txIndex int) *IntraBlockState {
	ibs := NewWithVersionMap(&emptyReader{}, vm)
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

// Reverting a credit made after a burning SELFDESTRUCT must restore the burn's
// deletion stamp, not just the value; before EIP-161 nothing else removes the
// destroyed account.
func TestNoMaterialize_RevertedCreditAfterBurnKeepsBalanceDeleted(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc0, 0x88})
	code := []byte("destroyed-contract-code")
	committed := accounts.NewAccount()
	committed.Nonce = 1
	committed.Balance = *uint256.NewInt(1000)
	committed.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	ibs := NewWithVersionMap(&fieldReader{addr: addr, account: &committed, code: code}, NewVersionMap(nil))
	ibs.SetTxContext(1, 0)

	_, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	snap := ibs.PushSnapshot()
	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(5), tracing.BalanceChangeTransfer))
	ibs.RevertToSnapshot(snap, nil)

	writes := finalizedWrites(t, ibs, &chain.Rules{IsHomestead: true})
	bal, ok := writes.GetBalance(addr)
	require.True(t, ok)
	require.True(t, bal.Deletes(), "the burned account must stay deleted after the revert")
}

// A second SELFDESTRUCT over a burned-then-credited account journals a balance cell
// that already carries the burn's deletion stamp; reverting it must keep the stamp.
func TestNoMaterialize_RevertedRepeatDestructAfterBurnKeepsBalanceDeleted(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc0, 0x99})
	code := []byte("destroyed-contract-code")
	committed := accounts.NewAccount()
	committed.Nonce = 1
	committed.Balance = *uint256.NewInt(1000)
	committed.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	ibs := NewWithVersionMap(&fieldReader{addr: addr, account: &committed, code: code}, NewVersionMap(nil))
	ibs.SetTxContext(1, 0)

	_, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(5), tracing.BalanceChangeTransfer))
	snap := ibs.PushSnapshot()
	_, err = ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	ibs.RevertToSnapshot(snap, nil)

	writes := finalizedWrites(t, ibs, &chain.Rules{IsHomestead: true})
	bal, ok := writes.GetBalance(addr)
	require.True(t, ok)
	require.True(t, bal.Deletes(), "the burned account must stay deleted after the revert")
}

// Reverting the only SELFDESTRUCT restores the credited balance as a live value.
func TestNoMaterialize_RevertedDestructRestoresLiveBalance(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc0, 0xaa})
	code := []byte("reverted-destruct-code")
	committed := accounts.NewAccount()
	committed.Nonce = 1
	committed.Balance = *uint256.NewInt(1000)
	committed.CodeHash = accounts.InternCodeHash(crypto.Keccak256Hash(code))
	ibs := NewWithVersionMap(&fieldReader{addr: addr, account: &committed, code: code}, NewVersionMap(nil))
	ibs.SetTxContext(1, 0)

	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(5), tracing.BalanceChangeTransfer))
	snap := ibs.PushSnapshot()
	_, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	ibs.RevertToSnapshot(snap, nil)

	writes := finalizedWrites(t, ibs, &chain.Rules{IsHomestead: true})
	bal, ok := writes.GetBalance(addr)
	require.True(t, ok)
	require.False(t, bal.Deletes(), "a reverted destruct must not leave the deletion stamp")
	require.Equal(t, *uint256.NewInt(1005), bal.Val)
}

// A credit that revives a destroyed account starts it at nonce 0, not at the destroyed
// incarnation's nonce left in the version-map floor.
func TestNoMaterialize_RevivedAccountDropsDestroyedNonce(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc0, 0xbb})
	vm := NewVersionMap(nil)
	ibs := newNoMaterializeTx(vm, 0)
	require.NoError(t, ibs.CreateAccount(addr, true))
	require.NoError(t, ibs.SetNonce(addr, 8, tracing.NonceChangeUnspecified))
	vm.FlushVersionedWrites(ibs.VersionedWrites(), true)
	ibs = newNoMaterializeTx(vm, 1)
	_, err := ibs.Selfdestruct(addr, false)
	require.NoError(t, err)
	vm.FlushVersionedWrites(ibs.VersionedWrites(), true)
	ibs = newNoMaterializeTx(vm, 2)
	require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(5), tracing.BalanceChangeTransfer))
	vm.FlushVersionedWrites(ibs.VersionedWrites(), true)

	acc, ok, _ := vm.View(3, &emptyReader{}).GetAccount(addr)
	require.True(t, ok)
	require.Zero(t, acc.Nonce)
	require.Equal(t, *uint256.NewInt(5), acc.Balance)
}

// A reverted CREATE of an absent address records its base balance and incarnation reads with UnknownVersion. While
// no tx wrote a cell of the address they are not validated; a write below the reader still invalidates them.
func TestRevertedCreateOfAbsentAddressSkipsQuietBaseReads(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xc0, 0x44})
	reads := func() ReadSet {
		ibs := NewWithVersionMap(&emptyReader{}, NewVersionMap(nil))
		ibs.SetTxContext(0, 3)
		ibs.SetVersion(0)
		snap := ibs.PushSnapshot()
		require.NoError(t, ibs.CreateAccount(addr, true))
		ibs.RevertToSnapshot(snap, nil)
		return ibs.VersionedReads()
	}

	checks := 0
	countChecks := func(read, write Version) VersionValidity {
		checks++
		return validateEqualVersion(read, write)
	}
	require.Equal(t, VersionValid, NewVersionMap(nil).ValidateReadSet(3, reads(), countChecks, false, ""))
	require.Zero(t, checks, "the base reads of a quiet address are not validated")

	vm := NewVersionMap(nil)
	writeFor(vm, addr, BalancePath, accounts.NilKey, Version{TxIndex: 1}, *uint256.NewInt(5), true)
	require.Equal(t, VersionInvalid, vm.ValidateReadSet(3, reads(), validateEqualVersion, false, ""))
}
