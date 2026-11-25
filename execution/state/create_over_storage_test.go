package state

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// createdOverStorage runs tx 0, in its second run, as a contract creation over x, which holds
// slot 5 before the block, and flushes its writes in flight.
func createdOverStorage(t *testing.T) (accounts.Address, accounts.StorageKey, *storageReader, *VersionMap, *WriteSet) {
	t.Helper()
	x := accounts.InternAddress([20]byte{0x51})
	k2, k5 := accounts.InternKey([32]byte{31: 2}), accounts.InternKey([32]byte{31: 5})
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(1)
	acc.Incarnation = FirstContractIncarnation
	reader := &storageReader{addr: x, account: &acc, storage: map[accounts.StorageKey]uint256.Int{k5: *uint256.NewInt(3)}}
	rules := &chain.Rules{IsSpuriousDragon: true}
	vm := NewVersionMap(nil)
	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Close()
	ibs.SetTxContext(1, 0)
	ibs.SetVersion(1)
	require.NoError(t, ibs.CreateAccount(x, true))
	require.NoError(t, ibs.SetNonce(x, 1, tracing.NonceChangeUnspecified))
	require.NoError(t, ibs.SetState(x, k2, *uint256.NewInt(1)))
	ibs.EndTransaction()
	writes := finalizedWrites(t, ibs, rules)
	vm.FlushWrites(writes, false)
	return x, k5, reader, vm, writes
}

// A contract creation replaces the account's storage without writing the old slots, so a later
// tx reads an old slot as zero, also when its pre-block value is published as the slot's
// origin. Once the creation is validated or done, the read does not wait for it. The read
// validates until a tx between the creation and the read writes the slot.
func TestOldSlotReadsZeroAfterACreation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		settle func(*VersionMap, *WriteSet)
	}{
		{"validated", func(vm *VersionMap, writes *WriteSet) { vm.MarkWritesValidated(writes, nil) }},
		{"done", (*VersionMap).MarkWritesComplete},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			x, k5, reader, vm, writes := createdOverStorage(t)
			tc.settle(vm, writes)
			vm.StoreOriginStorage(x, k5, *uint256.NewInt(3))

			ibs := NewWithVersionMap(reader, vm)
			defer ibs.Close()
			ibs.SetTxContext(1, 2)
			current, err := ibs.GetState(x, k5)
			require.NoError(t, err)
			require.True(t, current.IsZero())
			original, err := ibs.GetCommittedState(x, k5)
			require.NoError(t, err)
			require.True(t, original.IsZero())
			require.False(t, ibs.HadInvalidRead(), "tx 2 waits for tx %d", ibs.DepTxIndex())
			require.Equal(t, VersionValid, validateReads(vm, ibs, 2))
			vm.WriteStorage(x, k5, Version{TxIndex: 1}, *uint256.NewInt(4), true)
			require.Equal(t, VersionInvalid, validateReads(vm, ibs, 2))
		})
	}
}

// While the creation is in flight, a later tx's read of an old slot depends on it and does not
// publish the pre-block value as the slot's origin.
func TestOldSlotReadDependsOnAnInFlightCreation(t *testing.T) {
	t.Parallel()
	x, k5, reader, vm, _ := createdOverStorage(t)

	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Close()
	ibs.SetTxContext(1, 1)
	original, err := ibs.GetCommittedState(x, k5)
	require.NoError(t, err)
	require.Equal(t, 0, ibs.DepTxIndex())
	require.True(t, original.IsZero())
	ibs.PublishOrigins()
	_, published := vm.LoadOriginStorage(x, k5)
	require.False(t, published)
}

// The creating tx reads an old slot as zero, also when a later tx has already published the
// slot's pre-block value as its origin.
func TestCreatingTxReadsOldSlotAsZero(t *testing.T) {
	t.Parallel()
	x := accounts.InternAddress([20]byte{0x51})
	k5 := accounts.InternKey([32]byte{31: 5})
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(1)
	acc.Incarnation = FirstContractIncarnation
	reader := &storageReader{addr: x, account: &acc, storage: map[accounts.StorageKey]uint256.Int{k5: *uint256.NewInt(3)}}
	vm := NewVersionMap(nil)
	vm.StoreOriginStorage(x, k5, *uint256.NewInt(3))

	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Close()
	ibs.SetTxContext(1, 0)
	require.NoError(t, ibs.CreateAccount(x, true))
	current, err := ibs.GetState(x, k5)
	require.NoError(t, err)
	require.True(t, current.IsZero())
	original, err := ibs.GetCommittedState(x, k5)
	require.NoError(t, err)
	require.True(t, original.IsZero())
	require.Equal(t, VersionValid, validateReads(vm, ibs, 0))
}

// The creating tx reads an old slot as zero, which is not the slot's pre-block value, so the
// read publishes no origin, also when the create frame is reverted. A read before the create
// publishes the pre-block value. An earlier tx reads the pre-block value, and the creating tx's
// reads stay valid once that tx publishes the origin.
func TestCreatingTxPublishesNoOriginForAnOldSlot(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                string
		readFirst, reverted bool
	}{
		{"the create stays", false, false},
		{"the create frame is reverted", false, true},
		{"the slot is read before the create", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			x := accounts.InternAddress([20]byte{0x51})
			k5 := accounts.InternKey([32]byte{31: 5})
			preBlock := *uint256.NewInt(3)
			acc := accounts.NewAccount()
			acc.Balance = *uint256.NewInt(1)
			acc.Incarnation = FirstContractIncarnation
			reader := &storageReader{addr: x, account: &acc, storage: map[accounts.StorageKey]uint256.Int{k5: preBlock}}
			vm := NewVersionMap(nil)

			creating := NewWithVersionMap(reader, vm)
			defer creating.Close()
			creating.SetTxContext(1, 1)
			if tc.readFirst {
				v, err := creating.GetState(x, k5)
				require.NoError(t, err)
				require.Equal(t, preBlock, v)
			}
			snapshot := creating.PushSnapshot()
			require.NoError(t, creating.CreateAccount(x, true))
			current, err := creating.GetState(x, k5)
			require.NoError(t, err)
			original, err := creating.GetCommittedState(x, k5)
			require.NoError(t, err)
			if !tc.readFirst {
				require.True(t, current.IsZero())
				require.True(t, original.IsZero())
			}
			if tc.reverted {
				creating.RevertToSnapshot(snapshot, nil)
			}
			vm.FlushWrites(finalizedWrites(t, creating, &chain.Rules{IsSpuriousDragon: true}), true)
			origin, published := vm.LoadOriginStorage(x, k5)
			require.Equal(t, tc.readFirst, published)
			if published {
				require.Equal(t, preBlock, origin)
			}
			require.Equal(t, VersionValid, validateReads(vm, creating, 1))

			earlier := NewWithVersionMap(reader, vm)
			defer earlier.Close()
			earlier.SetTxContext(1, 0)
			v, err := earlier.GetState(x, k5)
			require.NoError(t, err)
			require.Equal(t, preBlock, v)
			earlier.PublishOrigins()
			require.Equal(t, VersionValid, validateReads(vm, creating, 1))
		})
	}
}

// Pre-Cancun, tx 1 destroys a contract with a committed slot, tx 2 credits the address, and
// tx 1 runs again while tx 2 is not validated. An attempt of tx 3 then creates a contract at the
// address and stores to the slot; it depends on tx 2, so the scheduler discards it. Tx 1 still
// reads the pre-block value of the slot, and so does tx 4 once the final runs leave the contract
// alone: tx 1 does not destroy it and tx 2 only credits it.
func TestDiscardedCreateKeepsTheSlotOrigin(t *testing.T) {
	t.Parallel()
	for _, keptAlive := range []bool{false, true} {
		t.Run(fmt.Sprintf("the final runs leave the contract alone=%v", keptAlive), func(t *testing.T) {
			t.Parallel()
			rules := &chain.Rules{IsSpuriousDragon: true, IsByzantium: true, IsBerlin: true, IsLondon: true, IsShanghai: true}
			x := accounts.InternAddress([20]byte{0xc0, 0x01})
			beneficiary := accounts.InternAddress([20]byte{0xbe})
			slot := accounts.InternKey([32]byte{0x11})
			preBlock := *uint256.NewInt(22)
			acc := accounts.NewAccount()
			acc.Nonce = 1
			acc.Incarnation = FirstContractIncarnation
			acc.Balance = *uint256.NewInt(7)
			acc.CodeHash = accounts.InternCodeHash(common.Hash{0xc0})
			reader := &storageReader{addr: x, account: &acc, storage: map[accounts.StorageKey]uint256.Int{slot: preBlock}}
			vm := NewVersionMap(nil)
			newTx := func(txIndex int, inc Incarnation) *IntraBlockState {
				ibs := NewWithVersionMap(reader, vm)
				t.Cleanup(ibs.Close)
				ibs.SetTxContext(1, txIndex)
				ibs.SetVersion(inc)
				return ibs
			}
			rerun := func(prev, cur *WriteSet, v Version) {
				for h := range prev.AllHeaders() {
					if !cur.Has(h) {
						vm.DropWrite(h.Address, h.Path, h.Key, v)
					}
				}
				vm.FlushWrites(cur, false)
			}
			destroy := func(inc Incarnation) *WriteSet {
				ibs := newTx(1, inc)
				balance, err := ibs.GetBalance(x)
				require.NoError(t, err)
				require.NoError(t, ibs.AddBalance(beneficiary, balance, tracing.BalanceIncreaseSelfdestruct))
				_, err = ibs.Selfdestruct(x, false)
				require.NoError(t, err)
				return finalizedWrites(t, ibs, rules)
			}

			destroyed := destroy(0)
			vm.FlushWrites(destroyed, false)
			vm.MarkWritesValidated(destroyed, nil)

			credit := newTx(2, 0)
			exists, err := credit.Exist(x)
			require.NoError(t, err)
			require.False(t, exists)
			require.NoError(t, credit.CreateAccount(x, false))
			require.NoError(t, credit.AddBalance(x, *uint256.NewInt(20), tracing.BalanceChangeTransfer))
			credited := finalizedWrites(t, credit, rules)
			vm.FlushWrites(credited, false)

			destroyedAgain := destroy(1)
			rerun(destroyed, destroyedAgain, Version{TxIndex: 1, Incarnation: 1})

			create := newTx(3, 0)
			require.NoError(t, create.CreateAccount(x, true))
			require.NoError(t, create.SetNonce(x, 1, tracing.NonceChangeNewContract))
			require.NoError(t, create.SetState(x, slot, *uint256.NewInt(5)))
			finalizedWrites(t, create, rules)
			require.Equal(t, 2, create.DepTxIndex())

			readerTx := 1
			if keptAlive {
				rerun(destroyedAgain, &WriteSet{}, Version{TxIndex: 1, Incarnation: 2})
				onlyCredit := newTx(2, 1)
				require.NoError(t, onlyCredit.AddBalance(x, *uint256.NewInt(20), tracing.BalanceChangeTransfer))
				credits := finalizedWrites(t, onlyCredit, rules)
				rerun(credited, credits, Version{TxIndex: 2, Incarnation: 1})
				vm.MarkWritesValidated(credits, nil)
				readerTx = 4
			}
			v, err := newTx(readerTx, 0).GetState(x, slot)
			require.NoError(t, err)
			require.Equal(t, preBlock, v)
		})
	}
}

// A slot written between a destruct and a creation reads zero, as the read path, the
// validator and the versioned reader all see the creation.
func TestSlotWrittenBetweenADestructAndACreationReadsZero(t *testing.T) {
	t.Parallel()
	x := accounts.InternAddress([20]byte{0x61})
	k := accounts.InternKey([32]byte{31: 1})
	vm := NewVersionMap(nil)
	vm.WriteSelfDestruct(x, Version{TxIndex: 0}, true, true)
	vm.WriteBalance(x, Version{TxIndex: 1}, *uint256.NewInt(1), true)
	vm.WriteStorage(x, k, Version{TxIndex: 1}, *uint256.NewInt(7), true)
	vm.WriteCreateContract(x, Version{TxIndex: 2}, true, true)

	ibs := NewWithVersionMap(&emptyReader{}, vm)
	defer ibs.Close()
	ibs.SetTxContext(1, 3)
	v, err := ibs.GetState(x, k)
	require.NoError(t, err)
	require.True(t, v.IsZero())
	require.Equal(t, VersionValid, validateReads(vm, ibs, 3))

	v, found, err := NewVersionedStateReader(3, ReadSet{}, vm, &emptyReader{}).ReadAccountStorage(x, k)
	require.NoError(t, err)
	require.True(t, v.IsZero())
	require.False(t, found)
}

// A read of a slot's pre-block value, made before a lower tx's creation clears the slot, is
// stale once the creation is settled, unless it read zero: the slot is zero before and after the
// creation.
func TestBaseReadOfZeroStaysValidOnceACreationClearsTheSlot(t *testing.T) {
	t.Parallel()
	x, k1, k5, reader := slotsBeforeTheBlock()
	vm := NewVersionMap(nil)
	zeroRead, oldRead := slotRead(t, reader, vm, x, k1, 1), slotRead(t, reader, vm, x, k5, 1)

	vm.WriteCreateContract(x, Version{TxIndex: 0, Incarnation: 1}, true, true)
	require.Equal(t, VersionValid, vm.ValidateReadSet(1, zeroRead, validateEqualVersion, false, ""))
	require.Equal(t, VersionInvalid, vm.ValidateReadSet(1, oldRead, validateEqualVersion, false, ""))
}

// A read of zero made before a creation does not validate while the creation is in flight.
func TestBaseReadOfZeroWaitsForAnInFlightCreation(t *testing.T) {
	t.Parallel()
	x, k1, _, reader := slotsBeforeTheBlock()
	vm := NewVersionMap(nil)
	zeroRead := slotRead(t, reader, vm, x, k1, 1)

	vm.WriteCreateContract(x, Version{TxIndex: 0, Incarnation: 1}, true, false)
	require.NotEqual(t, VersionValid, vm.ValidateReadSet(1, zeroRead, validateEqualVersion, false, ""))
}

// A read of zero made before a destruct is stale once the destruct is settled: a write above the
// destruct can still be in flight, and the validator anchors the slot on the destruct.
func TestBaseReadOfZeroIsStaleAfterADestruct(t *testing.T) {
	t.Parallel()
	x, k1, _, reader := slotsBeforeTheBlock()
	vm := NewVersionMap(nil)
	zeroRead := slotRead(t, reader, vm, x, k1, 2)

	vm.WriteSelfDestruct(x, Version{TxIndex: 0, Incarnation: 1}, true, true)
	vm.WriteStorage(x, k1, Version{TxIndex: 1, Incarnation: 1}, *uint256.NewInt(5), false)
	require.Equal(t, VersionInvalid, vm.ValidateReadSet(2, zeroRead, validateEqualVersion, false, ""))
}

// slotsBeforeTheBlock returns a contract x whose slot k5 is 3 before the block and whose slot k1
// is zero.
func slotsBeforeTheBlock() (x accounts.Address, k1, k5 accounts.StorageKey, reader *storageReader) {
	x = accounts.InternAddress([20]byte{0x51})
	k1, k5 = accounts.InternKey([32]byte{31: 1}), accounts.InternKey([32]byte{31: 5})
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(1)
	acc.Incarnation = FirstContractIncarnation
	return x, k1, k5, &storageReader{addr: x, account: &acc, storage: map[accounts.StorageKey]uint256.Int{k5: *uint256.NewInt(3)}}
}

// slotRead reads slot k of x in tx txIdx and returns a read set with only that read.
func slotRead(t *testing.T, reader StateReader, vm *VersionMap, x accounts.Address, k accounts.StorageKey, txIdx int) ReadSet {
	t.Helper()
	ibs := NewWithVersionMap(reader, vm)
	defer ibs.Close()
	ibs.SetTxContext(1, txIdx)
	_, err := ibs.GetState(x, k)
	require.NoError(t, err)
	read, ok := ibs.Reads().storage[x][k]
	require.True(t, ok)
	return ReadSet{storage: map[accounts.Address]map[accounts.StorageKey]VersionedRead[uint256.Int]{x: {k: read}}}
}

// A reverted SELFDESTRUCT leaves the contract created in this tx, so a slot from the
// account's storage before the creation still reads zero.
func TestRevertedSelfdestructKeepsCreatedContract(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		new  func(StateReader) *IntraBlockState
	}{
		{"serial", New},
		{"versioned", func(r StateReader) *IntraBlockState { return NewWithVersionMap(r, NewVersionMap(nil)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			x, _, k5, reader := slotsBeforeTheBlock()
			ibs := tc.new(reader)
			defer ibs.Close()
			ibs.SetTxContext(1, 0)
			beforeCreate := ibs.PushSnapshot()
			require.NoError(t, ibs.CreateAccount(x, true))
			snap := ibs.PushSnapshot()
			_, err := ibs.Selfdestruct(x, false)
			require.NoError(t, err)
			require.True(t, ibs.ContractCreatedInTx(x))
			_, err = ibs.GetState(x, k5)
			require.NoError(t, err)
			ibs.RevertToSnapshot(snap, nil)
			current, err := ibs.GetState(x, k5)
			require.NoError(t, err)
			require.True(t, current.IsZero())
			original, err := ibs.GetCommittedState(x, k5)
			require.NoError(t, err)
			require.True(t, original.IsZero())
			ibs.RevertToSnapshot(beforeCreate, nil)
			require.False(t, ibs.ContractCreatedInTx(x))
		})
	}
}

// A reverted SELFDESTRUCT keeps the origin values that an earlier tx committed for the contract.
func TestRevertedSelfdestructKeepsEarlierTxOrigin(t *testing.T) {
	t.Parallel()
	x, _, k5, reader := slotsBeforeTheBlock()
	rules := &chain.Rules{}
	ibs := New(reader)
	defer ibs.Close()
	ibs.SetTxContext(1, 0)
	require.NoError(t, ibs.CreateAccount(x, true))
	require.NoError(t, ibs.SetState(x, k5, *uint256.NewInt(7)))
	writes, err := ibs.FinalizedWrites(rules)
	require.NoError(t, err)
	ibs.FlushWritesToVersionMap(writes)
	ibs.ResetIO()

	ibs.SetTxContext(1, 1)
	snap := ibs.PushSnapshot()
	_, err = ibs.Selfdestruct(x, false)
	require.NoError(t, err)
	require.False(t, ibs.ContractCreatedInTx(x))
	ibs.RevertToSnapshot(snap, nil)
	original, err := ibs.GetCommittedState(x, k5)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(7), original)
}

// A slot read while the created contract is destructed must not become the block-start value
// that decides whether CommitBlock writes the slot.
func TestRevertedSelfdestructCommitsSlotEqualToPreCreationValue(t *testing.T) {
	t.Parallel()
	x, _, k5, reader := slotsBeforeTheBlock()
	rules := &chain.Rules{}
	ibs := New(reader)
	defer ibs.Close()
	ibs.SetTxContext(1, 0)
	require.NoError(t, ibs.CreateAccount(x, true))
	snap := ibs.PushSnapshot()
	_, err := ibs.Selfdestruct(x, false)
	require.NoError(t, err)
	_, err = ibs.GetState(x, k5)
	require.NoError(t, err)
	ibs.RevertToSnapshot(snap, nil)
	require.NoError(t, ibs.SetState(x, k5, *uint256.NewInt(3)))
	writes, err := ibs.FinalizedWrites(rules)
	require.NoError(t, err)
	vw, ok := writes.GetStorage(x, k5)
	require.True(t, ok)
	require.Equal(t, *uint256.NewInt(3), vw.Val)
}

// A slot written while the created contract is destructed journals zero as its previous value,
// so the revert does not bring back the value from before the creation.
func TestRevertedSelfdestructUndoesWriteToZero(t *testing.T) {
	t.Parallel()
	x, _, k5, reader := slotsBeforeTheBlock()
	rules := &chain.Rules{}
	ibs := New(reader)
	defer ibs.Close()
	ibs.SetTxContext(1, 0)
	require.NoError(t, ibs.CreateAccount(x, true))
	snap := ibs.PushSnapshot()
	_, err := ibs.Selfdestruct(x, false)
	require.NoError(t, err)
	require.NoError(t, ibs.SetState(x, k5, *uint256.NewInt(9)))
	ibs.RevertToSnapshot(snap, nil)
	current, err := ibs.GetState(x, k5)
	require.NoError(t, err)
	require.True(t, current.IsZero())
	ibs.EndTransaction()
	writes, err := ibs.FinalizedWrites(rules)
	require.NoError(t, err)
	_, ok := writes.GetStorage(x, k5)
	require.False(t, ok)
}
