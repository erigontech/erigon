package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

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
	require.NoError(t, ibs.FinalizeTx(rules, NewNoopWriter()))
	writes := finalizedWrites(t, ibs, rules)
	vm.FlushVersionedWrites(writes, false)
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
	read, ok := ibs.VersionedReads().storage[x][k]
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
			x := accounts.InternAddress([20]byte{0x51})
			k5 := accounts.InternKey([32]byte{31: 5})
			acc := accounts.NewAccount()
			acc.Balance = *uint256.NewInt(1)
			reader := &storageReader{addr: x, account: &acc, storage: map[accounts.StorageKey]uint256.Int{k5: *uint256.NewInt(3)}}
			ibs := tc.new(reader)
			defer ibs.Close()
			ibs.SetTxContext(1, 0)
			require.NoError(t, ibs.CreateAccount(x, true))
			snap := ibs.PushSnapshot()
			_, err := ibs.Selfdestruct(x, false)
			require.NoError(t, err)
			_, err = ibs.GetState(x, k5)
			require.NoError(t, err)
			ibs.RevertToSnapshot(snap, nil)
			current, err := ibs.GetState(x, k5)
			require.NoError(t, err)
			require.True(t, current.IsZero())
			original, err := ibs.GetCommittedState(x, k5)
			require.NoError(t, err)
			require.True(t, original.IsZero())
		})
	}
}

// A reverted SELFDESTRUCT keeps the origin values that an earlier tx committed for the contract.
func TestRevertedSelfdestructKeepsEarlierTxOrigin(t *testing.T) {
	t.Parallel()
	x := accounts.InternAddress([20]byte{0x51})
	k5 := accounts.InternKey([32]byte{31: 5})
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(1)
	reader := &storageReader{addr: x, account: &acc, storage: map[accounts.StorageKey]uint256.Int{k5: *uint256.NewInt(3)}}
	rules := &chain.Rules{}
	ibs := New(reader)
	defer ibs.Close()
	ibs.SetTxContext(1, 0)
	require.NoError(t, ibs.CreateAccount(x, true))
	require.NoError(t, ibs.SetState(x, k5, *uint256.NewInt(7)))
	require.NoError(t, ibs.FinalizeTx(rules, NewNoopWriter()))

	ibs.SetTxContext(1, 1)
	snap := ibs.PushSnapshot()
	_, err := ibs.Selfdestruct(x, false)
	require.NoError(t, err)
	ibs.RevertToSnapshot(snap, nil)
	original, err := ibs.GetCommittedState(x, k5)
	require.NoError(t, err)
	require.Equal(t, *uint256.NewInt(7), original)
}

// A slot read while the created contract is destructed must not become the block-start value
// that decides whether CommitBlock writes the slot.
func TestRevertedSelfdestructCommitsSlotEqualToPreCreationValue(t *testing.T) {
	t.Parallel()
	x := accounts.InternAddress([20]byte{0x51})
	k5 := accounts.InternKey([32]byte{31: 5})
	acc := accounts.NewAccount()
	acc.Balance = *uint256.NewInt(1)
	reader := &storageReader{addr: x, account: &acc, storage: map[accounts.StorageKey]uint256.Int{k5: *uint256.NewInt(3)}}
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
	require.NoError(t, ibs.FinalizeTx(rules, NewNoopWriter()))
	w := newFinalStateWriter()
	require.NoError(t, ibs.CommitBlock(rules, w))
	require.Equal(t, *uint256.NewInt(3), w.storage[x][k5])
}
