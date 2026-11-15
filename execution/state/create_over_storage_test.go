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
// slot 5 before the block, and flushes its writes: Done when complete, in flight otherwise.
func createdOverStorage(t *testing.T, complete bool) (accounts.Address, accounts.StorageKey, *storageReader, *VersionMap) {
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
	vm.FlushVersionedWrites(finalizedWrites(t, ibs, rules), complete)
	return x, k5, reader, vm
}

// A contract creation replaces the account's storage without writing the old slots, so a later
// tx reads an old slot as zero, also when its pre-block value is published as the slot's
// origin. The read validates until a tx between the creation and the read writes the slot.
func TestOldSlotReadsZeroAfterACreation(t *testing.T) {
	t.Parallel()
	x, k5, reader, vm := createdOverStorage(t, true)
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
	require.Equal(t, VersionValid, validateReads(vm, ibs, 2))
	vm.WriteStorage(x, k5, Version{TxIndex: 1}, *uint256.NewInt(4), true)
	require.Equal(t, VersionInvalid, validateReads(vm, ibs, 2))
}

// While the creation is in flight, a later tx's read of an old slot depends on it and does not
// publish the pre-block value as the slot's origin.
func TestOldSlotReadDependsOnAnInFlightCreation(t *testing.T) {
	t.Parallel()
	x, k5, reader, vm := createdOverStorage(t, false)

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
