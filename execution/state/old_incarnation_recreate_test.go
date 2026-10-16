package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// tx0 writes slot k=4; tx1 self-destructs (pre-Cancun full destruct);
// tx2 recreates the address without touching k; tx3 reads k and must see 0.
func TestOldIncarnationStorageMaskedAfterRecreate(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xDE, 0xAD})
	key := accounts.InternKey([32]byte{0x01})
	acc := accounts.NewAccount()
	acc.Nonce = 1
	acc.Incarnation = 1
	acc.Balance = *uint256.NewInt(1000)

	reader := &storageReader{
		addr:    addr,
		account: &acc,
		storage: map[accounts.StorageKey]uint256.Int{},
	}
	vm := NewVersionMap(nil)

	runTx := func(txIdx int, f func(ibs *IntraBlockState)) {
		ibs := NewWithVersionMap(reader, vm)
		ibs.SetTxContext(100, txIdx)
		ibs.SetVersion(0)
		ibs.SetNoMaterialize(true)
		f(ibs)
		vm.FlushVersionedWrites(ibs.FinalizedWrites(&chain.Rules{}), true)
	}

	runTx(0, func(ibs *IntraBlockState) {
		require.NoError(t, ibs.SetState(addr, key, *uint256.NewInt(4)))
	})
	runTx(1, func(ibs *IntraBlockState) {
		ok, err := ibs.Selfdestruct(addr, false)
		require.NoError(t, err)
		require.True(t, ok)
	})
	runTx(2, func(ibs *IntraBlockState) {
		require.NoError(t, ibs.CreateAccount(addr, true))
	})

	ibs3 := NewWithVersionMap(reader, vm)
	ibs3.SetTxContext(100, 3)
	ibs3.SetVersion(0)
	ibs3.SetNoMaterialize(true)
	v, err := ibs3.GetState(addr, key)
	require.NoError(t, err)
	require.True(t, v.IsZero(), "recreated contract's unwritten slot must read 0, got %s", v.String())
}

// runRevivalTx runs f as tx txIdx on a noMaterialize IBS over vm and flushes its
// finalized writes, which it returns.
func runRevivalTx(t *testing.T, vm *VersionMap, txIdx int, f func(ibs *IntraBlockState)) *WriteSet {
	t.Helper()
	ibs := NewWithVersionMap(&emptyReader{}, vm)
	ibs.SetTxContext(100, txIdx)
	ibs.SetNoMaterialize(true)
	f(ibs)
	writes := ibs.FinalizedWrites(&chain.Rules{})
	vm.FlushVersionedWrites(writes, true)
	return writes
}

func createAndDestroy(t *testing.T, addr accounts.Address) func(ibs *IntraBlockState) {
	return func(ibs *IntraBlockState) {
		require.NoError(t, ibs.CreateAccount(addr, true))
		_, err := ibs.Selfdestruct(addr, false)
		require.NoError(t, err)
	}
}

func credit(t *testing.T, addr accounts.Address) func(ibs *IntraBlockState) {
	return func(ibs *IntraBlockState) {
		require.NoError(t, ibs.AddBalance(addr, *uint256.NewInt(5), tracing.BalanceChangeTransfer))
	}
}

// tx0 creates and self-destructs the address; tx1 credits it, which revives it
// as a fresh account committed with incarnation 0.
func TestCreditRevivalCommitsIncarnationZero(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xDE, 0xAD})
	vm := NewVersionMap(nil)
	runRevivalTx(t, vm, 0, createAndDestroy(t, addr))
	writes := runRevivalTx(t, vm, 1, credit(t, addr))

	normalized, err := writes.Normalize(vm, 1, 0, &emptyReader{}, nil, true, false, false)
	require.NoError(t, err)
	inc, ok := normalized.GetIncarnation(addr)
	require.True(t, ok)
	require.Zero(t, inc.Val)
}

// tx0 creates and self-destructs the address; tx1 credits it, which revives it
// as a fresh account; tx2 recreates it, carrying the credit, as incarnation 1.
func TestRecreateAfterCreditRevivalCarriesBalance(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xDE, 0xAD})
	vm := NewVersionMap(nil)
	runRevivalTx(t, vm, 0, createAndDestroy(t, addr))
	runRevivalTx(t, vm, 1, credit(t, addr))
	runRevivalTx(t, vm, 2, func(ibs *IntraBlockState) {
		require.NoError(t, ibs.CreateAccount(addr, true))
		balance, err := ibs.GetBalance(addr)
		require.NoError(t, err)
		require.Equal(t, uint64(5), balance.Uint64())
		incarnation, err := ibs.GetIncarnation(addr)
		require.NoError(t, err)
		require.Equal(t, uint64(1), incarnation)
	})
}

// tx0 creates and self-destructs the address; tx1 recreates it as incarnation 1,
// as serial execution does.
func TestRecreateAfterDestructInEarlierTx(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xDE, 0xAD})
	vm := NewVersionMap(nil)
	runRevivalTx(t, vm, 0, createAndDestroy(t, addr))
	runRevivalTx(t, vm, 1, func(ibs *IntraBlockState) {
		require.NoError(t, ibs.CreateAccount(addr, true))
		incarnation, err := ibs.GetIncarnation(addr)
		require.NoError(t, err)
		require.Equal(t, uint64(1), incarnation)
	})
}
