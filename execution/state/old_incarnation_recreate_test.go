package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

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
		vm.FlushVersionedWrites(ibs.FinalizedWrites(), true, "")
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
