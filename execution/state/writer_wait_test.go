package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestReadOfInFlightWriteWaitsForWriter(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xC0})
	key := accounts.InternKey([32]byte{1})
	acc := accounts.NewAccount()
	acc.Incarnation = 1
	writer := Version{TxIndex: 0, Incarnation: 1}

	vm := NewVersionMap(nil)
	vm.WriteStorage(addr, key, writer, *uint256.NewInt(7), false)
	var waits [][2]int
	wait := func(reader, w int) bool {
		waits = append(waits, [2]int{reader, w})
		vm.WriteStorage(addr, key, Version{TxIndex: 0, Incarnation: 2}, *uint256.NewInt(8), true)
		return true
	}

	ibs := NewWithVersionMap(&codeReader{addr: addr, account: &acc}, vm)
	ibs.SetWriterWait(wait)
	ibs.SetTxContext(1, 1)
	ibs.SetVersion(0)
	got, err := ibs.GetState(addr, key)
	require.NoError(t, err)

	require.Equal(t, [][2]int{{1, 0}}, waits, "the reader did not wait for its writer")
	require.Equal(t, uint64(8), got.Uint64(), "the reader did not see the value of the writer's final run")
	require.False(t, ibs.HadInvalidRead(), "a read that waited for its writer must not void the run")
}

func TestReadWaitsOnceWhenWriterStaysInFlight(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0xC0})
	key := accounts.InternKey([32]byte{1})
	acc := accounts.NewAccount()
	acc.Incarnation = 1

	vm := NewVersionMap(nil)
	vm.WriteStorage(addr, key, Version{TxIndex: 0, Incarnation: 1}, *uint256.NewInt(7), false)
	waits := 0
	wait := func(int, int) bool {
		waits++
		return waits < 100
	}

	ibs := NewWithVersionMap(&codeReader{addr: addr, account: &acc}, vm)
	ibs.SetWriterWait(wait)
	ibs.SetTxContext(1, 1)
	ibs.SetVersion(0)
	_, err := ibs.GetState(addr, key)
	require.NoError(t, err)

	require.Equal(t, 1, waits, "a read waits for its writer once")
	require.Equal(t, 0, ibs.DepTxIndex(), "a writer still in flight after the wait voids the run")
}
