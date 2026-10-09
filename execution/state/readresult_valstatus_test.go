package state

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestReadResultCarriesValStatus pins that a storage read surfaces the floor
// cell's write transition on the ReadResult, so validation can treat a no-op
// (ValueUnchanged) write as non-invalidating without re-reading the live value.
func TestReadResultCarriesValStatus(t *testing.T) {
	addr := accounts.InternAddress([20]byte{0x01})
	key := accounts.InternKey([32]byte{0x02})
	v := Version{TxIndex: 3, Incarnation: 1}

	t.Run("unchanged", func(t *testing.T) {
		vm := NewVersionMap(nil)
		ws := &WriteSet{}
		ws.SetStorage(addr, key, &VersionedWrite[uint256.Int]{
			WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: key, Version: v, valStatus: ValueUnchanged},
			Val:         *uint256.NewInt(100),
		})
		vm.FlushVersionedWrites(ws, true, "")

		_, rr, ok := vm.ReadStorage(addr, key, 10)
		require.True(t, ok)
		require.Equal(t, ValueUnchanged, rr.ValStatus())
	})

	t.Run("changed", func(t *testing.T) {
		vm := NewVersionMap(nil)
		ws := &WriteSet{}
		ws.SetStorage(addr, key, &VersionedWrite[uint256.Int]{
			WriteHeader: WriteHeader{Address: addr, Path: StoragePath, Key: key, Version: v, valStatus: ValueChanged},
			Val:         *uint256.NewInt(200),
		})
		vm.FlushVersionedWrites(ws, true, "")

		_, rr, ok := vm.ReadStorage(addr, key, 10)
		require.True(t, ok)
		require.Equal(t, ValueChanged, rr.ValStatus())
	})
}
