package state

import (
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// The parallel executor writes each committed tx's cells into a shared VersionMap
// while other workers read and validate against it concurrently. This exercises
// that surface with real goroutines so the race detector can catch unsynchronised
// access to the per-address entries (run under -race in CI's race shards).
func TestVersionMap_ConcurrentWriteReadValidate(t *testing.T) {
	t.Parallel()
	vm := NewVersionMap(nil)
	const workers = 16
	addrs := make([]accounts.Address, workers)
	for i := range addrs {
		addrs[i] = accounts.InternAddress([20]byte{0xab, byte(i)})
	}

	var wg sync.WaitGroup
	// Writers: each publishes a balance cell for its own tx index, then a few
	// shared addresses so writers and readers contend on the same entries.
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(txIdx int) {
			defer wg.Done()
			v := Version{TxIndex: txIdx, Incarnation: 0}
			vm.WriteBalance(addrs[txIdx], v, *uint256.NewInt(uint64(txIdx + 1)), true)
			vm.WriteBalance(addrs[0], v, *uint256.NewInt(uint64(txIdx + 1)), true)
			vm.WriteNonce(addrs[txIdx], v, uint64(txIdx), true)
		}(w)
	}
	// Readers: concurrently read balances across all addresses at varying floors.
	for r := 0; r < workers; r++ {
		wg.Add(1)
		go func(txIdx int) {
			defer wg.Done()
			for i := 0; i < workers; i++ {
				vm.ReadBalance(addrs[i], txIdx+workers)
				vm.ReadNonce(addrs[i], txIdx+workers)
			}
		}(r)
	}
	wg.Wait()

	// Each writer's own-address balance must be its published value (last writer
	// wins per address; own addresses have a single writer).
	for i := 1; i < workers; i++ {
		got, res, ok := vm.ReadBalance(addrs[i], workers)
		require.True(t, ok && res.Status() == MVReadResultDone)
		require.Equal(t, uint64(i+1), got.Uint64())
	}
}
