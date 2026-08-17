package state

import (
	"sync"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types/accounts"
)

// Uses real goroutines so `go test -race` can catch unsynchronized access to the shared VersionMap's per-address entries.
func TestVersionMap_ConcurrentWriteReadValidate(t *testing.T) {
	t.Parallel()
	vm := NewVersionMap(nil)
	const workers = 16
	addrs := make([]accounts.Address, workers)
	for i := range addrs {
		addrs[i] = accounts.InternAddress([20]byte{0xab, byte(i)})
	}

	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(txIdx int) {
			defer wg.Done()
			v := Version{TxIndex: txIdx, Incarnation: 0}
			vm.WriteBalance(addrs[txIdx], v, *uint256.NewInt(uint64(txIdx + 1)), true)
			vm.WriteBalance(addrs[0], v, *uint256.NewInt(uint64(txIdx + 1)), true)
			vm.WriteNonce(addrs[txIdx], v, uint64(txIdx), true)
		}(w)
	}
	for r := range workers {
		wg.Add(1)
		go func(txIdx int) {
			defer wg.Done()
			for i := range workers {
				vm.ReadBalance(addrs[i], txIdx+workers)
				vm.ReadNonce(addrs[i], txIdx+workers)
			}
		}(r)
	}
	wg.Wait()

	// Only addrs[1:] have a single writer each, so their balance is deterministic; addrs[0] is shared and skipped.
	for i := 1; i < workers; i++ {
		got, res, ok := vm.ReadBalance(addrs[i], workers)
		require.True(t, ok && res.Status() == MVReadResultDone)
		require.Equal(t, uint64(i+1), got.Uint64())
	}
}
