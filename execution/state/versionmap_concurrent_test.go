package state

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	// Readers: concurrently read balances across all addresses at varying floors.
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

	// Each writer's own-address balance must be its published value (last writer
	// wins per address; own addresses have a single writer).
	for i := 1; i < workers; i++ {
		got, res, ok := vm.ReadBalance(addrs[i], workers)
		require.True(t, ok && res.Status() == MVReadResultDone)
		require.Equal(t, uint64(i+1), got.Uint64())
	}
}

// A first read of a slot no tx wrote must not wait for a writer that holds the
// account's entry lock: on a hot contract that wait is the read's whole cost.
func TestVersionMap_UnwrittenSlotReadDoesNotWaitForEntryLock(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xab})
	written := accounts.InternKey([32]byte{1})
	unwritten := accounts.InternKey([32]byte{2})

	vm := NewVersionMap(nil)
	vm.WriteStorage(addr, written, Version{TxIndex: 1}, *uint256.NewInt(11), true)
	vm.StoreOriginStorage(addr, unwritten, *uint256.NewInt(7))

	e := vm.load(addr)
	e.mu.Lock()
	defer e.mu.Unlock()

	type read struct {
		val  uint256.Int
		res  ReadResult
		ok   bool
		life lifecycleVerdict
	}
	done := make(chan read, 1)
	go func() {
		val, res, ok, life := vm.ReadStorageLife(addr, unwritten, 5)
		done <- read{val, res, ok, life}
	}()
	select {
	case r := <-done:
		require.True(t, r.ok)
		require.Equal(t, uint64(7), r.val.Uint64())
		require.Equal(t, originIndex, r.res.DepIdx())
		require.Equal(t, LifecycleLive, r.life.state)
	case <-time.After(2 * time.Second):
		t.Fatal("read of an unwritten slot blocked on the entry lock")
	}
}

// One hot account, one hot slot. Readers run against writers that publish, flip and
// delete cells and allocate fresh slots; a slot published before a read starts must
// be visible to that read, and the read must see one version of it.
func TestVersionMap_ConcurrentReadersWritersOneSlot(t *testing.T) {
	t.Parallel()
	addr := accounts.InternAddress([20]byte{0xcd})
	hot := accounts.InternKey([32]byte{1})
	const txs, fresh = 64, 2000
	freshKey := func(i int) accounts.StorageKey { return accounts.InternKey([32]byte{2, byte(i >> 8), byte(i)}) }

	vm := NewVersionMap(nil)
	vm.StoreOriginStorage(addr, hot, *uint256.NewInt(1))

	var published atomic.Int64
	published.Store(-1)
	stop := make(chan struct{})
	var writers, readers sync.WaitGroup

	writers.Add(1)
	go func() {
		defer writers.Done()
		for i := range fresh {
			vm.WriteStorage(addr, freshKey(i), Version{TxIndex: 1}, *uint256.NewInt(uint64(i) + 1), true)
			published.Store(int64(i))
		}
	}()
	writers.Add(1)
	go func() {
		defer writers.Done()
		for round := range 200 {
			for tx := 2; tx < txs; tx++ {
				val := *uint256.NewInt(uint64(tx) + 100)
				writes := newWriteSet(&VersionedWrite[uint256.Int]{
					WriteHeader: WriteHeader{Address: addr, Key: hot, Version: Version{TxIndex: tx, Incarnation: Incarnation(round)}, Path: StoragePath},
					Val:         val,
				})
				vm.FlushVersionedWrites(writes, false)
				vm.MarkWritesValidated(writes, nil)
				vm.MarkWritesComplete(writes)
				vm.Delete(addr, StoragePath, hot, tx, true)
			}
		}
	}()

	var errs atomic.Pointer[string]
	fail := func(msg string) { errs.CompareAndSwap(nil, &msg) }
	for range 8 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				n := published.Load()
				if n >= 0 {
					j := int(n) - i%(int(n)+1)
					val, res, ok, life := vm.ReadStorageLife(addr, freshKey(j), txs)
					if !ok || val.Uint64() != uint64(j)+1 || res.DepIdx() != 1 || life.state != LifecycleLive {
						fail("a published slot read as absent or with a wrong value")
						return
					}
				}
				val, res, ok, _ := vm.ReadStorageLife(addr, hot, txs)
				if !ok {
					fail("a slot with a seeded origin read as absent")
					return
				}
				if res.DepIdx() == originIndex && val.Uint64() != 1 {
					fail("an origin read returned a cell value")
					return
				}
				if res.DepIdx() >= 2 && val.Uint64() != uint64(res.DepIdx())+100 {
					fail("a cell read returned the value of another version")
					return
				}
			}
		}()
	}

	writers.Wait()
	close(stop)
	readers.Wait()
	if msg := errs.Load(); msg != nil {
		t.Fatal(*msg)
	}

	// A read that saw only the origin stops validating once a lower tx publishes.
	rs := ReadSet{}
	rs.SetStorage(addr, hot, VersionedRead[uint256.Int]{ReadHeader: ReadHeader{Source: MapRead, Version: Version{TxIndex: originIndex}}, Val: *uint256.NewInt(1)})
	check := func(rv, wv Version) VersionValidity {
		if rv != wv {
			return VersionInvalid
		}
		return VersionValid
	}
	require.Equal(t, VersionValid, vm.ValidateReadSet(txs, rs, check, false, ""))
	vm.WriteStorage(addr, hot, Version{TxIndex: 3}, *uint256.NewInt(9), true)
	require.NotEqual(t, VersionValid, vm.ValidateReadSet(txs, rs, check, false, ""))
}

// A destruct published before a read starts must reach a read of a slot no tx wrote:
// that read skips the entry lock, so the destruct has to be visible without it.
func TestVersionMap_ConcurrentDestructVisibleToUnwrittenSlotRead(t *testing.T) {
	t.Parallel()
	const n = 2000
	written := accounts.InternKey([32]byte{1})
	unwritten := accounts.InternKey([32]byte{2})
	vm := NewVersionMap(nil)
	addrs := make([]accounts.Address, n)
	for i := range addrs {
		addrs[i] = accounts.InternAddress([20]byte{0xde, byte(i >> 8), byte(i)})
		vm.WriteStorage(addrs[i], written, Version{TxIndex: 0}, *uint256.NewInt(1), true)
	}

	var published atomic.Int64
	published.Store(-1)
	stop := make(chan struct{})
	var readers sync.WaitGroup
	var stale atomic.Bool
	for range 8 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				p := published.Load()
				vm.ReadStorageLife(addrs[min(int(p)+1, n-1)], unwritten, 5)
				if p < 0 {
					continue
				}
				j := int(p) - i%(int(p)+1)
				_, _, _, life := vm.ReadStorageLife(addrs[j], unwritten, 5)
				state, _, _, _ := vm.AccountLifecycleAt(addrs[j], 5)
				if life.state != LifecycleAbsent || state != LifecycleAbsent {
					stale.Store(true)
					return
				}
			}
		}()
	}
	for i := range addrs {
		vm.WriteSelfDestruct(addrs[i], Version{TxIndex: 1}, true, true)
		published.Store(int64(i))
	}
	close(stop)
	readers.Wait()
	require.False(t, stale.Load(), "a read missed a destruct published before it started")
}
