// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package cache

import (
	"encoding/binary"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/maphash"
)

// Concurrent puts of the same cold code must account each content layer once.
// The per-key stripe keeps the membership check, accounting, and insertion atomic.
func TestCodeCache_ConcurrentPutSameCode_NoSizeDrift(t *testing.T) {
	cc := closeOnCleanup(t, NewCodeCache(64*datasize.MB, 16*datasize.MB))

	addr := make([]byte, 20)
	addr[0] = 0xab
	code := []byte("some non-trivial contract bytecode payload xyz")
	codeHash := crypto.Keccak256(code)

	const workers = 64
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			cc.PutWithCodeHash(addr, code, codeHash, 1)
		})
	}
	wg.Wait()

	require.Equal(t, int64(8+len(code)), cc.codeSize.Load(),
		"hashToCode size must reflect exactly one insert after concurrent same-code Puts")
	require.Equal(t, int64(len(codeHash)+len(code)), cc.codeHashCodeSize.Load(),
		"codeHashToCode size must reflect exactly one insert after concurrent same-code Puts")
	require.Equal(t, int64(1), cc.codeSizeEntries.Load(),
		"codeSizeByCodeHash must hold exactly one entry after concurrent same-code Puts")

	got, ok := cc.GetByCodeHash(codeHash)
	require.True(t, ok)
	require.Equal(t, code, got)
}

// TestCodeCache_ByteCheckRejectsForeignKeyHash verifies the collision guard:
// an entry whose stored keyHash differs from the requested codeHash is treated
// as a miss, so a 64-bit maphash collision can never serve the wrong code.
func TestCodeCache_ByteCheckRejectsForeignKeyHash(t *testing.T) {
	cc := closeOnCleanup(t, NewCodeCache(64*datasize.MB, 16*datasize.MB))

	code := []byte("contract A bytecode")
	realHash := crypto.Keccak256(code)
	cc.PutWithCodeHash(nil, code, realHash, 1)

	// Sanity: the real hash hits.
	_, ok := cc.GetByCodeHash(realHash)
	require.True(t, ok)

	// Simulate a foreign 32-byte codeHash that collapses to the same maphash
	// bucket by storing a colliding entry directly under a different keyHash.
	foreign := make([]byte, 32)
	copy(foreign, realHash)
	foreign[0] ^= 0xff // different 32-byte key
	cc.codeHashToCode.Put(maphash.Hash(foreign), codeEntry{code: code, keyHash: hash32(realHash), txNum: 1, epoch: cc.coh.Epoch()})

	// The stored entry's keyHash is realHash, not foreign — Get must reject it.
	_, ok = cc.GetByCodeHash(foreign)
	require.False(t, ok, "byte-check must reject an entry whose keyHash differs from the requested codeHash")
}

// TestCodeCache_ConcurrentDistinctPuts_RespectCap drives many workers putting
// distinct codes whose combined size far exceeds a tiny cap. The freelru layer
// evicts the coldest entries to stay within its entry cap (no freeze), and the
// OnEvict-maintained byte counter must never drift negative under concurrency.
func TestCodeCache_ConcurrentDistinctPuts_RespectCap(t *testing.T) {
	const codeCap = 4 * datasize.KB
	cc := closeOnCleanup(t, NewCodeCache(codeCap, 16*datasize.MB))

	const workers = 128
	var wg sync.WaitGroup
	for i := range workers {
		idx := i
		wg.Go(func() {
			code := make([]byte, 256)
			code[0], code[1] = byte(idx), byte(idx>>8) // distinct code per worker
			cc.PutWithCodeHash(nil, code, crypto.Keccak256(code), 1)
		})
	}
	wg.Wait()

	// The entry cap (codeCap/avgCodeEntryBytes) is the hard bound; residency
	// settled far below the 128 distinct puts rather than freezing at the first.
	require.Less(t, cc.codeHashToCode.Len(), workers,
		"freelru must evict to its entry cap, not hold all 128 distinct codes")
	require.GreaterOrEqual(t, cc.codeHashCodeSize.Load(), int64(0),
		"byte counter must stay non-negative (OnEvict accounting must not double-subtract)")
}

// Same atomicity requirement for the addr→code binding: a concurrent
// authoritative Put must win over a conditional prefetch put in every
// interleaving.
func TestCodeCache_PutIfAbsentAtomicWithPut(t *testing.T) {
	cc := closeOnCleanup(t, NewCodeCache(64*datasize.MB, 16*datasize.MB))
	addr := make([]byte, 20)
	addr[0] = 0xcd
	fresh := []byte{0xaa, 1, 2, 3}
	stale := []byte{0xbb, 4, 5, 6}
	for round := range 20000 {
		binary.BigEndian.PutUint64(addr[1:], uint64(round))
		var wg sync.WaitGroup
		wg.Go(func() { cc.Put(addr, fresh, 20) })
		wg.Go(func() { cc.PutIfAbsent(addr, stale, 10) })
		wg.Wait()
		v, ok := cc.Get(addr)
		require.True(t, ok)
		require.Equal(t, fresh, v, "round %d: PutIfAbsent raced past a concurrent Put", round)
	}
}

func TestCodeCache_ClearRacingPut_EpochAlias(t *testing.T) {
	cc := closeOnCleanup(t, NewCodeCache(64*datasize.MB, 16*datasize.MB))
	cc.Unwind(300)

	addr := make([]byte, 20)
	addr[0] = 0xef
	code := []byte("dead-fork-code")
	codeID := maphash.Hash(code)
	preClearEpoch := cc.coh.Epoch()

	cc.Clear()
	// Model a writer that sampled the epoch before Clear and published after
	// the relevant layers were purged.
	cc.addrToHash.Add(common.BytesToAddress(addr), versionedAddressID{addrID: codeID, txNum: 200, epoch: preClearEpoch})
	cc.hashToCode.Put(codeID, codeEntry{code: code, txNum: 200, epoch: preClearEpoch})
	cc.Unwind(150)

	_, ok := cc.Get(addr)
	require.False(t, ok, "pre-Clear epoch must not alias the live epoch after a later unwind")
}

func TestCodeCache_ClearFencesStartedPut(t *testing.T) {
	// Limit Go execution to one logical processor. Each runtime.Gosched call
	// yields to the queued goroutine, which runs until it reaches the blocked lock.
	previousProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previousProcs)

	cc := closeOnCleanup(t, NewCodeCache(64*datasize.MB, 16*datasize.MB))
	cc.Unwind(300)

	addr := []byte{0xef}
	code := []byte("dead-fork-code")
	cc.addrBindMu.Lock()

	var wg sync.WaitGroup
	putStarted := make(chan struct{})
	wg.Go(func() {
		close(putStarted)
		cc.Put(addr, code, 200)
	})
	<-putStarted
	runtime.Gosched()

	clearStarted := make(chan struct{})
	wg.Go(func() {
		close(clearStarted)
		cc.Clear()
	})
	<-clearStarted
	runtime.Gosched()

	cc.addrBindMu.Unlock()
	wg.Wait()

	_, ok := cc.Get(addr)
	require.False(t, ok, "Clear must remove a write that started in the retiring generation")
}

// The O(1) entry counter that replaced freelru's all-shard Len on growLRU's add
// path must not drift from the LRU's real length on any mutation path.
func TestGrowLRU_LenTracksLRU(t *testing.T) {
	var evictions int
	g := newGrowLRU[uint64](8*datasize.MB, 16, func(uint64, uint64) { evictions++ })
	defer g.Close()
	check := func(phase string) {
		t.Helper()
		require.Equal(t, g.cur.Load().lru.Len(), g.Len(), "entry counter drifted after %s", phase)
	}
	// growLRU hashes keys with the identity function, so spread the shard-selection
	// bits the way its real maphash/keccak-derived keys do.
	key := func(i uint64) uint64 { return i * 0x9E3779B97F4A7C15 }

	for i := range uint64(500) {
		g.Put(key(i), i)
	}
	check("adds")
	require.Equal(t, 500, g.Len(), "500 distinct keys must fit below the 1024-slot start capacity")

	for i := range uint64(100) {
		g.Remove(key(i))
	}
	check("removes")
	require.Equal(t, 100, evictions, "the caller's OnEvict must still fire for each removal")

	before := g.cur.Load()
	for i := uint64(500); i < 4000; i++ {
		g.Put(key(i), i)
	}
	require.NotEqual(t, before, g.cur.Load(), "grow did not happen")
	check("grow")

	g.Purge()
	require.Equal(t, 0, g.Len())
	check("purge")
}

// growLRU's generation swap is not fenced against writers (see its doc
// comment): a grow can copy a key into the new generation while a same-key
// refresh (putContentLocked's stale-entry path) is in flight. Put resolves
// g.cur once so its removal and its store land on the same generation; split
// across the swap, the removal would hit the retired generation and the store
// would replace the copy in place without firing OnEvict, double-counting it.
func TestGrowLRU_GrowRacePutDoesNotDoubleCount(t *testing.T) {
	g := newGrowLRU[uint64](8*datasize.MB, 16, nil)
	defer g.Close()

	h := uint64(7)
	g.Put(h, 1)
	gen1 := g.cur.Load()

	// Build the next generation exactly like maybeGrow's copy loop, and
	// publish it while h is still present in gen1.
	newCap := g.curCap.Load() * genericCacheGrowFactor
	gen2 := g.newShards(newCap)
	for _, k := range gen1.lru.Keys() {
		if v, ok := gen1.lru.Get(k); ok {
			gen2.add(k, v)
		}
	}
	g.cur.Store(gen2)
	g.curCap.Store(newCap)

	g.Put(h, 2)

	require.Equal(t, gen2.lru.Len(), g.Len(),
		"counter must not double-count a grow-copied key replaced in place")
	got, ok := g.Get(h)
	require.True(t, ok)
	require.Equal(t, uint64(2), got, "the refreshed value must be the one served")

	// A refresh that resolved the retired generation must leave the live count alone.
	live := g.Len()
	gen1.lru.Remove(h)
	gen1.add(h, 3)
	require.Equal(t, live, g.Len(), "a write lost in the retired generation must not move the live counter")
	require.Equal(t, gen1.lru.Len(), gen1.len(), "the retired generation's own counter must stay exact")
}

// CodeCache drives all three growLRU layers through putContentLocked, whose
// stale path displaces the resident entry. The key space repeats so puts land on
// resident keys, an unwinder makes those repeats stale so the displacing path
// runs, and the code budget leaves room above the start capacity so the grow
// gates fire. Each layer's counter must still equal its LRU's real length.
func TestCodeCache_GrowLRULenCounterUnderConcurrency(t *testing.T) {
	// 64MB over avgCodeEntryBytes puts the two code layers' ceiling well above
	// genericCacheStartCapacity; at 8MB it lands below and they never grow.
	cc := closeOnCleanup(t, NewCodeCache(64*datasize.MB, 8*datasize.MB))
	// Floor 0, so every later Unwind turns the whole resident set stale on its
	// epoch bump alone.
	cc.Unwind(0)

	const workers = 8
	const perWorker = 3000
	const keySpace = 4096

	var unwinder sync.WaitGroup
	done := make(chan struct{})
	unwinder.Go(func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			cc.Unwind(0)
			runtime.Gosched()
		}
	})

	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			addr := make([]byte, 20)
			code := make([]byte, 40)
			codeHash := make([]byte, 32)
			for i := range perWorker {
				k := uint64(w*perWorker+i) % keySpace
				binary.BigEndian.PutUint64(addr[12:], k)
				binary.BigEndian.PutUint64(code, k)
				binary.BigEndian.PutUint64(codeHash, k)
				cc.PutWithCodeHash(addr, code, codeHash, uint64(i))
			}
		})
	}
	wg.Wait()
	close(done)
	unwinder.Wait()

	for _, layer := range []struct {
		name string
		lru  *growLRU[codeEntry]
	}{
		{"hashToCode", cc.hashToCode},
		{"codeHashToCode", cc.codeHashToCode},
	} {
		require.Equal(t, layer.lru.cur.Load().lru.Len(), layer.lru.Len(), "%s counter drifted", layer.name)
		require.Greater(t, layer.lru.curCap.Load(), layer.lru.startCap, "%s never grew", layer.name)
	}
	require.Equal(t, cc.codeSizeByCodeHash.cur.Load().lru.Len(), cc.codeSizeByCodeHash.Len(),
		"codeSizeByCodeHash counter drifted")
	require.Greater(t, cc.codeSizeByCodeHash.curCap.Load(), cc.codeSizeByCodeHash.startCap,
		"codeSizeByCodeHash never grew")
}

// Parallel adds while the LRU is still below its ceiling — the window where
// every add runs the grow check.
func BenchmarkGrowLRUParallelPutGrow(b *testing.B) {
	g := newGrowLRU[uint64](256*datasize.MB, 48, nil)
	defer g.Close()
	var seq atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := seq.Add(1) * 0x9E3779B97F4A7C15
			g.Put(n, n)
		}
	})
}

// The three stale drops on CodeCache's read path hold no put stripe, so they
// interleave freely with a striped writer's stale-entry refresh while the LRU
// grows. Whatever the interleaving, the live generation's counter must equal
// its real length: an under-count wedges growth for good, because both grow
// gates read the counter.
func TestGrowLRU_CountExactUnderStripedRefreshAndUnstripedRemove(t *testing.T) {
	g := newGrowLRU[uint64](8*datasize.MB, 16, nil)
	defer g.Close()

	const keySpace = 4096
	key := func(i uint64) uint64 { return (i % keySpace) * 0x9E3779B97F4A7C15 }

	var stripes [256]sync.Mutex
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			for i := range uint64(20000) {
				h := key(uint64(w)*20000 + i)
				stripe := &stripes[uint8(h)]
				stripe.Lock()
				g.Put(h, i)
				stripe.Unlock()
			}
		})
	}
	wg.Go(func() {
		for i := range uint64(60000) {
			g.Remove(key(i))
		}
	})
	wg.Wait()

	gen := g.cur.Load()
	require.Equal(t, gen.lru.Len(), gen.len(), "live generation's counter drifted from its real length")
	require.NotEqual(t, g.startCap, g.curCap.Load(), "the LRU never grew, so the growth gates were not exercised")
}

// Keys() fixes the copy order before the loop and insertion order alone sets the
// new generation's recency, so reading the retiring generation with Peek must
// produce exactly the generation a Get-based copy would.
func TestGrowLRU_GrowCopyMatchesGetBasedCopy(t *testing.T) {
	key := func(i uint64) uint64 { return i * 0x9E3779B97F4A7C15 }

	grown := newGrowLRU[uint64](8*datasize.MB, 16, nil)
	defer grown.Close()
	// Same start geometry but pinned there, so it keeps holding what the grow read.
	source := newGrowLRU[uint64](genericCacheStartCapacity*16*datasize.B, 16, nil)
	defer source.Close()
	startCap := grown.curCap.Load()
	require.Equal(t, startCap, source.curCap.Load())
	require.Equal(t, source.maxCap, source.curCap.Load(), "source must not grow")

	const warmup = genericCacheStartCapacity / 2
	for i := range uint64(warmup) {
		grown.Put(key(i), i)
		source.Put(key(i), i)
	}
	for i := uint64(0); i < warmup; i += 3 { // pull recency away from insertion order
		grown.Get(key(i))
		source.Get(key(i))
	}

	var trigger uint64
	grew := false
	for i := uint64(warmup); i < 8*genericCacheStartCapacity && !grew; i++ {
		grown.Put(key(i), i)
		if grew = grown.curCap.Load() > startCap; grew {
			trigger = i // this put landed in the new generation, after the copy
			break
		}
		source.Put(key(i), i)
	}
	require.True(t, grew, "the fill must have triggered a real grow")

	// Rebuild the copy the way it read before Peek, in the grow's own geometry.
	want := grown.newShards(grown.curCap.Load())
	src := source.cur.Load()
	for _, k := range src.lru.Keys() {
		if v, ok := src.lru.Get(k); ok {
			want.add(k, v)
		}
	}
	want.add(key(trigger), trigger)

	got := grown.cur.Load()
	require.Positive(t, want.len())
	require.Equal(t, want.len(), got.len(), "the grown generation must hold every source entry")
	require.Equal(t, want.lru.Keys(), got.lru.Keys(), "the grown generation must keep the get-copy order")
	for _, k := range got.lru.Keys() {
		wantV, ok := want.lru.Peek(k)
		require.True(t, ok)
		gotV, ok := got.lru.Peek(k)
		require.True(t, ok)
		require.Equal(t, wantV, gotV)
	}
}
