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
	cc.codeHashToCode.Add(maphash.Hash(foreign), codeEntry{code: code, keyHash: hash32(realHash), txNum: 1, epoch: cc.coh.Epoch()})

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
	cc.hashToCode.Add(codeID, codeEntry{code: code, txNum: 200, epoch: preClearEpoch})
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
		g.Add(key(i), i)
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
		g.Add(key(i), i)
	}
	require.NotEqual(t, before, g.cur.Load(), "grow did not happen")
	check("grow")

	g.Purge()
	require.Equal(t, 0, g.Len())
	check("purge")
}

// growLRU's generation swap is not fenced against writers (see its doc
// comment): a grow can copy a key into the new generation before a same-key
// Remove/Add pair (putContentLocked's stale-entry refresh) observes the swap.
// The Remove then lands on the retired generation while the Add lands on the
// new one, which already holds the copy — freelru.Add replaces it in place
// without firing OnEvict, so the counter must not increment again.
func TestGrowLRU_GrowRaceReplaceDoesNotDoubleCount(t *testing.T) {
	g := newGrowLRU[uint64](8*datasize.MB, 16, nil)
	defer g.Close()

	h := uint64(7)
	g.Add(h, 1)
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

	// The writer's Remove call raced ahead of the swap and landed on the
	// retired generation.
	gen1.lru.Remove(h)
	// Its Add call now targets gen2, which already holds h from the copy.
	g.Add(h, 2)

	require.Equal(t, gen2.lru.Len(), g.Len(),
		"counter must not double-count a grow-copied key replaced in place")
}

// CodeCache drives three growLRU layers through putContentLocked, which removes
// a stale entry before re-adding it; concurrent puts of distinct code must still
// leave each layer's counter equal to its LRU's real length.
func TestCodeCache_GrowLRULenCounterUnderConcurrency(t *testing.T) {
	cc := closeOnCleanup(t, NewCodeCache(8*datasize.MB, 8*datasize.MB))

	const workers = 8
	const perWorker = 3000
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			addr := make([]byte, 20)
			code := make([]byte, 40)
			for i := range perWorker {
				binary.BigEndian.PutUint64(addr[12:], uint64(w*perWorker+i))
				binary.BigEndian.PutUint64(code, uint64(w*perWorker+i))
				cc.Put(addr, code, uint64(i))
			}
		})
	}
	wg.Wait()

	require.Equal(t, cc.hashToCode.cur.Load().lru.Len(), cc.hashToCode.Len(), "hashToCode counter drifted")
	require.Equal(t, cc.codeHashToCode.cur.Load().lru.Len(), cc.codeHashToCode.Len(), "codeHashToCode counter drifted")
	require.Equal(t, cc.codeSizeByCodeHash.cur.Load().lru.Len(), cc.codeSizeByCodeHash.Len(), "codeSizeByCodeHash counter drifted")
}

// Parallel adds while the LRU is still below its ceiling — the window where
// every add runs the grow check.
func BenchmarkGrowLRUParallelAddGrow(b *testing.B) {
	g := newGrowLRU[uint64](256*datasize.MB, 48, nil)
	defer g.Close()
	var seq atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := seq.Add(1) * 0x9E3779B97F4A7C15
			g.Add(n, n)
		}
	})
}
