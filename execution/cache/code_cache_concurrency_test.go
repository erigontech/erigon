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
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/cachebudget"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/maphash"
	"github.com/erigontech/erigon/common/math"
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

	require.Equal(t, codeEntryBytes+int64(len(code)), cc.codeSize.Load(),
		"hashToCode size must reflect exactly one insert after concurrent same-code Puts")
	require.Equal(t, codeEntryBytes+int64(len(code)), cc.codeHashCodeSize.Load(),
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
// distinct codes whose combined size far exceeds a tiny budget. The layer
// evicts to stay within its byte bound (no freeze), and the onEvict-maintained
// byte counter must never drift negative under concurrency.
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

	// Residency settled far below the 128 distinct puts rather than freezing at
	// the first. No byte assertion at this size: the layer holds single-digit
	// entries here, where the admission granularity is the same order as the
	// budget. TestCodeCacheStaysWithinByteBudget pins the bound at a realistic size.
	require.Less(t, cc.codeHashToCode.Len(), workers,
		"the layer must evict to its byte budget, not hold all 128 distinct codes")
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

// The grow copy must carry the retiring generation over in Keys() order --
// oldest first -- because insertion order alone sets the new generation's
// recency. Reading each entry back with Get re-links it in the generation being
// retired, so the order the copy observes shifts as the copy walks it.
func TestGrowLRU_GrowCopyPreservesOrder(t *testing.T) {
	key := func(i uint64) uint64 { return i * 0x9E3779B97F4A7C15 }

	g := newGrowLRU[uint64](8*datasize.MB, 16, nil)
	defer g.Close()
	startCap := g.curCap.Load()

	const warmup = genericCacheStartCapacity / 2
	for i := range uint64(warmup) {
		g.Add(key(i), i)
	}
	for i := uint64(0); i < warmup; i += 3 { // pull recency away from insertion order
		g.Get(key(i))
	}

	var oldKeys, oldVals []uint64
	var trigger uint64
	grew := false
	for i := uint64(warmup); i < 8*genericCacheStartCapacity && !grew; i++ {
		old := g.cur.Load()
		if g.curCap.Load() < g.maxCap && old.Len() >= int(g.curCap.Load()) {
			oldKeys = old.Keys() // snapshot the generation this add is about to retire
			oldVals = make([]uint64, len(oldKeys))
			for j, k := range oldKeys {
				oldVals[j], _ = old.Peek(k)
			}
		}
		g.Add(key(i), i)
		if grew = g.curCap.Load() > startCap; grew {
			trigger = i // this add landed in the new generation, after the copy
		}
	}
	require.True(t, grew, "the fill must have triggered a real grow")

	// Replay the snapshot into the geometry the grow chose.
	want := g.newShards(g.curCap.Load())
	for j, k := range oldKeys {
		want.Add(k, oldVals[j])
	}
	want.Add(key(trigger), trigger)

	got := g.cur.Load()
	require.Positive(t, want.Len())
	require.Equal(t, want.Len(), got.Len(), "the grown generation must hold every copied entry")
	require.Equal(t, want.Keys(), got.Keys(), "the grown generation must keep the pre-grow order")
	for _, k := range got.Keys() {
		wantV, ok := want.Peek(k)
		require.True(t, ok)
		gotV, ok := got.Peek(k)
		require.True(t, ok)
		require.Equal(t, wantV, gotV)
	}
}

// A growLRU generation reserves no external payload for a value freelru stores
// inline, so the slot and per-shard charges alone have to cover it.
func TestGrowLRU_EnvelopeCoversInlineValueGeneration(t *testing.T) {
	prevBudget := cachebudget.Global
	t.Cleanup(func() { cachebudget.Global = prevBudget })
	cachebudget.Global = cachebudget.New(math.MaxInt64)

	sizeLayer := newGrowLRUEntries[codeSizeEntry](1<<20, 0, 0, nil)
	defer sizeLayer.Close()
	require.Zero(t, sizeLayer.avgBytes, "the size layer must reserve no external payload")

	// Zero payload so the assertion weighs the table and shard charge alone; the
	// code bytes a real content layer also reserves would mask an undercharge.
	contentLayer := newGrowLRUEntries[codeEntry](1<<20, 0, 0, nil)
	defer contentLayer.Close()

	t.Run("codeSizeEntry", func(t *testing.T) { requireGenerationCovered(t, sizeLayer) })
	t.Run("codeEntry", func(t *testing.T) { requireGenerationCovered(t, contentLayer) })
}

func requireGenerationCovered[V any](t *testing.T, g *growLRU[V]) {
	t.Helper()
	for _, capacity := range []uint32{1 << 12, 1 << 14, 1 << 16} {
		// TotalAlloc rather than HeapAlloc: a collection inside the window would
		// swamp a heap-size delta.
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		gen := g.newShards(capacity)
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		runtime.KeepAlive(gen)

		allocated := int64(after.TotalAlloc) - int64(before.TotalAlloc)
		charged := g.generationBytes(capacity)
		require.Positive(t, allocated)
		require.GreaterOrEqual(t, charged, allocated,
			"envelope reserves %d B for %d slots but the generation allocates %d B",
			charged, capacity, allocated)
	}
}
