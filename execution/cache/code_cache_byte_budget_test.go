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

	"github.com/erigontech/erigon/common/cachebudget"
	"github.com/erigontech/erigon/common/crypto"
)

// The bound must hold whatever the contract-size distribution turns out to be.
func TestCodeCacheStaysWithinByteBudget(t *testing.T) {
	for _, codeLen := range []int{64 * 1024, 24 * 1024, 1024} {
		t.Run(datasize.ByteSize(codeLen).HR(), func(t *testing.T) {
			const budget = 4 * datasize.MB
			cache := closeOnCleanup(t, NewCodeCache(budget, 1*datasize.MB))

			for i := range 4000 {
				code := make([]byte, codeLen)
				binary.BigEndian.PutUint64(code, uint64(i))
				addr := make([]byte, 20)
				binary.BigEndian.PutUint64(addr, uint64(i))
				cache.PutWithCodeHash(addr, code, crypto.Keccak256(code), uint64(i))
			}
			cache.hashToCode.c.CleanUp()
			cache.codeHashToCode.c.CleanUp()

			// An addr is required or putCodeLocked never runs and codeSize is 0.
			perLayer := int64(budget / 2)
			require.NotZero(t, cache.CodeSizeBytes(), "hashToCode must be populated")
			require.LessOrEqual(t, cache.CodeSizeBytes(), perLayer,
				"hashToCode exceeds its share of the budget")
			require.LessOrEqual(t, cache.codeHashCodeSize.Load(), perLayer,
				"codeHashToCode exceeds its share of the budget")
			require.LessOrEqual(t, cache.CodeSizeBytes()+cache.codeHashCodeSize.Load(), int64(budget),
				"the two layers together exceed the configured budget")
		})
	}
}

// Same bound with concurrent writers.
func TestCodeCacheStaysWithinByteBudgetConcurrent(t *testing.T) {
	const budget = 4 * datasize.MB
	cache := closeOnCleanup(t, NewCodeCache(budget, 1*datasize.MB))

	var wg sync.WaitGroup
	for w := range 16 {
		wg.Go(func() {
			for i := range 500 {
				code := make([]byte, 64*1024)
				binary.BigEndian.PutUint64(code, uint64(w*500+i))
				addr := make([]byte, 20)
				binary.BigEndian.PutUint64(addr, uint64(w*500+i))
				cache.PutWithCodeHash(addr, code, crypto.Keccak256(code), 1)
			}
		})
	}
	wg.Wait()
	// Eviction is not synchronous with Add; force the drain before reading.
	cache.hashToCode.c.CleanUp()
	cache.codeHashToCode.c.CleanUp()

	perLayer := int64(budget / 2)
	require.LessOrEqual(t, cache.CodeSizeBytes(), perLayer)
	require.LessOrEqual(t, cache.codeHashCodeSize.Load(), perLayer)
}

// A closed cache must release its bytes: it stays reachable until its runtime
// cleanup runs, so whatever the eviction callback captures outlives Close.
func TestCodeCacheClosedIsCollectable(t *testing.T) {
	prev := cachebudget.Global
	t.Cleanup(func() { cachebudget.Global = prev })
	cachebudget.Global = cachebudget.New(1 << 40)

	heap := func() float64 {
		for range 4 {
			runtime.GC()
		}
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return float64(m.HeapAlloc) / 1048576
	}

	// Direct check first; the heap check below sees only the aggregate.
	one := NewCodeCache(1*datasize.MB, 1*datasize.MB)
	for i := range 4000 {
		code := make([]byte, 256)
		binary.BigEndian.PutUint64(code, uint64(i))
		one.Put(nil, code, uint64(i))
	}
	require.NotZero(t, one.hashToCode.Len(), "layer must be populated before Close")
	one.Close()
	require.Zero(t, one.hashToCode.Len(), "Close must drop the entries")
	require.Zero(t, one.hashToCode.resident.Load(), "Close must zero the residency")

	const caches = 200
	base := heap()
	for range caches {
		cc := NewCodeCache(1*datasize.MB, 1*datasize.MB)
		for i := range 4000 {
			code := make([]byte, 256)
			binary.BigEndian.PutUint64(code, uint64(i))
			cc.Put(nil, code, uint64(i))
		}
		cc.Close()
	}
	retained := heap() - base

	// Each held ~1.2MB while live.
	require.Less(t, retained, float64(caches)*0.12,
		"closed caches retained %.2f MB across %d caches", retained, caches)
}

// EXTCODESIZE is answered by the size-only layer, so that layer has to outlive
// eviction from the content layers. It cannot grow once the content layers have
// drawn the envelope down, so it has to be born wide enough.
func TestCodeSizeOutlivesContentEviction(t *testing.T) {
	prev := cachebudget.Global
	t.Cleanup(func() { cachebudget.Global = prev })
	// An exhausted envelope: the unconditional birth Take still lands, every
	// growth step is refused.
	cachebudget.Global = cachebudget.New(0)

	const budget = 32 * datasize.MB
	cache := closeOnCleanup(t, NewCodeCache(budget, 1*datasize.MB))

	const contracts = 3000
	hashes := make([][]byte, contracts)
	for i := range contracts {
		code := make([]byte, 64*1024)
		binary.BigEndian.PutUint64(code, uint64(i))
		addr := make([]byte, 20)
		binary.BigEndian.PutUint64(addr, uint64(i))
		hashes[i] = crypto.Keccak256(code)
		cache.PutWithCodeHash(addr, code, hashes[i], uint64(i))
	}

	known := 0
	for _, h := range hashes {
		if _, ok := cache.GetCodeSizeByCodeHash(h); ok {
			known++
		}
	}
	require.Equal(t, contracts, known,
		"the size layer dropped sizes for code the content layers evicted")
}
