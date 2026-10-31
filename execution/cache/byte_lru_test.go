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
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"weak"

	"github.com/c2h5oh/datasize"
	"github.com/maypok86/otter/v2"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/cachebudget"
)

// NewByteLRU stays out of cachebudget.Global: filling it reserves nothing and Close returns nothing.
func TestNewByteLRUOutsideBudget(t *testing.T) {
	used := cachebudget.Global.Used()
	b := NewByteLRU(4*datasize.KB, func(_ uint64, v []byte) int64 { return int64(len(v)) })
	for i := range uint64(64) {
		b.Add(i, make([]byte, 512))
	}
	require.Equal(t, used, cachebudget.Global.Used())
	require.LessOrEqual(t, b.Len(), 8)
	b.Close()
	require.Equal(t, used, cachebudget.Global.Used())
}

func TestNewByteLRUDroppedIsCollectable(t *testing.T) {
	dropped := func() weak.Pointer[otter.Cache[uint64, []byte]] {
		b := NewByteLRU(datasize.MB, func(_ uint64, v []byte) int64 { return int64(len(v)) })
		b.Add(1, make([]byte, 64))
		return weak.Make(b.c)
	}()
	runtime.GC()
	require.Nil(t, dropped.Value(), "an unbudgeted cache dropped without Close must be collected")
}

func TestBudgetedByteLRUClosedIsCollectable(t *testing.T) {
	dropped := func() weak.Pointer[otter.Cache[uint64, []byte]] {
		b := newByteLRU(datasize.MB, func(_ uint64, v []byte) int64 { return int64(len(v)) }, nil)
		b.Add(1, make([]byte, 64))
		b.Close()
		return weak.Make(b.c)
	}()
	runtime.GC()
	require.Nil(t, dropped.Value(), "a budgeted cache dropped after Close must be collected")
}

func TestHashByteLRUMissesForeignHash(t *testing.T) {
	l := NewHashByteLRU(datasize.MB, func(v []byte) int64 { return int64(len(v)) })
	hash := common.Hash{1, 2, 3}
	l.Add(hash, []byte{1})
	_, ok := l.Get(hash)
	require.True(t, ok)

	foreign := hash
	foreign[31]++
	_, ok = l.Get(foreign)
	require.False(t, ok, "a hash sharing the 8-byte slot must miss")
}

func TestHashByteLRUWeighsAnEntryOnce(t *testing.T) {
	var calls atomic.Int32
	l := NewHashByteLRU(datasize.MB, func(v []byte) int64 { calls.Add(1); return int64(len(v)) })
	l.Add(common.Hash{1}, make([]byte, 100))
	_, ok := l.Get(common.Hash{1})
	require.True(t, ok)
	require.Equal(t, int32(1), calls.Load())
}

// otter drops a replacement node written before the first write drains.
func TestByteLRUConcurrentSameKeyStaysBounded(t *testing.T) {
	b := NewByteLRU(datasize.MB, func(_ uint64, v []byte) int64 { return int64(len(v)) })
	value := make([]byte, 64*1024)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for key := range uint64(2000) {
				b.Add(key, value)
			}
		})
	}
	wg.Wait()
	b.c.CleanUp()
	require.LessOrEqual(t, b.Len(), 16, "a 1MB cache of 64KB entries holds 16")
}

func TestByteLRUAddReplacesLiveKey(t *testing.T) {
	b := NewByteLRU(datasize.MB, func(_ uint64, v []byte) int64 { return int64(len(v)) })
	require.True(t, b.Add(1, []byte("first")))
	require.True(t, b.Add(1, []byte("second")))
	v, ok := b.Get(1)
	require.True(t, ok)
	require.Equal(t, []byte("second"), v)
}

func TestByteLRUBudgetedAddReplacesLiveKey(t *testing.T) {
	var evicted [][]byte
	b := newByteLRU(datasize.MB, func(_ uint64, v []byte) int64 { return int64(len(v)) },
		func(_ uint64, v []byte) { evicted = append(evicted, v) })
	defer b.Close()

	require.True(t, b.Add(1, []byte("first")))
	require.True(t, b.Add(1, []byte("second")))
	b.c.CleanUp()

	v, ok := b.Get(1)
	require.True(t, ok)
	require.Equal(t, []byte("second"), v)
	// The displaced value is refunded through onEvict, which also sees it.
	require.Equal(t, [][]byte{[]byte("first")}, evicted)
	require.Equal(t, int64(len("second")), b.resident.Load())
	require.Equal(t, int64(b.c.WeightedSize()), b.resident.Load())
}

// A budgeted layer refunds through onEvict, so its charge must match what otter holds.
func TestByteLRUBudgetedConcurrentSameKeyAccounting(t *testing.T) {
	b := newByteLRU(8*datasize.MB, func(_ uint64, v []byte) int64 { return int64(len(v)) }, nil)
	defer b.Close()
	value := make([]byte, 64*1024)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			for key := range uint64(2000) {
				b.Add(key, value)
			}
		})
	}
	wg.Wait()
	b.c.CleanUp()
	require.Equal(t, int64(b.c.WeightedSize()), b.resident.Load())
	require.LessOrEqual(t, b.resident.Load(), b.limit.Load())
}

// A growing working set must still raise the limit: the growth step is decided
// before the write, so maintenance evicting the newcomer cannot hide the
// pressure that called for it.
func TestByteLRUGrowsOnPositiveNetChange(t *testing.T) {
	prev := cachebudget.Global
	t.Cleanup(func() { cachebudget.Global = prev })
	cachebudget.Global = cachebudget.New(1 << 40)

	val := make([]byte, 256*1024)
	b := newByteLRU(64*datasize.MB, func(_ uint64, v []byte) int64 { return int64(len(v)) }, nil)
	defer b.Close()

	start := b.limit.Load()
	for key := range uint64(64) {
		require.True(t, b.Add(key, val))
	}
	b.c.CleanUp()

	require.Greater(t, b.limit.Load(), start, "distinct keys past the floor must grow the limit")
	require.LessOrEqual(t, b.resident.Load(), b.limit.Load())
}

// A newcomer W-TinyLFU refuses, or evicts straight away, must not leave its
// weight charged: otter reports the insert as done, so the charge is only
// returned by the deletion handler (CauseOverflow, not CauseReplacement).
func TestByteLRURejectedNewcomerIsRefunded(t *testing.T) {
	// A budget far smaller than the working set, so admission has to refuse.
	val := make([]byte, 64*1024)
	b := newByteLRU(datasize.MB, func(_ uint64, v []byte) int64 { return int64(len(v)) }, nil)
	defer b.Close()

	for key := range uint64(512) {
		require.True(t, b.Add(key, val), "Add reports the value was offered")
	}
	b.c.CleanUp()

	// The charge tracks what otter actually holds, however many were refused.
	require.Equal(t, int64(b.c.WeightedSize()), b.resident.Load())
	require.LessOrEqual(t, b.resident.Load(), b.limit.Load())
	require.Positive(t, b.Len())
}

// An unbudgeted layer holds maxBytes from the start, so it never grows and its
// residency counter is not maintained. What it must still do is stay inside the
// byte bound under eviction pressure and serve correct values.
func TestByteLRUUnbudgetedStaysBoundedAndCorrect(t *testing.T) {
	const maxBytes = datasize.ByteSize(1 << 20)
	val := func(i uint64) []byte { return append(make([]byte, 64*1024-1), byte(i)) }
	b := NewByteLRU(maxBytes, func(_ uint64, v []byte) int64 { return int64(len(v)) })

	for key := range uint64(256) {
		require.True(t, b.Add(key, val(key)))
	}
	b.c.CleanUp()

	require.LessOrEqual(t, int64(b.c.WeightedSize()), int64(maxBytes), "bound holds without a refund path")
	require.Positive(t, b.Len())
	// Whatever survived answers with its own value.
	for key := range uint64(256) {
		if v, ok := b.Get(key); ok {
			require.Equal(t, val(key), v, "key %d served the wrong value", key)
		}
	}
	// The limit never moves: there is nothing to grow into.
	require.Equal(t, int64(maxBytes), b.limit.Load())
}
