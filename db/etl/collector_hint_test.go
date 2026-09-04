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

package etl

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

func emptyPool(bufferSize datasize.ByteSize) *Allocator {
	return NewAllocator(&sync.Pool{New: func() any { return NewSortableBuffer(bufferSize) }})
}

func collectN(t *testing.T, c *Collector, n int) {
	t.Helper()
	key := make([]byte, 8)
	for i := range n {
		binary.BigEndian.PutUint64(key, uint64(i))
		require.NoError(t, c.Collect(key, key))
	}
}

func runCycle(t *testing.T, c *Collector, n int) {
	t.Helper()
	collectN(t, c, n)
	require.NoError(t, c.Load(nil, "", discardLoad, TransformArgs{}))
	c.Close()
}

func purgePool() {
	runtime.GC()
	runtime.GC()
}

func TestCollectorSizesADrawnBufferToTheNamesLastFill(t *testing.T) {
	allocator := emptyPool(BufferOptimalSize)
	runCycle(t, NewCollectorWithAllocator("writer", t.TempDir(), allocator, log.New()), 1000)
	require.Equal(t, 1000, allocator.lastFill("writer"))

	purgePool()
	c := NewCollectorWithAllocator("writer", t.TempDir(), allocator, log.New())
	require.NoError(t, c.Collect([]byte{1}, []byte{1}))
	require.GreaterOrEqual(t, cap(c.buf.(*sortableBuffer).entries), 1000,
		"a buffer drawn from a purged pool must hold the previous cycle's entries without growing")
	c.Close()

	purgePool()
	other := NewCollectorWithAllocator("other", t.TempDir(), allocator, log.New())
	require.NoError(t, other.Collect([]byte{1}, []byte{1}))
	require.Less(t, cap(other.buf.(*sortableBuffer).entries), 1000, "a name that never filled anything gets no hint")
	other.Close()
}

func TestCollectorFillHintFollowsTheLastCycle(t *testing.T) {
	allocator := emptyPool(BufferOptimalSize)
	runCycle(t, NewCollectorWithAllocator("writer", t.TempDir(), allocator, log.New()), 1000)
	require.Equal(t, 1000, allocator.lastFill("writer"))

	c := NewCollectorWithAllocator("writer", t.TempDir(), allocator, log.New())
	runCycle(t, c, 10)
	require.Equal(t, 10, allocator.lastFill("writer"), "the hint must follow the last cycle down, not keep the high-water mark")

	c.Close()
	require.Equal(t, 10, allocator.lastFill("writer"), "a second Close must not erase the hint")

	idle := NewCollectorWithAllocator("writer", t.TempDir(), allocator, log.New())
	idle.Close()
	require.Equal(t, 10, allocator.lastFill("writer"), "a cycle that wrote nothing keeps the previous hint")
}

func TestCollectorFillHintCoversABackgroundSpill(t *testing.T) {
	c := NewCollectorWithAllocator(t.Name(), t.TempDir(), emptyPool(4*datasize.KB), log.New()).SortAndFlushInBackground(true)
	key := make([]byte, 8)
	collected := 0
	for len(c.dataProviders) == 0 {
		binary.BigEndian.PutUint64(key, uint64(collected))
		require.NoError(t, c.Collect(key, key))
		collected++
	}
	require.Nil(t, c.buf, "a background spill hands the buffer to the flusher")
	require.Equal(t, collected, c.fill, "the spilled buffer's fill is the hint for the next draw")

	require.NoError(t, c.Collect([]byte{1}, []byte{1}))
	require.GreaterOrEqual(t, cap(c.buf.(*sortableBuffer).entries), collected)
	require.NoError(t, c.Load(nil, "", discardLoad, TransformArgs{}))
	c.Close()
	require.Equal(t, collected, c.allocator.lastFill(t.Name()))
}

func TestAllocatorFillHintsAreBounded(t *testing.T) {
	allocator := emptyPool(BufferOptimalSize)
	for i := range 2 * maxFillHints {
		c := NewCollectorWithAllocator(fmt.Sprintf("index-%d", i), t.TempDir(), allocator, log.New())
		collectN(t, c, 1)
		c.Close()
		allocator.mu.Lock()
		require.LessOrEqual(t, len(allocator.fills), maxFillHints,
			"a collector named after a one-off file must not add to the hint table forever")
		allocator.mu.Unlock()
	}
	require.Equal(t, 1, allocator.lastFill("index-0"), "an established name survives one-shot name churn")
	require.Zero(t, allocator.lastFill(fmt.Sprintf("index-%d", 2*maxFillHints-1)), "a new name is refused once the table is full")
}

func TestAllocatorFillHintSurvivesNameChurn(t *testing.T) {
	allocator := emptyPool(BufferOptimalSize)
	runCycle(t, NewCollectorWithAllocator("accounts.domain.flush", t.TempDir(), allocator, log.New()), 1000)
	require.Equal(t, 1000, allocator.lastFill("accounts.domain.flush"))

	for i := range maxFillHints {
		c := NewCollectorWithAllocator(fmt.Sprintf("RecSplit Building index-%d", i), t.TempDir(), allocator, log.New())
		collectN(t, c, 1)
		c.Close()
	}
	require.Equal(t, 1000, allocator.lastFill("accounts.domain.flush"),
		"the long-lived writer's hint must survive one-shot name churn")
}
