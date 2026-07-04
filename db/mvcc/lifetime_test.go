// Copyright 2024 The Erigon Authors
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

package mvcc

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// Instantiated with plain scalars (no file types) to keep the package honestly
// biz-logic-free: V=int payload, R=int retired.
type testLifetime = Lifetime[int, int]

func newTestLifetime() (lt *testLifetime, reclaimed *[]int) {
	lt = &testLifetime{}
	reclaimed = &[]int{}
	lt.Init(func(r []int) { *reclaimed = append(*reclaimed, r...) })
	return lt, reclaimed
}

func publish(t *testing.T, lt *testLifetime, value int, retired ...int) {
	t.Helper()
	require.NoError(t, lt.Recalc(
		func() ([]int, error) { return retired, nil },
		func() *int { return &value },
	))
}

func TestLifetime_AcquireSeesLatestPublish(t *testing.T) {
	lt, _ := newTestLifetime()

	v := lt.Acquire()
	require.Equal(t, 0, v.Value) // initial empty generation
	lt.Release(v)

	publish(t, lt, 42)
	v = lt.Acquire()
	require.Equal(t, 42, v.Value)
	lt.Release(v)
}

func TestLifetime_ReclaimsRetiredWhenNoReaderPins(t *testing.T) {
	lt, reclaimed := newTestLifetime()

	publish(t, lt, 1, 10, 11) // supersedes the empty gen; nobody pinned it
	require.Equal(t, []int{10, 11}, *reclaimed)
}

func TestLifetime_DefersReclaimUntilReaderDrains(t *testing.T) {
	lt, reclaimed := newTestLifetime()

	pinned := lt.Acquire() // pins the empty generation
	publish(t, lt, 1, 10)  // retire 10 against the still-pinned generation
	require.Empty(t, *reclaimed)

	lt.Release(pinned) // last reader drains → now reclaimable
	require.Equal(t, []int{10}, *reclaimed)
}

func TestLifetime_RecalcNilDeclinesPublish(t *testing.T) {
	lt, reclaimed := newTestLifetime()
	publish(t, lt, 7)
	before := lt.Visible()

	require.NoError(t, lt.Recalc(
		func() ([]int, error) { return nil, nil },
		func() *int { return nil }, // decline publishing
	))
	require.Same(t, before, lt.Visible())
	require.Empty(t, *reclaimed)
}

func TestLifetime_MutateOnlyDoesNotPublish(t *testing.T) {
	lt, _ := newTestLifetime()
	before := lt.Visible()

	mutated := false
	require.NoError(t, lt.Recalc(
		func() ([]int, error) { mutated = true; return nil, nil },
		nil, // no publish
	))
	require.True(t, mutated)
	require.Same(t, before, lt.Visible())
}

// intStore is a slice-backed Dirty[int] — keeps the test free of any real
// container type, proving Lifetime only needs the structural Scan.
type intStore []int

func (s intStore) Scan(iter func(item int) bool) {
	for _, v := range s {
		if !iter(v) {
			return
		}
	}
}

func TestLifetime_ForEachDirtyItemSweepsAllStores(t *testing.T) {
	lt, _ := newTestLifetime()
	lt.RegisterDirty(intStore{1, 2}, intStore{3})

	var seen []int
	lt.ForEachDirtyItem(func(item int) { seen = append(seen, item) })
	require.Equal(t, []int{1, 2, 3}, seen)
}

// Exercises Acquire's load-then-validate retry loop against concurrent publishes
// (run with -race).
func TestLifetime_ConcurrentReadersAndPublish(t *testing.T) {
	lt := &Lifetime[int, int]{}
	var reclaimedCount atomic.Int64
	lt.Init(func(r []int) { reclaimedCount.Add(int64(len(r))) })

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					v := lt.Acquire()
					_ = v.Value
					lt.Release(v)
				}
			}
		}()
	}

	const publishes = 1000
	for i := 1; i <= publishes; i++ {
		publish(t, lt, i, i)
	}
	close(stop)
	wg.Wait()

	require.LessOrEqual(t, reclaimedCount.Load(), int64(publishes))
}
