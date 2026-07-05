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
// biz-logic-free: visibleFiles=int payload, dirtyFiles=int source, dirtyFile=int retired.
type testLifetime = Lifetime[int, int, int]

// newTestLifetime's stored recalc publishes *toPublish, so a test controls each
// published value by setting it before UpdateDirtyFiles.
func newTestLifetime() (lt *testLifetime, reclaimed *[]int, toPublish *int) {
	lt = &testLifetime{}
	reclaimed = &[]int{}
	toPublish = new(int)
	lt.Init(
		0,
		func(r []int) { *reclaimed = append(*reclaimed, r...) },
		func(int) *int { return toPublish },
	)
	return lt, reclaimed, toPublish
}

func publish(t *testing.T, lt *testLifetime, toPublish *int, value int, retired ...int) {
	t.Helper()
	*toPublish = value
	require.NoError(t, lt.UpdateDirtyFiles(func() ([]int, bool, error) { return retired, true, nil }))
}

func TestLifetime_AcquireSeesLatestPublish(t *testing.T) {
	lt, _, tp := newTestLifetime()

	v := lt.Acquire()
	require.Equal(t, 0, v.Value) // initial empty generation
	lt.Release(v)

	publish(t, lt, tp, 42)
	v = lt.Acquire()
	require.Equal(t, 42, v.Value)
	lt.Release(v)
}

func TestLifetime_ReclaimsRetiredWhenNoReaderPins(t *testing.T) {
	lt, reclaimed, tp := newTestLifetime()

	publish(t, lt, tp, 1, 10, 11) // supersedes the empty gen; nobody pinned it
	require.Equal(t, []int{10, 11}, *reclaimed)
}

func TestLifetime_DefersReclaimUntilReaderDrains(t *testing.T) {
	lt, reclaimed, tp := newTestLifetime()

	pinned := lt.Acquire()    // pins the empty generation
	publish(t, lt, tp, 1, 10) // retire 10 against the still-pinned generation
	require.Empty(t, *reclaimed)

	lt.Release(pinned) // last reader drains → now reclaimable
	require.Equal(t, []int{10}, *reclaimed)
}

func TestLifetime_MutatePublishFlagControlsPublish(t *testing.T) {
	lt, _, tp := newTestLifetime()
	publish(t, lt, tp, 7)
	before := lt.Visible()

	// publish=false → no new generation, same view
	*tp = 8
	require.NoError(t, lt.UpdateDirtyFiles(func() ([]int, bool, error) { return nil, false, nil }))
	require.Same(t, before, lt.Visible())
	require.Equal(t, 7, *lt.Visible())

	// publish=true → publishes the pending value
	require.NoError(t, lt.UpdateDirtyFiles(func() ([]int, bool, error) { return []int{10}, true, nil }))
	require.NotSame(t, before, lt.Visible())
	require.Equal(t, 8, *lt.Visible())
}

func TestLifetime_SlowReadDirtyFilesRunsUnderLockWithoutPublishing(t *testing.T) {
	lt, _, _ := newTestLifetime()
	before := lt.Visible()

	ran := false
	lt.SlowReadDirtyFiles(func() { ran = true })
	require.True(t, ran)
	require.Same(t, before, lt.Visible())
}

// Exercises Acquire's load-then-validate retry loop against concurrent publishes
// (run with -race).
func TestLifetime_ConcurrentReadersAndPublish(t *testing.T) {
	lt := &Lifetime[int, int, int]{}
	var reclaimedCount atomic.Int64
	toPublish := new(int)
	lt.Init(0, func(r []int) { reclaimedCount.Add(int64(len(r))) }, func(int) *int { return toPublish })

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
		publish(t, lt, toPublish, i, i)
	}
	close(stop)
	wg.Wait()

	require.LessOrEqual(t, reclaimedCount.Load(), int64(publishes))
}
