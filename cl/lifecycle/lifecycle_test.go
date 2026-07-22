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

package lifecycle

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBundle_StopDrainsGoroutines(t *testing.T) {
	b := NewBundle()
	b.Start(context.Background())

	var running atomic.Int32
	for i := 0; i < 5; i++ {
		b.Go(func(ctx context.Context) {
			running.Add(1)
			defer running.Add(-1)
			<-ctx.Done()
		})
	}
	require.Eventually(t, func() bool { return running.Load() == 5 }, time.Second, 5*time.Millisecond)

	b.Stop()
	require.Equal(t, int32(0), running.Load(), "Stop must not return until all goroutines have exited")
}

func TestBundle_StopIsIdempotent(t *testing.T) {
	b := NewBundle()
	b.Start(context.Background())
	b.Go(func(ctx context.Context) { <-ctx.Done() })
	b.Stop()
	b.Stop()
}

func TestBundle_RestartWorks(t *testing.T) {
	b := NewBundle()
	b.Start(context.Background())
	b.Go(func(ctx context.Context) { <-ctx.Done() })
	b.Stop()

	b.Start(context.Background())
	var got atomic.Int32
	b.Go(func(ctx context.Context) {
		got.Store(1)
		<-ctx.Done()
	})
	require.Eventually(t, func() bool { return got.Load() == 1 }, time.Second, 5*time.Millisecond)
	b.Stop()
}

func TestGroup_StopInReverseOrder(t *testing.T) {
	g := NewGroup(nil)
	var order []string
	g.OnStop("a", func() { order = append(order, "a") })
	g.OnStop("b", func() { order = append(order, "b") })
	g.OnStop("c", func() { order = append(order, "c") })
	g.Stop()
	require.Equal(t, []string{"c", "b", "a"}, order)
}

func TestGroup_StopWaitsForEachDrain(t *testing.T) {
	g := NewGroup(nil)
	drained := make([]atomic.Bool, 3)
	for i := 0; i < 3; i++ {
		i := i
		b := NewBundle()
		b.Start(context.Background())
		b.Go(func(ctx context.Context) {
			<-ctx.Done()
			time.Sleep(20 * time.Millisecond)
			drained[i].Store(true)
		})
		g.OnStop("comp", b.Stop)
	}
	g.Stop()
	for i := 0; i < 3; i++ {
		require.True(t, drained[i].Load(), "component %d not drained", i)
	}
}

func TestGroup_OnStopAfterStopFiresImmediately(t *testing.T) {
	g := NewGroup(nil)
	g.Stop()
	var fired atomic.Bool
	g.OnStop("late", func() { fired.Store(true) })
	require.True(t, fired.Load())
}

func TestGroup_StopIsIdempotent(t *testing.T) {
	g := NewGroup(nil)
	var count atomic.Int32
	g.OnStop("a", func() { count.Add(1) })
	g.Stop()
	g.Stop()
	require.Equal(t, int32(1), count.Load())
}
