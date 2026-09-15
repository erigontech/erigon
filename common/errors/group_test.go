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

package errors

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGroup(t *testing.T) {
	t.Parallel()

	t.Run("empty group waits clean", func(t *testing.T) {
		g, _ := NewGroup(context.Background())
		require.NoError(t, g.Wait())
	})

	t.Run("a single real error is returned as-is", func(t *testing.T) {
		boom := errors.New("boom")
		g, _ := NewGroup(context.Background())
		g.Go(func() error { return boom })
		require.Same(t, boom, g.Wait())
	})

	t.Run("cancellation-only members are routine teardown", func(t *testing.T) {
		g, _ := NewGroup(context.Background())
		g.Go(func() error { return context.Canceled })
		g.Go(func() error { return fmt.Errorf("drain: %w", context.Canceled) })
		require.NoError(t, g.Wait())
	})

	t.Run("a raw canceled member cannot mask a late real failure", func(t *testing.T) {
		boom := errors.New("boom")
		canceledReturned := make(chan struct{})
		g, _ := NewGroup(context.Background())
		g.Go(func() error {
			defer close(canceledReturned)
			return context.Canceled
		})
		g.Go(func() error {
			<-canceledReturned
			return boom
		})
		require.ErrorIs(t, g.Wait(), boom)
	})

	t.Run("independent real failures are all preserved", func(t *testing.T) {
		first := errors.New("first")
		second := errors.New("second")
		g, _ := NewGroup(context.Background())
		g.Go(func() error { return first })
		g.Go(func() error {
			time.Sleep(20 * time.Millisecond)
			return second
		})
		got := g.Wait()
		require.ErrorIs(t, got, first)
		require.ErrorIs(t, got, second)
	})

	t.Run("the first member error cancels the group context", func(t *testing.T) {
		boom := errors.New("boom")
		g, gctx := NewGroup(context.Background())
		g.Go(func() error { return boom })
		g.Go(func() error {
			<-gctx.Done()
			return gctx.Err()
		})
		require.Same(t, boom, g.Wait())
	})
}
