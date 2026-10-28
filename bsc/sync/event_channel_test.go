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

package bscsync

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestEventChannel(t *testing.T) {
	t.Parallel()

	t.Run("PushEvent1", func(t *testing.T) {
		ch := NewEventChannel[string](2)

		ch.PushEvent("event1")
		e, ok := ch.takeEvent()
		require.True(t, ok)
		require.Equal(t, "event1", e)

		_, ok = ch.takeEvent()
		require.False(t, ok)
	})

	t.Run("PushEvent3", func(t *testing.T) {
		ch := NewEventChannel[string](2)

		ch.PushEvent("event1")
		ch.PushEvent("event2")
		ch.PushEvent("event3")

		e, ok := ch.takeEvent()
		require.True(t, ok)
		require.Equal(t, "event2", e)

		e, ok = ch.takeEvent()
		require.True(t, ok)
		require.Equal(t, "event3", e)

		_, ok = ch.takeEvent()
		require.False(t, ok)
	})

	t.Run("ConsumeEvents", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			ch := NewEventChannel[string](2)

			ch.PushEvent("event1")
			ch.PushEvent("event2")
			ch.PushEvent("event3")

			eg := errgroup.Group{}
			eg.Go(func() error {
				return ch.Run(ctx)
			})
			t.Cleanup(func() {
				err := eg.Wait()
				require.ErrorIs(t, err, context.Canceled)
			})

			events := ch.Events()
			require.Equal(t, "event2", <-events)
			require.Equal(t, "event3", <-events)
			require.Empty(t, events)
		})
	})
}

func TestEventChannelTakeAll(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ch := NewEventChannel[string](3)

		done := make(chan []string)
		go func() {
			events, err := ch.TakeAll(t.Context())
			require.NoError(t, err)
			done <- events
		}()
		synctest.Wait()
		ch.PushEvent("event1")
		require.Equal(t, []string{"event1"}, <-done)

		ch.PushEvent("event2")
		ch.PushEvent("event3")
		events, err := ch.TakeAll(t.Context())
		require.NoError(t, err)
		require.Equal(t, []string{"event2", "event3"}, events)

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		_, err = ch.TakeAll(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}
