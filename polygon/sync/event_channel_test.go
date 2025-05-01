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

package sync

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
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
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ch := NewEventChannel[string](2)

		go func() {
			err := ch.Run(ctx)
			if !errors.Is(err, context.Canceled) {
				panic("expected another error")
			}
		}()

		ch.PushEvent("event1")
		ch.PushEvent("event2")
		ch.PushEvent("event3")

		events := ch.Events()
		require.Equal(t, "event2", <-events)
		require.Equal(t, "event3", <-events)
		require.Empty(t, events)
	})
}
