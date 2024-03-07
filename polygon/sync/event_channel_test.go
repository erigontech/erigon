package sync

import (
	"context"
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
			require.ErrorIs(t, err, context.Canceled)
		}()

		ch.PushEvent("event1")
		ch.PushEvent("event2")
		ch.PushEvent("event3")

		events := ch.Events()
		require.Equal(t, "event2", <-events)
		require.Equal(t, "event3", <-events)
		require.Equal(t, 0, len(events))
	})
}
