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
	"testing"
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

func TestCompositeEventChannel(t *testing.T) {
	t.Parallel()

	ch := NewCompositeEventChannel[topicEvent](map[string]*EventChannel[topicEvent]{
		"topic1": NewEventChannel[topicEvent](2),
		"topic2": NewEventChannel[topicEvent](3),
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		err := ch.Run(ctx)
		println(err)
		return err
	})

	ch.PushEvent(topicEvent{
		event: "event1", // should get dropped due to event2 and event3 overflowing the channel for topic1
		topic: "topic1",
	})
	ch.PushEvent(topicEvent{
		event: "event2",
		topic: "topic1",
	})
	ch.PushEvent(topicEvent{
		event: "event3",
		topic: "topic1",
	})
	ch.PushEvent(topicEvent{
		event: "event4",
		topic: "topic2",
	})

	events := make([]string, 3)
	events[0] = read(ctx, t, ch.Events()).event
	events[1] = read(ctx, t, ch.Events()).event
	events[2] = read(ctx, t, ch.Events()).event
	require.ElementsMatch(t, events, []string{"event2", "event3", "event4"})
	require.Len(t, ch.topics["topic1"].events, 0)
	require.Len(t, ch.topics["topic2"].events, 0)
	cancel()
	err := eg.Wait()
	require.ErrorIs(t, err, context.Canceled)
}

type topicEvent struct {
	event string
	topic string
}

func (te topicEvent) Topic() string {
	return te.topic
}

func read(ctx context.Context, t *testing.T, ch <-chan topicEvent) topicEvent {
	select {
	case e := <-ch:
		return e
	case <-ctx.Done():
		require.FailNow(t, "timed out waiting for event")
		return topicEvent{}
	}
}
