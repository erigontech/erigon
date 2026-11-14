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

package beaconevents

import (
	"testing"
	"time"

	"github.com/erigontech/erigon/common"
	ethevent "github.com/erigontech/erigon/common/event"
	"github.com/stretchr/testify/require"
)

func TestEmitterSet(t *testing.T) {
	emitter := NewEventEmitter()
	done := make(chan struct{})
	go func() {
		ch := make(chan *EventStream, 10)
		t.Logf("Subscribing to emitter")
		sub := emitter.Operation().Subscribe(ch)
		defer sub.Unsubscribe()
		t.Logf("Subscribed to emitter")
		event := <-ch
		t.Logf("Received event: %v", event)
		close(done)
	}()
	time.Sleep(100 * time.Millisecond)
	emitter.Operation().SendAttestation(&AttestationData{})
	<-done
}

func TestPayloadAttributesSubscriberTracking(t *testing.T) {
	emitter := NewEventEmitter()
	require.False(t, emitter.HasPayloadAttributesSubscribers())

	releaseFirst := emitter.TrackPayloadAttributesSubscriber()
	releaseSecond := emitter.TrackPayloadAttributesSubscriber()
	require.True(t, emitter.HasPayloadAttributesSubscribers())

	releaseFirst()
	require.True(t, emitter.HasPayloadAttributesSubscribers())

	releaseSecond()
	require.False(t, emitter.HasPayloadAttributesSubscribers())
}

func TestNotifyDataColumnStoredDoesNotBlockWhenFull(t *testing.T) {
	emitter := NewEventEmitter()
	for range dataColumnStoredBufferSize {
		emitter.NotifyDataColumnStored(common.Hash{1})
	}

	done := make(chan struct{})
	go func() {
		emitter.NotifyDataColumnStored(common.Hash{2})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("notification blocked on a full buffer")
	}
}

func TestNotifyDataColumnStoredIsNilSafe(t *testing.T) {
	var emitter *EventEmitter
	require.Nil(t, emitter.DataColumnStored())
	emitter.NotifyDataColumnStored(common.Hash{1})
}

// head and head_v2 report a dropped event only on their own feeds, to subscribers of that topic. The state feed, which
// carries every topic, drops them silently: its subscribers may not have asked for them.
func TestHeadEventsReportAFullSubscriberOnlyOnTheirOwnFeed(t *testing.T) {
	for name, tc := range map[string]struct {
		subscribe func(*stateFeed, chan *EventStream) ethevent.Subscription
		send      func(*stateFeed) int
	}{
		"head": {
			subscribe: (*stateFeed).SubscribeHead,
			send:      func(f *stateFeed) int { return f.TrySendHead(&HeadData{}) },
		},
		"head_v2": {
			subscribe: (*stateFeed).SubscribeHeadV2,
			send:      func(f *stateFeed) int { return f.SendHeadV2(&HeadV2Data{}) },
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newStateFeed()
			topic := make(chan *EventStream)
			topicSub := tc.subscribe(f, topic)
			defer topicSub.Unsubscribe()
			all := make(chan *EventStream)
			allSub := f.Subscribe(all)
			defer allSub.Unsubscribe()

			require.Zero(t, tc.send(f))
			select {
			case err := <-topicSub.Err():
				require.ErrorIs(t, err, ethevent.ErrSubscriberLagged)
			default:
				t.Fatal("head event dropped without telling its subscriber")
			}
			select {
			case err := <-allSub.Err():
				t.Fatalf("state feed subscriber told about a head event it may not want: %v", err)
			default:
			}
		})
	}
}
