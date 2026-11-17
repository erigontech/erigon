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
	"testing/synctest"
	"time"

	"github.com/erigontech/erigon/common"
	ethevent "github.com/erigontech/erigon/common/event"
	"github.com/stretchr/testify/require"
)

func TestEmitterSet(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
		synctest.Wait()
		emitter.Operation().SendAttestation(&AttestationData{})
		<-done
	})
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

func TestNotifyEnvelopeArrivedDoesNotBlockWhenFull(t *testing.T) {
	emitter := NewEventEmitter()
	for range envelopeArrivedBufferSize {
		emitter.NotifyEnvelopeArrived(common.Hash{1})
	}

	done := make(chan struct{})
	go func() {
		emitter.NotifyEnvelopeArrived(common.Hash{2})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("notification blocked on a full buffer")
	}
}

func TestNotifyEnvelopeArrivedIsNilSafe(t *testing.T) {
	var emitter *EventEmitter
	require.Nil(t, emitter.EnvelopeArrived())
	emitter.NotifyEnvelopeArrived(common.Hash{1})
}

// head, head_v2, execution_payload and execution_payload_available report a dropped event only on their own feeds, to
// subscribers of that topic. The state and operation feeds, which carry every topic, drop them silently: their
// subscribers may not have asked for them.
func TestEventsReportAFullSubscriberOnlyOnTheirOwnFeed(t *testing.T) {
	state, operation := newStateFeed(), newOpFeed()
	for name, tc := range map[string]struct {
		subscribe    func(chan *EventStream) ethevent.Subscription
		subscribeAll func(chan *EventStream) ethevent.Subscription
		send         func() int
	}{
		"head": {
			subscribe:    state.SubscribeHead,
			subscribeAll: state.Subscribe,
			send:         func() int { return state.TrySendHead(&HeadData{}) },
		},
		"head_v2": {
			subscribe:    state.SubscribeHeadV2,
			subscribeAll: state.Subscribe,
			send:         func() int { return state.SendHeadV2(&HeadV2Data{}) },
		},
		"execution_payload": {
			subscribe:    operation.SubscribeExecutionPayload,
			subscribeAll: operation.Subscribe,
			send:         func() int { return operation.SendExecutionPayload(&ExecutionPayloadData{}) },
		},
		"execution_payload_available": {
			subscribe:    operation.SubscribeExecutionPayloadAvailable,
			subscribeAll: operation.Subscribe,
			send:         func() int { return operation.SendExecutionPayloadAvailable(&ExecutionPayloadAvailableData{}) },
		},
	} {
		t.Run(name, func(t *testing.T) {
			topic := make(chan *EventStream)
			topicSub := tc.subscribe(topic)
			defer topicSub.Unsubscribe()
			all := make(chan *EventStream)
			allSub := tc.subscribeAll(all)
			defer allSub.Unsubscribe()

			require.Zero(t, tc.send())
			select {
			case err := <-topicSub.Err():
				require.ErrorIs(t, err, ethevent.ErrSubscriberLagged)
			default:
				t.Fatalf("%s event dropped without telling its subscriber", name)
			}
			select {
			case err := <-allSub.Err():
				t.Fatalf("subscriber of every topic told about a %s event it may not want: %v", name, err)
			default:
			}
		})
	}
}
