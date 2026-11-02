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

package handler

import (
	"reflect"
	"strings"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
)

// A stream that asked only for state topics is not subscribed to the operation feed, so gossip traffic never reaches
// its buffer.
func TestEventStreamSubscribesOnlyTheFeedsOfItsTopics(t *testing.T) {
	emitters := beaconevents.NewEventEmitter()
	sub := subscribeEventStream(emitters, mapset.NewSet(beaconevents.StateHead, beaconevents.StateBlock))
	defer sub.unsubscribe()

	require.Zero(t, emitters.Operation().SendSingleAttestation(&beaconevents.SingleAttestationData{}))
	require.Equal(t, 1, emitters.State().TrySendHead(&beaconevents.HeadData{}))
}

// State and operation events have separate buffers: a full operation buffer does not make the stream miss a head event,
// which is sent without waiting for subscribers.
func TestEventStreamHeadIsNotCrowdedOutByOperationEvents(t *testing.T) {
	emitters := beaconevents.NewEventEmitter()
	sub := subscribeEventStream(emitters, mapset.NewSet(beaconevents.StateHead, beaconevents.OpSingleAttestation))
	defer sub.unsubscribe()
	for range eventStreamFeedBuffer {
		require.Equal(t, 1, emitters.Operation().SendSingleAttestation(&beaconevents.SingleAttestationData{}))
	}

	require.Equal(t, 1, emitters.State().TrySendHead(&beaconevents.HeadData{}), "head event dropped behind operation events")
	e := <-sub.state
	require.Equal(t, beaconevents.StateHead, e.Event)
}

// Every topic a feed emits is listed under that feed: a stream subscribes only the feeds of its topics, so a topic
// listed under the wrong feed would never reach it.
func TestEventStreamTopicsAreListedUnderTheFeedThatEmitsThem(t *testing.T) {
	emitters := beaconevents.NewEventEmitter()
	emitted := map[beaconevents.EventTopic]eventFeed{}
	for feed, sender := range map[eventFeed]any{stateFeed: emitters.State(), operationFeed: emitters.Operation()} {
		events := make(chan *beaconevents.EventStream, 1)
		subscribe := reflect.ValueOf(sender).MethodByName("Subscribe")
		sub := subscribe.Call([]reflect.Value{reflect.ValueOf(events)})[0].Interface().(interface{ Unsubscribe() })
		senderType := reflect.TypeOf(sender)
		for i := range senderType.NumMethod() {
			method := senderType.Method(i)
			if !strings.HasPrefix(method.Name, "Send") && !strings.HasPrefix(method.Name, "TrySend") {
				continue
			}
			require.Equal(t, 2, method.Type.NumIn(), method.Name)
			reflect.ValueOf(sender).Method(i).Call([]reflect.Value{reflect.New(method.Type.In(1).Elem())})
			e := <-events
			emitted[e.Event] = feed
		}
		sub.Unsubscribe()
	}

	require.Len(t, emitted, len(validTopics), "a feed emits a topic that is not listed, or a listed topic is never emitted")
	for topic, feed := range emitted {
		listed, ok := validTopics[topic]
		require.True(t, ok, "topic %s is emitted but not listed", topic)
		require.Equal(t, feed, listed, "topic %s is listed under the wrong feed", topic)
	}
}
