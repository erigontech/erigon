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
	"bytes"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
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

// collectingStreamWriter keeps everything an event stream writes, safe to read while the stream runs.
type collectingStreamWriter struct {
	header http.Header
	mu     sync.Mutex
	body   bytes.Buffer
}

func (w *collectingStreamWriter) Header() http.Header { return w.header }

func (w *collectingStreamWriter) WriteHeader(int) {}

func (w *collectingStreamWriter) Flush() {}

func (w *collectingStreamWriter) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.body.Write(b)
}

func (w *collectingStreamWriter) count(topic beaconevents.EventTopic) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return strings.Count(w.body.String(), "event: "+string(topic)+"\n")
}

// A stream of state and operation topics delivers both: its operation buffer is drained, so more operation events than
// the buffer holds reach the client without blocking the emitter, and the head event arrives with them.
func TestEventStreamDeliversStateAndOperationTopics(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	a := &ApiHandler{emitters: beaconevents.NewEventEmitter(), beaconChainCfg: &cfg}
	w := &collectingStreamWriter{header: http.Header{}}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/events?topics=head,single_attestation", nil)
	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		a.EventSourceGetV1Events(w, req)
	}()
	require.Eventually(t, func() bool {
		return a.emitters.State().SendHead(&beaconevents.HeadData{}) == 1
	}, 5*time.Second, 10*time.Millisecond, "event stream never subscribed")
	// The stream starts reading only after it has subscribed both feeds, so a delivered head event means the operation
	// feed is subscribed too and no attestation below is sent to zero subscribers.
	require.Eventually(t, func() bool {
		return w.count(beaconevents.StateHead) == 1
	}, 5*time.Second, 10*time.Millisecond, "the stream did not deliver the head event")

	attestations := 2 * eventStreamFeedBuffer
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		for range attestations {
			a.emitters.Operation().SendSingleAttestation(&beaconevents.SingleAttestationData{})
		}
	}()
	select {
	case <-sent:
	case <-time.After(5 * time.Second):
		t.Fatal("operation events blocked the emitter: the stream does not drain its operation buffer")
	}

	require.Eventually(t, func() bool {
		return w.count(beaconevents.OpSingleAttestation) == attestations
	}, 5*time.Second, 10*time.Millisecond, "the stream did not deliver every attestation")
	require.Equal(t, 1, w.count(beaconevents.StateHead))
	select {
	case <-handlerDone:
		t.Fatal("event stream closed")
	default:
	}
}

// floodUntilDropped sends head events from several goroutines until one is dropped for every subscriber, so the only
// subscriber has lagged.
func floodUntilDropped(t *testing.T, emitters *beaconevents.EventEmitter) {
	t.Helper()
	var dropped atomic.Bool
	var senders sync.WaitGroup
	deadline := time.Now().Add(5 * time.Second)
	for range 4 {
		senders.Go(func() {
			for !dropped.Load() && time.Now().Before(deadline) {
				if emitters.State().TrySendHead(&beaconevents.HeadData{}) == 0 {
					dropped.Store(true)
				}
			}
		})
	}
	senders.Wait()
	require.True(t, dropped.Load(), "the subscriber never lagged")
}

// A stream that cannot take a head event is closed, so the client reconnects instead of missing the head silently.
func TestEventStreamClosesWhenItMissesAHeadEvent(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	a := &ApiHandler{emitters: beaconevents.NewEventEmitter(), beaconChainCfg: &cfg}
	w := &collectingStreamWriter{header: http.Header{}}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/events?topics=head", nil)
	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		a.EventSourceGetV1Events(w, req)
	}()
	require.Eventually(t, func() bool {
		return a.emitters.State().SendHead(&beaconevents.HeadData{}) == 1
	}, 5*time.Second, 10*time.Millisecond, "event stream never subscribed")

	floodUntilDropped(t, a.emitters)
	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the stream kept running after it missed a head event")
	}
}
