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
	// The state feed reaches the block buffer, which ignores head; the head feed reaches the head buffer.
	require.Equal(t, 2, emitters.State().TrySendHead(&beaconevents.HeadData{}))
	require.Equal(t, beaconevents.StateHead, (<-sub.head).Event)
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
	e := <-sub.head
	require.Equal(t, beaconevents.StateHead, e.Event)
}

// A full operation buffer does not make the stream miss an execution_payload_available event, which is sent without
// waiting for subscribers.
func TestEventStreamPayloadAvailableIsNotCrowdedOutByOperationEvents(t *testing.T) {
	emitters := beaconevents.NewEventEmitter()
	sub := subscribeEventStream(emitters, mapset.NewSet(beaconevents.OpExecutionPayloadAvailable, beaconevents.OpDataColumnSidecar))
	defer sub.unsubscribe()
	for range eventStreamFeedBuffer {
		require.Equal(t, 1, emitters.Operation().SendDataColumnSidecar(&beaconevents.DataColumnSidecarData{}))
	}

	require.Equal(t, 1, emitters.Operation().SendExecutionPayloadAvailable(&beaconevents.ExecutionPayloadAvailableData{}), "execution_payload_available event dropped behind operation events")
	e := <-sub.payload
	require.Equal(t, beaconevents.OpExecutionPayloadAvailable, e.Event)
}

// A stream that asked for both payload topics gets both events.
func TestEventStreamDeliversBothPayloadTopics(t *testing.T) {
	emitters := beaconevents.NewEventEmitter()
	sub := subscribeEventStream(emitters, mapset.NewSet(beaconevents.OpExecutionPayload, beaconevents.OpExecutionPayloadAvailable))
	defer sub.unsubscribe()

	require.Equal(t, 1, emitters.Operation().SendExecutionPayload(&beaconevents.ExecutionPayloadData{}))
	require.Equal(t, 1, emitters.Operation().SendExecutionPayloadAvailable(&beaconevents.ExecutionPayloadAvailableData{}))
	require.Len(t, sub.payload, 2)
}

// A stream that has been unsubscribed no longer takes the payload events.
func TestEventStreamUnsubscribeStopsPayloadEvents(t *testing.T) {
	emitters := beaconevents.NewEventEmitter()
	sub := subscribeEventStream(emitters, mapset.NewSet(beaconevents.OpExecutionPayload, beaconevents.OpExecutionPayloadAvailable))
	sub.unsubscribe()

	require.Zero(t, emitters.Operation().SendExecutionPayload(&beaconevents.ExecutionPayloadData{}))
	require.Zero(t, emitters.Operation().SendExecutionPayloadAvailable(&beaconevents.ExecutionPayloadAvailableData{}))
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

// floodUntilLagged calls send, which returns how many subscribers took the event, at most limit times from several
// goroutines, and stops at the first call that reaches fewer than subscribers, so one subscriber has lagged while still
// subscribed.
func floodUntilLagged(t *testing.T, send func() int, subscribers, limit int) {
	t.Helper()
	var sends atomic.Int64
	var lagged atomic.Bool
	var senders sync.WaitGroup
	for range 4 {
		senders.Go(func() {
			for !lagged.Load() && sends.Add(1) <= int64(limit) {
				if send() < subscribers {
					lagged.Store(true)
				}
			}
		})
	}
	senders.Wait()
	require.True(t, lagged.Load(), "no subscriber lagged within %d events", limit)
}

// A stream that cannot take an event it asked for from a feed of its own is closed, so the client reconnects instead of
// missing the event silently.
func TestEventStreamClosesWhenItMissesAnEvent(t *testing.T) {
	for _, tc := range []struct {
		topic beaconevents.EventTopic
		send  func(*beaconevents.EventEmitter) int
	}{
		{
			topic: beaconevents.StateHead,
			send:  func(e *beaconevents.EventEmitter) int { return e.State().TrySendHead(&beaconevents.HeadData{}) },
		},
		{
			topic: beaconevents.OpExecutionPayload,
			send: func(e *beaconevents.EventEmitter) int {
				return e.Operation().SendExecutionPayload(&beaconevents.ExecutionPayloadData{})
			},
		},
		{
			topic: beaconevents.OpExecutionPayloadAvailable,
			send: func(e *beaconevents.EventEmitter) int {
				return e.Operation().SendExecutionPayloadAvailable(&beaconevents.ExecutionPayloadAvailableData{})
			},
		},
	} {
		t.Run(string(tc.topic), func(t *testing.T) {
			a, _, handlerDone := startTopicStream(t, string(tc.topic))
			require.Eventually(t, func() bool { return tc.send(a.emitters) == 1 }, 5*time.Second, 10*time.Millisecond, "event stream never subscribed")

			// Fewer events than the write queue holds cannot close the stream through a queue overflow, so only the
			// lag error can close it.
			floodUntilLagged(t, func() int { return tc.send(a.emitters) }, 1, eventStreamWriteQueueSize-1)
			waitDone(t, handlerDone, "the stream kept running after it missed the "+string(tc.topic)+" event")
		})
	}
}

// floodEvents sends n events from several goroutines, faster than a stream drains them.
func floodEvents(n int, send func()) {
	var senders sync.WaitGroup
	var sent atomic.Int64
	for range 4 {
		senders.Go(func() {
			for sent.Add(1) <= int64(n) {
				send()
			}
		})
	}
	senders.Wait()
}

// startTopicStream runs the handler for topics until the test ends.
func startTopicStream(t *testing.T, topics string) (*ApiHandler, *collectingStreamWriter, <-chan struct{}) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	a := &ApiHandler{emitters: beaconevents.NewEventEmitter(), beaconChainCfg: &cfg}
	w := &collectingStreamWriter{header: http.Header{}}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/events?topics="+topics, nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		a.EventSourceGetV1Events(w, req)
	}()
	return a, w, done
}

// A stream is closed only for a dropped event of a topic it asked for. A block stream does not ask for head, a head
// stream does not ask for head_v2 and an execution_payload stream does not ask for execution_payload_available, so a
// flood of those must not close them.
func TestEventStreamStaysOpenWhenAnUnrequestedEventIsDropped(t *testing.T) {
	for _, tc := range []struct {
		topics string
		flood  func(*beaconevents.EventEmitter)
		own    func(*beaconevents.EventEmitter) int
		topic  beaconevents.EventTopic
	}{
		{
			topics: "block",
			flood:  func(e *beaconevents.EventEmitter) { e.State().TrySendHead(&beaconevents.HeadData{}) },
			own:    func(e *beaconevents.EventEmitter) int { return e.State().SendBlock(&beaconevents.BlockData{}) },
			topic:  beaconevents.StateBlock,
		},
		{
			topics: "head",
			flood:  func(e *beaconevents.EventEmitter) { e.State().SendHeadV2(&beaconevents.HeadV2Data{}) },
			own:    func(e *beaconevents.EventEmitter) int { return e.State().SendHead(&beaconevents.HeadData{}) },
			topic:  beaconevents.StateHead,
		},
		{
			topics: "execution_payload",
			flood: func(e *beaconevents.EventEmitter) {
				e.Operation().SendExecutionPayloadAvailable(&beaconevents.ExecutionPayloadAvailableData{})
			},
			own: func(e *beaconevents.EventEmitter) int {
				return e.Operation().SendExecutionPayload(&beaconevents.ExecutionPayloadData{})
			},
			topic: beaconevents.OpExecutionPayload,
		},
	} {
		t.Run(tc.topics, func(t *testing.T) {
			a, w, done := startTopicStream(t, tc.topics)
			require.Eventually(t, func() bool { return tc.own(a.emitters) > 0 }, 5*time.Second, 10*time.Millisecond, "event stream never subscribed")

			floodEvents(1<<14, func() { tc.flood(a.emitters) })
			require.Positive(t, tc.own(a.emitters))
			require.Eventually(t, func() bool { return w.count(tc.topic) >= 2 }, 5*time.Second, 10*time.Millisecond, "the stream stopped delivering its own topic")
			select {
			case <-done:
				t.Fatal("the stream was closed for an event it did not ask for")
			default:
			}
		})
	}
}

// A stream with head and another state topic takes head from the head feed only, not again from the state feed.
func TestEventStreamDeliversAHeadEventOnce(t *testing.T) {
	a, w, _ := startTopicStream(t, "head,block")
	require.Eventually(t, func() bool { return a.emitters.State().SendBlock(&beaconevents.BlockData{}) > 0 }, 5*time.Second, 10*time.Millisecond, "event stream never subscribed")
	require.Eventually(t, func() bool { return w.count(beaconevents.StateBlock) == 1 }, 5*time.Second, 10*time.Millisecond)

	require.Equal(t, 2, a.emitters.State().SendHead(&beaconevents.HeadData{}))
	require.Equal(t, 1, a.emitters.State().SendBlock(&beaconevents.BlockData{}))
	// The second block follows the state feed's copy of the head in the same buffer, so once it is written that copy
	// has been handled. The head's own buffer is read independently, so wait for it before counting.
	require.Eventually(t, func() bool { return w.count(beaconevents.StateBlock) == 2 }, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool { return w.count(beaconevents.StateHead) >= 1 }, 5*time.Second, 10*time.Millisecond)
	require.Never(t, func() bool { return w.count(beaconevents.StateHead) > 1 }, 200*time.Millisecond, 10*time.Millisecond)
}

// A stream with a payload topic and an operation topic takes the payload event from its own feed only, not again from
// the operation feed.
func TestEventStreamDeliversAPayloadEventOnce(t *testing.T) {
	for _, tc := range []struct {
		topic beaconevents.EventTopic
		send  func(*beaconevents.EventEmitter) int
	}{
		{
			topic: beaconevents.OpExecutionPayload,
			send: func(e *beaconevents.EventEmitter) int {
				return e.Operation().SendExecutionPayload(&beaconevents.ExecutionPayloadData{})
			},
		},
		{
			topic: beaconevents.OpExecutionPayloadAvailable,
			send: func(e *beaconevents.EventEmitter) int {
				return e.Operation().SendExecutionPayloadAvailable(&beaconevents.ExecutionPayloadAvailableData{})
			},
		},
	} {
		t.Run(string(tc.topic), func(t *testing.T) {
			a, w, _ := startTopicStream(t, string(tc.topic)+",data_column_sidecar")
			require.Eventually(t, func() bool {
				return a.emitters.Operation().SendDataColumnSidecar(&beaconevents.DataColumnSidecarData{}) > 0
			}, 5*time.Second, 10*time.Millisecond, "event stream never subscribed")
			require.Eventually(t, func() bool { return w.count(beaconevents.OpDataColumnSidecar) == 1 }, 5*time.Second, 10*time.Millisecond)

			require.Equal(t, 2, tc.send(a.emitters))
			require.Equal(t, 1, a.emitters.Operation().SendDataColumnSidecar(&beaconevents.DataColumnSidecarData{}))
			require.Eventually(t, func() bool { return w.count(beaconevents.OpDataColumnSidecar) == 2 }, 5*time.Second, 10*time.Millisecond)
			require.Eventually(t, func() bool { return w.count(tc.topic) >= 1 }, 5*time.Second, 10*time.Millisecond)
			require.Never(t, func() bool { return w.count(tc.topic) > 1 }, 200*time.Millisecond, 10*time.Millisecond)
		})
	}
}
