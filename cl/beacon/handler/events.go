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

package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	event "github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	ethevent "github.com/erigontech/erigon/common/event"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

const (
	eventStreamWriteQueueSize  = 1024
	eventStreamWriteQueueBytes = 64 << 20
	eventStreamWriteTimeout    = 30 * time.Second
)

type eventStreamQueue struct {
	msgs   chan []byte
	queued atomic.Int64
}

func newEventStreamQueue() *eventStreamQueue {
	return &eventStreamQueue{msgs: make(chan []byte, eventStreamWriteQueueSize)}
}

// push is called from a single goroutine. It never blocks and rejects a message that exceeds the
// message count or byte budget; an empty queue always accepts one message.
func (q *eventStreamQueue) push(msg []byte) bool {
	size := int64(len(msg))
	if queued := q.queued.Load(); queued > 0 && queued+size > eventStreamWriteQueueBytes {
		return false
	}
	q.queued.Add(size)
	select {
	case q.msgs <- msg:
		return true
	default:
		q.queued.Add(-size)
		return false
	}
}

func (q *eventStreamQueue) release(size int) {
	q.queued.Add(-int64(size))
}

type eventFeed int

const (
	operationFeed eventFeed = iota
	stateFeed
)

// eventStreamsClosed counts the streams the handler closes because the client does not keep up.
var eventStreamsClosed = metrics.GetOrCreateCounterVec("caplin_event_streams_closed_total", []string{"reason"})

// validTopics maps each topic to the emitter feed that carries it.
var validTopics = map[event.EventTopic]eventFeed{
	event.OpAttestation:                    operationFeed,
	event.OpSingleAttestation:              operationFeed,
	event.OpAttesterSlashing:               operationFeed,
	event.OpBlobSidecar:                    operationFeed,
	event.OpDataColumnSidecar:              operationFeed,
	event.OpBlsToExecution:                 operationFeed,
	event.OpContributionProof:              operationFeed,
	event.OpProposerSlashing:               operationFeed,
	event.OpVoluntaryExit:                  operationFeed,
	event.OpPayloadAttestationMessage:      operationFeed,
	event.OpExecutionPayloadBid:            operationFeed,
	event.OpExecutionPayloadAvailable:      operationFeed,
	event.OpExecutionPayload:               operationFeed,
	event.OpExecutionPayloadGossip:         operationFeed,
	event.OpProposerPreferences:            operationFeed,
	event.StateBlock:                       stateFeed,
	event.StateBlockGossip:                 stateFeed,
	event.StateChainReorg:                  stateFeed,
	event.StateLightClientFinalityUpdate:   stateFeed,
	event.StateFinalizedCheckpoint:         stateFeed,
	event.StateHead:                        stateFeed,
	event.StateHeadV2:                      stateFeed,
	event.StateLightClientOptimisticUpdate: stateFeed,
	event.StatePayloadAttributes:           stateFeed,
}

func (a *ApiHandler) EventSourceGetV1Events(w http.ResponseWriter, r *http.Request) {
	if _, ok := w.(http.Flusher); !ok {
		beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("streaming unsupported")).WriteTo(w)
		return
	}

	subscribeTopics := mapset.NewSet[event.EventTopic]()
	for v := range strings.SplitSeq(strings.Join(r.URL.Query()["topics"], ","), ",") {
		topic := event.EventTopic(v)
		if _, ok := validTopics[topic]; !ok {
			beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("invalid Topic: %s", v)).WriteTo(w)
			return
		}
		subscribeTopics.Add(topic)
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	log.Debug("Subscribed to event stream topics", "topics", subscribeTopics)
	if subscribeTopics.Contains(event.StatePayloadAttributes) {
		release := a.emitters.TrackPayloadAttributesSubscriber()
		defer release()
	}

	sub := subscribeEventStream(a.emitters, subscribeTopics)
	// Emitters block until every subscriber takes an event, so network writes must not happen on this goroutine.
	queue := newEventStreamQueue()
	stop := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		if err := writeEventStream(w, queue, stop); err != nil {
			log.Warn("failed to write event", "err", err, "remote", r.RemoteAddr)
		}
	}()
	defer func() {
		sub.unsubscribe()
		close(stop)
		// Abort a write that is in flight; stop is closed first so the writer cannot re-arm the deadline and continue.
		_ = setWriteDeadline(http.NewResponseController(w), time.Now())
		<-writerDone
		// The writer may have cleared or re-armed the deadline after the abort above.
		_ = setWriteDeadline(http.NewResponseController(w), time.Now())
	}()
	enqueue := func(msg []byte) bool {
		if queue.push(msg) {
			return true
		}
		log.Warn("event stream client is not keeping up, closing stream", "remote", r.RemoteAddr, "topics", subscribeTopics)
		eventStreamsClosed.WithLabelValues("write_queue_full").Inc()
		return false
	}

	missedHead := func(err error) {
		log.Warn("event stream missed a head event, closing stream", "err", err, "remote", r.RemoteAddr, "topics", subscribeTopics)
		eventStreamsClosed.WithLabelValues("missed_head").Inc()
	}

	ticker := time.NewTicker(time.Duration(a.beaconChainCfg.SecondsPerSlot) * time.Second)
	defer ticker.Stop()

	for {
		var e *event.EventStream
		select {
		case e = <-sub.state:
			if isHeadTopic(e.Event) {
				continue // a stream gets head and head_v2 from their own feeds
			}
		case e = <-sub.op:
		case e = <-sub.head:
		case <-ticker.C:
			// keep connection alive
			if !enqueue([]byte(":\n\n")) {
				return
			}
		case <-writerDone:
			return
		case err := <-subErr(sub.headSub):
			missedHead(err)
			return
		case err := <-subErr(sub.headV2Sub):
			missedHead(err)
			return
		case err := <-subErr(sub.stateSub):
			log.Warn("event error", "err", err, "remote", r.RemoteAddr, "topics", subscribeTopics)
			return
		case err := <-subErr(sub.opSub):
			log.Warn("event error", "err", err, "remote", r.RemoteAddr, "topics", subscribeTopics)
			return
		case <-r.Context().Done():
			log.Debug("Client disconnected from event stream")
			return
		}
		if e == nil || !subscribeTopics.Contains(e.Event) {
			continue
		}
		if e.Data == nil {
			log.Warn("event data is nil", "event", e)
			continue
		}
		// marshal and send
		buf, err := json.Marshal(e.Data)
		if err != nil {
			log.Warn("failed to encode data", "err", err, "topic", e.Event)
			continue
		}
		if !enqueue(fmt.Appendf(nil, "event: %s\ndata: %s\n\n", e.Event, buf)) {
			return
		}
	}
}

func writeEventStream(w http.ResponseWriter, q *eventStreamQueue, stop <-chan struct{}) error {
	rc := http.NewResponseController(w)
	for {
		select {
		case <-stop:
			return nil
		case msg, ok := <-q.msgs:
			if !ok {
				return nil
			}
			if err := writeEvent(w, rc, msg, stop); err != nil {
				return err
			}
			q.release(len(msg))
		}
	}
}

func writeEvent(w http.ResponseWriter, rc *http.ResponseController, msg []byte, stop <-chan struct{}) error {
	if err := setWriteDeadline(rc, time.Now().Add(eventStreamWriteTimeout)); err != nil {
		return err
	}
	select {
	case <-stop:
		return nil
	default:
	}
	if _, err := w.Write(msg); err != nil {
		return err
	}
	if err := rc.Flush(); err != nil {
		return err
	}
	// A passed deadline cannot be extended, so leaving it armed would cut a healthy stream that is idle between keepalives.
	return setWriteDeadline(rc, time.Time{})
}

func setWriteDeadline(rc *http.ResponseController, deadline time.Time) error {
	if err := rc.SetWriteDeadline(deadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return err
	}
	return nil
}

// eventStreamFeedBuffer is the buffer of each feed subscription of a stream.
const eventStreamFeedBuffer = 128

// eventStreamSubscription subscribes a stream only to the feeds that carry its topics, each into its own buffer. State
// events must never share a buffer with operation events: the operation feed carries thousands of gossip events per
// slot. head and head_v2 come from feeds of their own: they are sent without waiting for subscribers, and a stream that
// misses one it asked for is closed, so the client reconnects. A missed head or head_v2 it did not ask for must not
// close it. A channel and subscription of a feed the stream does not need stay nil, and a nil channel is never ready in
// a select.
type eventStreamSubscription struct {
	state, op, head                     chan *event.EventStream
	stateSub, opSub, headSub, headV2Sub ethevent.Subscription
}

func isHeadTopic(topic event.EventTopic) bool {
	return topic == event.StateHead || topic == event.StateHeadV2
}

func subscribeEventStream(emitters *event.EventEmitter, topics mapset.Set[event.EventTopic]) *eventStreamSubscription {
	s := &eventStreamSubscription{}
	for topic := range topics.Iter() {
		switch {
		case isHeadTopic(topic):
			if s.head == nil {
				s.head = make(chan *event.EventStream, eventStreamFeedBuffer)
			}
			if topic == event.StateHead {
				s.headSub = emitters.State().SubscribeHead(s.head)
			} else {
				s.headV2Sub = emitters.State().SubscribeHeadV2(s.head)
			}
		case validTopics[topic] == stateFeed:
			if s.state == nil {
				s.state = make(chan *event.EventStream, eventStreamFeedBuffer)
				s.stateSub = emitters.State().Subscribe(s.state)
			}
		case validTopics[topic] == operationFeed:
			if s.op == nil {
				s.op = make(chan *event.EventStream, eventStreamFeedBuffer)
				s.opSub = emitters.Operation().Subscribe(s.op)
			}
		}
	}
	return s
}

func subErr(sub ethevent.Subscription) <-chan error {
	if sub == nil {
		return nil
	}
	return sub.Err()
}

func (s *eventStreamSubscription) unsubscribe() {
	for _, sub := range []ethevent.Subscription{s.stateSub, s.opSub, s.headSub, s.headV2Sub} {
		if sub != nil {
			sub.Unsubscribe()
		}
	}
}
