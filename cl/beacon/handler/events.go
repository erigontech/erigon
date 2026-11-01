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
	"github.com/erigontech/erigon/common/log/v3"
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

var validTopics = map[event.EventTopic]struct{}{
	// operation events
	event.OpAttestation:               {},
	event.OpSingleAttestation:         {},
	event.OpAttesterSlashing:          {},
	event.OpBlobSidecar:               {},
	event.OpDataColumnSidecar:         {},
	event.OpBlsToExecution:            {},
	event.OpContributionProof:         {},
	event.OpProposerSlashing:          {},
	event.OpVoluntaryExit:             {},
	event.OpPayloadAttestationMessage: {},
	event.OpExecutionPayloadBid:       {},
	event.OpExecutionPayloadAvailable: {},
	event.OpExecutionPayload:          {},
	event.OpExecutionPayloadGossip:    {},
	event.OpProposerPreferences:       {},
	// state events
	event.StateBlock:                       {},
	event.StateBlockGossip:                 {},
	event.StateChainReorg:                  {},
	event.StateLightClientFinalityUpdate:   {},
	event.StateFinalizedCheckpoint:         {},
	event.StateHead:                        {},
	event.StateHeadV2:                      {},
	event.StateLightClientOptimisticUpdate: {},
	event.StatePayloadAttributes:           {},
}

func (a *ApiHandler) EventSourceGetV1Events(w http.ResponseWriter, r *http.Request) {
	if _, ok := w.(http.Flusher); !ok {
		beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("streaming unsupported")).WriteTo(w)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	topics := r.URL.Query()["topics"]
	if len(topics) > 0 {
		topics = strings.Split(topics[0], ",")
	}
	subscribeTopics := mapset.NewSet[event.EventTopic]()
	for _, v := range topics {
		topic := event.EventTopic(v)
		if _, ok := validTopics[topic]; !ok {
			beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("invalid Topic: %s", v)).WriteTo(w)
			return
		}
		subscribeTopics.Add(topic)
	}
	log.Info("Subscribed to event stream topics", "topics", subscribeTopics)
	if subscribeTopics.Contains(event.StatePayloadAttributes) {
		release := a.emitters.TrackPayloadAttributesSubscriber()
		defer release()
	}

	eventCh := make(chan *event.EventStream, 128)
	opSub := a.emitters.Operation().Subscribe(eventCh)
	stateSub := a.emitters.State().Subscribe(eventCh)
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
		opSub.Unsubscribe()
		stateSub.Unsubscribe()
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
		return false
	}

	ticker := time.NewTicker(time.Duration(a.beaconChainCfg.SecondsPerSlot) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case e := <-eventCh:
			if !subscribeTopics.Contains(e.Event) {
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
		case <-ticker.C:
			// keep connection alive
			if !enqueue([]byte(":\n\n")) {
				return
			}
		case <-writerDone:
			return
		case err := <-stateSub.Err():
			log.Warn("event error", "err", err)
			return
		case err := <-opSub.Err():
			log.Warn("event error", "err", err)
			return
		case <-r.Context().Done():
			log.Info("Client disconnected from event stream")
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
