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
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/erigontech/erigon-lib/log/v3"
	event "github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
)

var validTopics = map[event.EventTopic]struct{}{
	// operation events
	event.OpAttestation:       {},
	event.OpAttesterSlashing:  {},
	event.OpBlobSidecar:       {},
	event.OpDataColumnSidecar: {},
	event.OpBlsToExecution:    {},
	event.OpContributionProof: {},
	event.OpProposerSlashing:  {},
	event.OpVoluntaryExit:     {},
	// state events
	event.StateBlock:                       {},
	event.StateBlockGossip:                 {},
	event.StateChainReorg:                  {},
	event.StateLightClientFinalityUpdate:   {},
	event.StateFinalizedCheckpoint:         {},
	event.StateHead:                        {},
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

	eventCh := make(chan *event.EventStream, 128)
	opSub := a.emitters.Operation().Subscribe(eventCh)
	stateSub := a.emitters.State().Subscribe(eventCh)
	defer opSub.Unsubscribe()
	defer stateSub.Unsubscribe()

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
			if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.Event, string(buf)); err != nil {
				log.Warn("failed to write event", "err", err)
				continue
			}
			w.(http.Flusher).Flush()
		case <-ticker.C:
			// keep connection alive
			if _, err := w.Write([]byte(":\n\n")); err != nil {
				log.Warn("failed to write keep alive", "err", err)
				continue
			}
			w.(http.Flusher).Flush()
		case err := <-stateSub.Err():
			log.Warn("event error", "err", err)
			beaconhttp.NewEndpointError(http.StatusInternalServerError, fmt.Errorf("event error %v", err)).WriteTo(w)
		case err := <-opSub.Err():
			log.Warn("event error", "err", err)
			beaconhttp.NewEndpointError(http.StatusInternalServerError, fmt.Errorf("event error %v", err)).WriteTo(w)
			return
		case <-r.Context().Done():
			log.Info("Client disconnected from event stream")
			return
		}
	}
}
