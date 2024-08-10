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
	"fmt"
	"net/http"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/erigontech/erigon-lib/log/v3"
	event "github.com/erigontech/erigon/cl/beacon/beaconevents"
)

var validTopics = map[event.EventTopic]struct{}{
	// operation events
	event.OpAttestation:       {},
	event.OpAttesterSlashing:  {},
	event.OpBlobSidecar:       {},
	event.OpBlsToExecution:    {},
	event.OpContributionProof: {},
	event.OpProposerSlashing:  {},
	event.OpVoluntaryExit:     {},
	// state events
	event.StateBlock:               {},
	event.StateBlockGossip:         {},
	event.StateChainReorg:          {},
	event.StateFinalityUpdate:      {},
	event.StateFinalizedCheckpoint: {},
	event.StateHead:                {},
	event.StateOptimisticUpdate:    {},
	event.StatePayloadAttributes:   {},
}

func (a *ApiHandler) EventSourceGetV1Events(w http.ResponseWriter, r *http.Request) {
	if _, ok := w.(http.Flusher); !ok {
		http.Error(w, "streaming unsupported", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Connection", "keep-alive")

	topics := r.URL.Query()["topics"]
	subscribeTopics := mapset.NewSet[event.EventTopic]()
	for _, v := range topics {
		topic := event.EventTopic(v)
		if _, ok := validTopics[topic]; !ok {
			http.Error(w, "invalid Topic: "+v, http.StatusBadRequest)
			return
		}
		subscribeTopics.Add(topic)
	}
	log.Info("Subscribed to topics", "topics", subscribeTopics)

	eventCh := make(chan *event.EventStream, 128)
	opSub := a.emitters.Operation().Subscribe(eventCh)
	stateSub := a.emitters.State().Subscribe(eventCh)
	defer opSub.Unsubscribe()
	defer stateSub.Unsubscribe()

	ticker := time.NewTicker(time.Duration(a.beaconChainCfg.SecondsPerSlot) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case event := <-eventCh:
			if !subscribeTopics.Contains(event.Event) {
				continue
			}
			if event.Data == nil {
				log.Warn("event data is nil", "event", event)
				continue
			}
			// marshal and send
			buf, err := json.Marshal(event.Data)
			if err != nil {
				log.Warn("failed to encode data", "err", err, "topic", event.Event)
				continue
			}
			if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event.Event, string(buf)); err != nil {
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
			http.Error(w, fmt.Sprintf("event error %v", err), http.StatusInternalServerError)
		case err := <-opSub.Err():
			log.Warn("event error", "err", err)
			http.Error(w, fmt.Sprintf("event error %v", err), http.StatusInternalServerError)
			return
		case <-r.Context().Done():
			log.Info("Client disconnected")
			return
		}
	}
}
