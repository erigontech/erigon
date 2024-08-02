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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/erigontech/erigon-lib/log/v3"
	event "github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/gfx-labs/sse"
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
	sink, err := sse.DefaultUpgrader.Upgrade(w, r)
	if err != nil {
		http.Error(w, "failed to upgrade", http.StatusInternalServerError)
		return
	}
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
	eventCh := make(chan *event.EventStream, 128)
	opSub := a.emitters.Operation().Subscribe(eventCh)
	defer opSub.Unsubscribe()

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
			buf := &bytes.Buffer{}
			if err := json.NewEncoder(buf).Encode(event.Data); err != nil {
				log.Warn("failed to encode data", "err", err, "topic", event.Event)
				continue
			}
			if err := sink.Encode(&sse.Event{
				Event: []byte(event.Event),
				Data:  buf,
			}); err != nil {
				log.Warn("failed to encode event", "err", err)
			}
		case <-ticker.C:
			// keep connection alive
			if _, err := w.Write([]byte(":\n\n")); err != nil {
				log.Warn("failed to write keep alive", "err", err)
				continue
			}
			w.(http.Flusher).Flush()
		case err := <-opSub.Err():
			http.Error(w, fmt.Sprintf("event error %v", err), http.StatusInternalServerError)
			return
		case <-r.Context().Done():
			return
		}
	}
}
