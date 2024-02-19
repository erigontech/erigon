package handler

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/gfx-labs/sse"
	"github.com/ledgerwatch/log/v3"
)

var validTopics = map[string]struct{}{
	"head":                           {},
	"block":                          {},
	"attestation":                    {},
	"voluntary_exit":                 {},
	"bls_to_execution_change":        {},
	"finalized_checkpoint":           {},
	"chain_reorg":                    {},
	"contribution_and_proof":         {},
	"light_client_finality_update":   {},
	"light_client_optimistic_update": {},
	"payload_attributes":             {},
	"*":                              {},
}

func (a *ApiHandler) EventSourceGetV1Events(w http.ResponseWriter, r *http.Request) {
	sink, err := sse.DefaultUpgrader.Upgrade(w, r)
	if err != nil {
		http.Error(w, "failed to upgrade", http.StatusInternalServerError)
	}
	topics := r.URL.Query()["topics"]
	for _, v := range topics {
		if _, ok := validTopics[v]; !ok {
			http.Error(w, fmt.Sprintf("invalid Topic: %s", v), http.StatusBadRequest)
		}
	}
	var mu sync.Mutex
	closer, err := a.emitters.Subscribe(topics, func(topic string, item any) {
		buf := &bytes.Buffer{}
		err := json.NewEncoder(buf).Encode(item)
		if err != nil {
			// return early
			return
		}
		mu.Lock()
		err = sink.Encode(&sse.Event{
			Event: []byte(topic),
			Data:  buf,
		})
		mu.Unlock()
		if err != nil {
			log.Error("failed to encode data", "topic", topic, "err", err)
		}
		// OK to ignore this error. maybe should log it later?
	})
	if err != nil {
		http.Error(w, "failed to subscribe", http.StatusInternalServerError)
		return
	}
	defer closer()
	<-r.Context().Done()

}
