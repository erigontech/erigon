package validatorapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/gfx-labs/sse"
	"github.com/go-chi/chi/v5"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/log/v3"
)

func (v *ValidatorApiHandler) GetEthV1NodeSyncing(w http.ResponseWriter, r *http.Request) (any, error) {
	_, slot, err := v.FC.GetHead()
	if err != nil {
		return nil, err
	}

	realHead := utils.GetCurrentSlot(v.GenesisCfg.GenesisTime, v.BeaconChainCfg.SecondsPerSlot)

	isSyncing := realHead > slot

	syncDistance := 0
	if isSyncing {
		syncDistance = int(realHead) - int(slot)
	}

	elOffline := true
	if v.FC.Engine() != nil {
		val, err := v.FC.Engine().Ready()
		if err == nil {
			elOffline = !val
		}
	}

	return map[string]any{
		"data": map[string]any{
			"head_slot":     strconv.FormatUint(slot, 10),
			"sync_distance": syncDistance,
			"is_syncing":    isSyncing,
			"el_offline":    elOffline,
			// TODO: figure out how to populat this field
			"is_optimistic": true,
		}}, nil
}

func (v *ValidatorApiHandler) GetEthV1ConfigSpec(w http.ResponseWriter, r *http.Request) (*clparams.BeaconChainConfig, error) {
	if v.BeaconChainCfg == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("beacon config not found"))
	}
	return v.BeaconChainCfg, nil
}

func (v *ValidatorApiHandler) GetEthV1EthNodeSyncing(w http.ResponseWriter, r *http.Request) (any, error) {
	// TODO: populate this map
	o := map[string]any{
		"data": map[string]any{},
	}
	return o, nil
}
func (v *ValidatorApiHandler) GetEthV3ValidatorBlocksSlot(w http.ResponseWriter, r *http.Request) (any, error) {
	// TODO: populate this map
	o := map[string]any{
		"data": map[string]any{},
	}

	slotString := chi.URLParam(r, "slot")
	slot, err := strconv.ParseUint(slotString, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("fail to parse slot: %w", err)
	}
	randaoRevealString := r.URL.Query().Get("randao_reveal")
	randaoReveal, err := hexutil.Decode(randaoRevealString)
	if err != nil {
		return nil, fmt.Errorf("fail to parse randao_reveal: %w", err)
	}
	graffitiString := r.URL.Query().Get("randao_reveal")
	if graffitiString == "" {
		graffitiString = "0x"
	}
	graffiti, err := hexutil.Decode(graffitiString)
	if err != nil {
		return nil, fmt.Errorf("fail to parse graffiti: %w", err)
	}
	skip_randao_verification := r.URL.Query().Has("skip_randao_verification")
	//if skip_randao_verification {
	//  if isInfinity(randaoReveal) {
	//   return nil, beaconhttp.NewEndpointError(400, "randao reveal must be set to infinity if skip randao verification is set")
	//  }
	//}
	_, _, _, _ = slot, graffiti, randaoReveal, skip_randao_verification
	return o, nil
}

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

func (v *ValidatorApiHandler) EventSourceGetV1Events(w http.ResponseWriter, r *http.Request) (any, error) {
	sink, err := sse.DefaultUpgrader.Upgrade(w, r)
	if err != nil {
		return nil, fmt.Errorf("failed to upgrade: %s", err)
	}
	topics := r.URL.Query()["topics"]
	for _, v := range topics {
		if _, ok := validTopics[v]; !ok {
			return nil, fmt.Errorf("Invalid Topic: %s", v)
		}
	}
	var mu sync.Mutex
	closer, err := v.Emitters.Subscribe(topics, func(topic string, item any) {
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
		return nil, beaconhttp.NewEndpointError(400, err)
	}
	defer closer()
	select {
	case <-r.Context().Done():
		return nil, nil
	}
}
