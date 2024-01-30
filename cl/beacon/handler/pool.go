package handler

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/gossip"
)

func (a *ApiHandler) GetEthV1BeaconPoolVoluntaryExits(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	return newBeaconResponse(a.operationsPool.VoluntaryExistsPool.Raw()), nil
}

func (a *ApiHandler) GetEthV1BeaconPoolAttesterSlashings(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	fmt.Println("GetEthV1BeaconPoolAttesterSlashings", a.operationsPool.AttesterSlashingsPool.Raw())
	return newBeaconResponse(a.operationsPool.AttesterSlashingsPool.Raw()), nil
}

func (a *ApiHandler) GetEthV1BeaconPoolProposerSlashings(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	return newBeaconResponse(a.operationsPool.ProposerSlashingsPool.Raw()), nil
}

func (a *ApiHandler) GetEthV1BeaconPoolBLSExecutionChanges(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	return newBeaconResponse(a.operationsPool.BLSToExecutionChangesPool.Raw()), nil
}

func (a *ApiHandler) GetEthV1BeaconPoolAttestations(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	slot, err := beaconhttp.Uint64FromQueryParams(r, "slot")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	committeeIndex, err := beaconhttp.Uint64FromQueryParams(r, "committee_index")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	atts := a.operationsPool.AttestationsPool.Raw()
	if slot == nil && committeeIndex == nil {
		return newBeaconResponse(atts), nil
	}
	ret := make([]any, 0, len(atts))
	for i := range atts {
		if slot != nil && atts[i].AttestantionData().Slot() != *slot {
			continue
		}
		if committeeIndex != nil && atts[i].AttestantionData().ValidatorIndex() != *committeeIndex {
			continue
		}
		ret = append(ret, atts[i])
	}

	return newBeaconResponse(ret), nil
}

func (a *ApiHandler) PostEthV1BeaconPoolVoluntaryExits(w http.ResponseWriter, r *http.Request) {
	req := cltypes.SignedVoluntaryExit{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := a.forkchoiceStore.OnVoluntaryExit(&req, false); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Broadcast to gossip
	if a.sentinel != nil {
		encodedSSZ, err := req.EncodeSSZ(nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if _, err := a.sentinel.PublishGossip(r.Context(), &sentinel.GossipData{
			Data: encodedSSZ,
			Name: gossip.TopicNameVoluntaryExit,
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		a.operationsPool.VoluntaryExistsPool.Insert(req.VoluntaryExit.ValidatorIndex, &req)
	}
	// Only write 200
	w.WriteHeader(http.StatusOK)
}

func (a *ApiHandler) PostEthV1BeaconPoolAttesterSlashings(w http.ResponseWriter, r *http.Request) {
	req := cltypes.NewAttesterSlashing()
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := a.forkchoiceStore.OnAttesterSlashing(req, false); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Broadcast to gossip
	if a.sentinel != nil {
		encodedSSZ, err := req.EncodeSSZ(nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if _, err := a.sentinel.PublishGossip(r.Context(), &sentinel.GossipData{
			Data: encodedSSZ,
			Name: gossip.TopicNameAttesterSlashing,
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	// Only write 200
	w.WriteHeader(http.StatusOK)
}

func (a *ApiHandler) PostEthV1BeaconPoolProposerSlashings(w http.ResponseWriter, r *http.Request) {
	req := cltypes.ProposerSlashing{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if err := a.forkchoiceStore.OnProposerSlashing(&req, false); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Broadcast to gossip
	if a.sentinel != nil {
		encodedSSZ, err := req.EncodeSSZ(nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if _, err := a.sentinel.PublishGossip(r.Context(), &sentinel.GossipData{
			Data: encodedSSZ,
			Name: gossip.TopicNameProposerSlashing,
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	// Only write 200
	w.WriteHeader(http.StatusOK)
}

type poolingFailure struct {
	Index   int    `json:"index"`
	Message string `json:"message"`
}

type poolingError struct {
	Code     int              `json:"code"`
	Message  string           `json:"message"`
	Failures []poolingFailure `json:"failures"`
}

func (a *ApiHandler) PostEthV1BeaconPoolBlsToExecutionChanges(w http.ResponseWriter, r *http.Request) {
	req := []*cltypes.SignedBLSToExecutionChange{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	failures := []poolingFailure{}
	for _, v := range req {
		if err := a.forkchoiceStore.OnBlsToExecutionChange(v, false); err != nil {
			failures = append(failures, poolingFailure{Index: len(failures), Message: err.Error()})
			continue
		}
		// Broadcast to gossip
		if a.sentinel != nil {
			encodedSSZ, err := v.EncodeSSZ(nil)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if _, err := a.sentinel.PublishGossip(r.Context(), &sentinel.GossipData{
				Data: encodedSSZ,
				Name: gossip.TopicNameBlsToExecutionChange,
			}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}

	if len(failures) > 0 {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(poolingError{Code: http.StatusBadRequest, Message: "some failures", Failures: failures})
		return
	}
	// Only write 200
	w.WriteHeader(http.StatusOK)
}

func (a *ApiHandler) PostEthV1ValidatorAggregatesAndProof(w http.ResponseWriter, r *http.Request) {
	req := []*cltypes.SignedAggregateAndProof{}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	failures := []poolingFailure{}
	for _, v := range req {
		if err := a.forkchoiceStore.OnAggregateAndProof(v, false); err != nil {
			failures = append(failures, poolingFailure{Index: len(failures), Message: err.Error()})
			continue
		}
		// Broadcast to gossip
		if a.sentinel != nil {
			encodedSSZ, err := v.EncodeSSZ(nil)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if _, err := a.sentinel.PublishGossip(r.Context(), &sentinel.GossipData{
				Data: encodedSSZ,
				Name: gossip.TopicNameBeaconAggregateAndProof,
			}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}
}
