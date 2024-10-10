package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/gossip"
	"github.com/ledgerwatch/erigon/cl/phase1/network/subnets"
	"github.com/ledgerwatch/log/v3"
)

type ValidatorSyncCommitteeSubscriptionsRequest struct {
	ValidatorIndex        uint64   `json:"validator_index,string"`
	SyncCommitteeIndicies []string `json:"sync_committee_indices"`
	UntilEpoch            uint64   `json:"until_epoch,string"`

	DEBUG bool `json:"DEBUG,omitempty"`
}

func (a *ApiHandler) PostEthV1ValidatorSyncCommitteeSubscriptions(w http.ResponseWriter, r *http.Request) {
	var req []ValidatorSyncCommitteeSubscriptionsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(req) == 0 {
		w.WriteHeader(http.StatusOK)
		return
	}
	headState := a.syncedData.HeadState()
	if headState == nil {
		http.Error(w, "head state not available", http.StatusServiceUnavailable)
		return
	}
	var err error
	// process each sub request
	for _, subRequest := range req {
		expiry := a.ethClock.GetSlotTime(subRequest.UntilEpoch * a.beaconChainCfg.SlotsPerEpoch)
		if expiry.Before(time.Now()) {
			continue
		}

		var syncnets []uint64
		if subRequest.DEBUG {
			for i := 0; i < int(a.beaconChainCfg.SyncCommitteeSubnetCount); i++ {
				syncnets = append(syncnets, uint64(i))
			}
		} else {
			syncnets, err = subnets.ComputeSubnetsForSyncCommittee(headState, subRequest.ValidatorIndex)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

		// subscribe to subnets
		for _, subnet := range syncnets {
			if _, err := a.sentinel.SetSubscribeExpiry(r.Context(), &sentinel.RequestSubscribeExpiry{
				Topic:          gossip.TopicNameSyncCommittee(int(subnet)),
				ExpiryUnixSecs: uint64(expiry.Unix()),
			}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (a *ApiHandler) PostEthV1ValidatorBeaconCommitteeSubscription(w http.ResponseWriter, r *http.Request) {
	req := []*cltypes.BeaconCommitteeSubscription{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Error("failed to decode request", "err", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if len(req) == 0 {
		http.Error(w, "empty request", http.StatusBadRequest)
		return
	}
	for _, sub := range req {
		if err := a.committeeSub.AddAttestationSubscription(context.Background(), sub); err != nil {
			log.Error("failed to add attestation subscription", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

func parseSyncCommitteeContribution(r *http.Request) (slot, subcommitteeIndex uint64, beaconBlockRoot common.Hash, err error) {
	slotStr := r.URL.Query().Get("slot")
	subCommitteeIndexStr := r.URL.Query().Get("subcommittee_index")
	blockRootStr := r.URL.Query().Get("beacon_block_root")
	// check if they required fields are present
	if slotStr == "" {
		err = fmt.Errorf("slot as query param is required")
		return
	}
	if subCommitteeIndexStr == "" {
		err = fmt.Errorf("subcommittee_index as query param is required")
		return
	}
	if blockRootStr == "" {
		err = fmt.Errorf("beacon_block_root as query param is required")
		return
	}
	slot, err = strconv.ParseUint(slotStr, 10, 64)
	if err != nil {
		err = fmt.Errorf("could not parse slot: %w", err)
		return
	}
	subcommitteeIndex, err = strconv.ParseUint(subCommitteeIndexStr, 10, 64)
	if err != nil {
		err = fmt.Errorf("could not parse subcommittee_index: %w", err)
		return
	}
	beaconBlockRoot = common.HexToHash(blockRootStr)
	return
}

func (a *ApiHandler) GetEthV1ValidatorSyncCommitteeContribution(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	slot, subCommitteeIndex, beaconBlockRoot, err := parseSyncCommitteeContribution(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	return newBeaconResponse(a.syncMessagePool.GetSyncContribution(slot, subCommitteeIndex, beaconBlockRoot)), nil
}
