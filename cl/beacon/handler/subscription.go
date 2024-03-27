package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/gossip"
	"github.com/ledgerwatch/erigon/cl/phase1/network"
	"github.com/ledgerwatch/erigon/cl/utils"
)

type ValidatorSyncCommitteeSubscriptionsRequest struct {
	ValidatorIndex        uint64   `json:"validator_index,string"`
	SyncCommitteeIndicies []string `json:"sync_committee_indices"`
	UntilEpoch            uint64   `json:"until_epoch,string"`

	DEBUG bool `json:"DEBUG,omitempty"`
}

func parseStringifiedSyncCommitteeIndicies(syncCommitteeIndicies []string) ([]uint64, error) {
	ret := make([]uint64, len(syncCommitteeIndicies))
	for i, idx := range syncCommitteeIndicies {
		var err error
		ret[i], err = strconv.ParseUint(idx, 10, 64)
		if err != nil {
			return nil, err
		}
	}
	return ret, nil
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
	// process each sub request
	for _, subRequest := range req {
		expiry := utils.GetSlotTime(headState.GenesisTime(), a.beaconChainCfg.SecondsPerSlot, subRequest.UntilEpoch*uint64(a.beaconChainCfg.SlotsPerEpoch))
		if expiry.Before(time.Now()) {
			continue
		}

		syncCommitteeIndicies, err := parseStringifiedSyncCommitteeIndicies(subRequest.SyncCommitteeIndicies)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var subnets []uint64
		if subRequest.DEBUG {
			for i := 0; i < int(a.beaconChainCfg.SyncCommitteeSubnetCount); i++ {
				subnets = append(subnets, uint64(i))
			}
		} else {
			subnets, err = network.ComputeSubnetsForSyncCommittee(headState, syncCommitteeIndicies, subRequest.ValidatorIndex)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

		// subscribe to subnets
		for _, subnet := range subnets {
			if _, err := a.sentinel.SetSubscribeExpiry(r.Context(), &sentinel.RequestSubscribeExpiry{
				Topic:          fmt.Sprintf(gossip.TopicNamePrefixSyncCommittee, subnet),
				ExpiryUnixSecs: uint64(expiry.Unix()),
			}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}
	w.WriteHeader(http.StatusOK)
}
