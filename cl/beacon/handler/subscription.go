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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/erigontech/erigon-lib/common"
	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/network/subnets"
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
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}
	if len(req) == 0 {
		w.WriteHeader(http.StatusOK)
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
			// headState, cn := a.syncedData.HeadState()
			// defer cn()
			// if headState == nil {
			// 	http.Error(w, "head state not available", http.StatusServiceUnavailable)
			// 	return
			// }
			if err := a.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
				syncnets, err = subnets.ComputeSubnetsForSyncCommittee(headState, subRequest.ValidatorIndex)
				if err != nil {
					return err
				}
				return nil
			}); err != nil {
				beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
				return
			}
			//cn()
		}

		// subscribe to subnets
		for _, subnet := range syncnets {
			if _, err := a.sentinel.SetSubscribeExpiry(r.Context(), &sentinel.RequestSubscribeExpiry{
				Topic:          gossip.TopicNameSyncCommittee(int(subnet)),
				ExpiryUnixSecs: uint64(expiry.Unix()),
			}); err != nil {
				beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
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
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}
	if len(req) == 0 {
		beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("empty request")).WriteTo(w)
		return
	}
	for _, sub := range req {
		if err := a.committeeSub.AddAttestationSubscription(context.Background(), sub); err != nil {
			log.Error("failed to add attestation subscription", "err", err)
			beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
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
		err = errors.New("slot as query param is required")
		return
	}
	if subCommitteeIndexStr == "" {
		err = errors.New("subcommittee_index as query param is required")
		return
	}
	if blockRootStr == "" {
		err = errors.New("beacon_block_root as query param is required")
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
