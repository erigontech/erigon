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
	"slices"
	"strconv"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
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

	// Validate every request before subscribing to any subnet.
	syncnets := make([][]uint64, len(req))
	for i, subRequest := range req {
		if subRequest.DEBUG {
			for subnet := uint64(0); subnet < a.beaconChainCfg.SyncCommitteeSubnetCount; subnet++ {
				syncnets[i] = append(syncnets[i], subnet)
			}
			continue
		}
		if subRequest.SyncCommitteeIndicies == nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("request %d: sync_committee_indices is required", i)).WriteTo(w)
			return
		}
		sn, err := a.syncCommitteeSubnets(subRequest.SyncCommitteeIndicies)
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("request %d: %w", i, err)).WriteTo(w)
			return
		}
		syncnets[i] = sn
	}
	for _, subRequest := range req {
		a.validatorParams.AddAttachedValidator(subRequest.ValidatorIndex)
	}

	for i, subRequest := range req {
		expiry := a.ethClock.GetSlotTime(subRequest.UntilEpoch * a.beaconChainCfg.SlotsPerEpoch)
		if expiry.Before(time.Now()) {
			continue
		}
		for _, subnet := range syncnets[i] {
			if err := a.gossipManager.SubscribeWithExpiry(gossip.TopicNameSyncCommittee(int(subnet)), expiry); err != nil {
				beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
				return
			}
		}
	}
	w.WriteHeader(http.StatusOK)
}

// syncCommitteeSubnets maps the validator's positions in the sync committee to their subnets, each once.
func (a *ApiHandler) syncCommitteeSubnets(indices []string) ([]uint64, error) {
	subcommitteeSize := a.beaconChainCfg.SyncCommitteeSize / a.beaconChainCfg.SyncCommitteeSubnetCount
	var syncnets []uint64
	for _, s := range indices {
		index, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid sync committee index %q: %w", s, err)
		}
		if index >= a.beaconChainCfg.SyncCommitteeSize {
			return nil, fmt.Errorf("sync committee index %d is out of range", index)
		}
		if subnet := index / subcommitteeSize; !slices.Contains(syncnets, subnet) {
			syncnets = append(syncnets, subnet)
		}
	}
	return syncnets, nil
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
		a.validatorParams.AddAttachedValidator(sub.ValidatorIndex)
	}
	for _, sub := range req {
		if err := a.committeeSub.AddAttestationSubscription(context.Background(), sub); err != nil {
			if !errors.Is(err, synced_data.ErrNotSynced) {
				log.Error("failed to add attestation subscription", "err", err)
			}
			beaconhttp.WrapEndpointError(err).WriteTo(w)
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
