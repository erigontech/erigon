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
	"fmt"
	"net/http"
	"strconv"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

type committeeResponse struct {
	Index      uint64   `json:"index,string"`
	Slot       uint64   `json:"slot,string"`
	Validators []string `json:"validators"` // do string directly but it is still a base10 number
}

func (a *ApiHandler) getCommittees(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()

	epochReq, err := beaconhttp.Uint64FromQueryParams(r, "epoch")
	if err != nil {
		return nil, err
	}

	index, err := beaconhttp.Uint64FromQueryParams(r, "index")
	if err != nil {
		return nil, err
	}

	slotFilter, err := beaconhttp.Uint64FromQueryParams(r, "slot")
	if err != nil {
		return nil, err
	}

	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	blockId, err := beaconhttp.StateIdFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}

	blockRoot, httpStatus, err := a.blockRootFromStateId(ctx, tx, blockId)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(httpStatus, err)
	}

	isOptimistic := a.forkchoiceStore.IsRootOptimistic(blockRoot)
	slotPtr, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
	if err != nil {
		return nil, err
	}
	if slotPtr == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not read block slot: %x", blockRoot))
	}
	slot := *slotPtr
	epoch := slot / a.beaconChainCfg.SlotsPerEpoch
	if epochReq != nil {
		epoch = *epochReq
	}
	// check if the filter (if any) is in the epoch
	if slotFilter != nil && !(epoch*a.beaconChainCfg.SlotsPerEpoch <= *slotFilter && *slotFilter < (epoch+1)*a.beaconChainCfg.SlotsPerEpoch) {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("slot %d is not in epoch %d", *slotFilter, epoch))
	}
	resp := make([]*committeeResponse, 0, a.beaconChainCfg.SlotsPerEpoch*a.beaconChainCfg.MaxCommitteesPerSlot)
	isFinalized := slot <= a.forkchoiceStore.FinalizedSlot()
	// s, cn := a.syncedData.HeadState()
	// defer cn()

	if a.forkchoiceStore.LowestAvailableSlot() <= slot {
		// non-finality case
		if err := a.syncedData.ViewHeadState(func(s *state.CachingBeaconState) error {
			if epoch > state.Epoch(s)+maxEpochsLookaheadForDuties {
				return beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("sync committees duties: epoch %d is too far in the future", epoch))
			}
			// get active validator indicies
			committeeCount := s.CommitteeCount(epoch)
			// now start obtaining the committees from the head state
			for currSlot := epoch * a.beaconChainCfg.SlotsPerEpoch; currSlot < (epoch+1)*a.beaconChainCfg.SlotsPerEpoch; currSlot++ {
				if slotFilter != nil && currSlot != *slotFilter {
					continue
				}
				for committeeIndex := uint64(0); committeeIndex < committeeCount; committeeIndex++ {
					if index != nil && committeeIndex != *index {
						continue
					}
					data := &committeeResponse{Index: committeeIndex, Slot: currSlot}
					idxs, err := s.GetBeaconCommitee(currSlot, committeeIndex)
					if err != nil {
						return err
					}
					for _, idx := range idxs {
						data.Validators = append(data.Validators, strconv.FormatUint(idx, 10))
					}
					resp = append(resp, data)
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}

		return newBeaconResponse(resp).WithFinalized(isFinalized).WithOptimistic(isOptimistic), nil
	}
	snRoTx := a.caplinStateSnapshots.View()
	defer snRoTx.Close()
	stateGetter := state_accessors.GetValFnTxAndSnapshot(tx, snRoTx)
	// finality case
	activeIdxs, err := state_accessors.ReadActiveIndicies(
		stateGetter,
		epoch*a.beaconChainCfg.SlotsPerEpoch)
	if err != nil {
		return nil, err
	}

	committeesPerSlot := uint64(len(activeIdxs)) / a.beaconChainCfg.SlotsPerEpoch / a.beaconChainCfg.TargetCommitteeSize
	if a.beaconChainCfg.MaxCommitteesPerSlot < committeesPerSlot {
		committeesPerSlot = a.beaconChainCfg.MaxCommitteesPerSlot
	}
	if committeesPerSlot < 1 {
		committeesPerSlot = 1
	}

	mixPosition := (epoch + a.beaconChainCfg.EpochsPerHistoricalVector - a.beaconChainCfg.MinSeedLookahead - 1) % a.beaconChainCfg.EpochsPerHistoricalVector
	mix, err := a.stateReader.ReadRandaoMixBySlotAndIndex(tx, stateGetter, epoch*a.beaconChainCfg.SlotsPerEpoch, mixPosition)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("could not read randao mix: %v", err))
	}

	for currSlot := epoch * a.beaconChainCfg.SlotsPerEpoch; currSlot < (epoch+1)*a.beaconChainCfg.SlotsPerEpoch; currSlot++ {
		if slotFilter != nil && currSlot != *slotFilter {
			continue
		}
		for committeeIndex := uint64(0); committeeIndex < committeesPerSlot; committeeIndex++ {
			if index != nil && committeeIndex != *index {
				continue
			}
			data := &committeeResponse{Index: committeeIndex, Slot: currSlot}
			index := (currSlot%a.beaconChainCfg.SlotsPerEpoch)*committeesPerSlot + committeeIndex
			committeeCount := committeesPerSlot * a.beaconChainCfg.SlotsPerEpoch
			idxs, err := a.stateReader.ComputeCommittee(mix, activeIdxs, currSlot, committeeCount, index)
			if err != nil {
				return nil, err
			}
			for _, idx := range idxs {
				data.Validators = append(data.Validators, strconv.FormatUint(idx, 10))
			}
			resp = append(resp, data)
		}
	}
	return newBeaconResponse(resp).WithFinalized(isFinalized).WithOptimistic(isOptimistic), nil
}
