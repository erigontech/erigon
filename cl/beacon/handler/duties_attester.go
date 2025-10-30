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
	"strconv"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

const maxEpochsLookaheadForDuties = 32

type attesterDutyResponse struct {
	Pubkey                  common.Bytes48 `json:"pubkey"`
	ValidatorIndex          uint64         `json:"validator_index,string"`
	CommitteeIndex          uint64         `json:"committee_index,string"`
	CommitteeLength         uint64         `json:"committee_length,string"`
	ValidatorCommitteeIndex uint64         `json:"validator_committee_index,string"`
	CommitteesAtSlot        uint64         `json:"committees_at_slot,string"`
	Slot                    uint64         `json:"slot,string"`
}

func (a *ApiHandler) getDependentRoot(epoch uint64, attester bool) (common.Hash, error) {
	var (
		dependentRoot common.Hash
		err           error
	)
	return dependentRoot, a.syncedData.ViewHeadState(func(s *state.CachingBeaconState) error {
		dependentRootSlot := (epoch * a.beaconChainCfg.SlotsPerEpoch) - 1
		if attester {
			dependentRootSlot = ((epoch - 1) * a.beaconChainCfg.SlotsPerEpoch) - 1
		}
		if !a.syncedData.Syncing() && dependentRootSlot == a.syncedData.HeadSlot() {
			dependentRoot = a.syncedData.HeadRoot()
			return nil
		}
		maxIterations := int(maxEpochsLookaheadForDuties * 2 * a.beaconChainCfg.SlotsPerEpoch)
		for i := 0; i < maxIterations; i++ {
			if dependentRootSlot > epoch*a.beaconChainCfg.SlotsPerEpoch {
				return nil
			}

			dependentRoot, err = s.GetBlockRootAtSlot(dependentRootSlot)
			if err != nil {
				dependentRootSlot--
				continue
			}
			return nil
		}
		return nil
	})
}

func (a *ApiHandler) getAttesterDuties(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	epoch, err := beaconhttp.EpochFromRequest(r)
	if err != nil {
		return nil, err
	}

	dependentRoot, err := a.getDependentRoot(epoch, true)
	if err != nil {
		return nil, err
	}

	var idxsStr []string
	if err := json.NewDecoder(r.Body).Decode(&idxsStr); err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("could not decode request body: %w. request body is required", err))
	}
	if len(idxsStr) == 0 {
		return newBeaconResponse([]string{}).WithOptimistic(a.forkchoiceStore.IsHeadOptimistic()).With("dependent_root", dependentRoot), nil
	}
	idxSet := map[int]struct{}{}
	// convert the request to uint64
	for _, idxStr := range idxsStr {

		idx, err := strconv.ParseUint(idxStr, 10, 64)
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("could not parse validator index: %w", err))
		}
		if _, ok := idxSet[int(idx)]; ok {
			continue
		}
		idxSet[int(idx)] = struct{}{}
	}

	resp := []attesterDutyResponse{}

	// get the duties
	if a.forkchoiceStore.LowestAvailableSlot() <= epoch*a.beaconChainCfg.SlotsPerEpoch {
		// non-finality case
		if err := a.syncedData.ViewHeadState(func(s *state.CachingBeaconState) error {
			if epoch > state.Epoch(s)+maxEpochsLookaheadForDuties {
				return beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("attestation duties: epoch %d is too far in the future", epoch))
			}

			// get active validator indicies
			committeeCount := s.CommitteeCount(epoch)
			// now start obtaining the committees from the head state
			for currSlot := epoch * a.beaconChainCfg.SlotsPerEpoch; currSlot < (epoch+1)*a.beaconChainCfg.SlotsPerEpoch; currSlot++ {
				for committeeIndex := uint64(0); committeeIndex < committeeCount; committeeIndex++ {
					idxs, err := s.GetBeaconCommitee(currSlot, committeeIndex)
					if err != nil {
						return err
					}
					for vIdx, idx := range idxs {
						if _, ok := idxSet[int(idx)]; !ok {
							continue
						}
						publicKey, err := s.ValidatorPublicKey(int(idx))
						if err != nil {
							return err
						}
						duty := attesterDutyResponse{
							Pubkey:                  publicKey,
							ValidatorIndex:          idx,
							CommitteeIndex:          committeeIndex,
							CommitteeLength:         uint64(len(idxs)),
							ValidatorCommitteeIndex: uint64(vIdx),
							CommitteesAtSlot:        committeeCount,
							Slot:                    currSlot,
						}
						resp = append(resp, duty)
					}
				}
			}
			return nil
		}); err != nil {
			return nil, err
		}

		return newBeaconResponse(resp).WithOptimistic(a.forkchoiceStore.IsHeadOptimistic()).With("dependent_root", dependentRoot), nil
	}

	tx, err := a.indiciesDB.BeginRo(r.Context())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	stageStateProgress, err := state_accessors.GetStateProcessingProgress(tx)
	if err != nil {
		return nil, err
	}
	if (epoch)*a.beaconChainCfg.SlotsPerEpoch >= stageStateProgress {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("attestation duties: epoch %d is not yet reconstructed", epoch))
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
		for committeeIndex := uint64(0); committeeIndex < committeesPerSlot; committeeIndex++ {
			index := (currSlot%a.beaconChainCfg.SlotsPerEpoch)*committeesPerSlot + committeeIndex
			committeeCount := committeesPerSlot * a.beaconChainCfg.SlotsPerEpoch
			idxs, err := a.stateReader.ComputeCommittee(mix, activeIdxs, currSlot, committeeCount, index)
			if err != nil {
				return nil, err
			}
			for vIdx, idx := range idxs {
				if _, ok := idxSet[int(idx)]; !ok {
					continue
				}

				publicKey, err := a.syncedData.ValidatorPublicKeyByIndex(int(idx))
				if err != nil {
					return nil, err
				}
				duty := attesterDutyResponse{
					Pubkey:                  publicKey,
					ValidatorIndex:          idx,
					CommitteeIndex:          committeeIndex,
					CommitteeLength:         uint64(len(idxs)),
					ValidatorCommitteeIndex: uint64(vIdx),
					CommitteesAtSlot:        committeesPerSlot,
					Slot:                    currSlot,
				}
				resp = append(resp, duty)
			}
		}
	}
	return newBeaconResponse(resp).WithOptimistic(a.forkchoiceStore.IsHeadOptimistic()).With("dependent_root", dependentRoot), nil
}
