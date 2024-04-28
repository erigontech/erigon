package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

type attesterDutyResponse struct {
	Pubkey                  libcommon.Bytes48 `json:"pubkey"`
	ValidatorIndex          uint64            `json:"validator_index,string"`
	CommitteeIndex          uint64            `json:"committee_index,string"`
	CommitteeLength         uint64            `json:"committee_length,string"`
	ValidatorCommitteeIndex uint64            `json:"validator_committee_index,string"`
	CommitteesAtSlot        uint64            `json:"committees_at_slot,string"`
	Slot                    uint64            `json:"slot,string"`
}

func (a *ApiHandler) getDependentRoot(s *state.CachingBeaconState, epoch uint64) libcommon.Hash {
	dependentRootSlot := ((epoch - 1) * a.beaconChainCfg.SlotsPerEpoch) - 3
	maxIterations := 2048
	for i := 0; i < maxIterations; i++ {
		if dependentRootSlot > epoch*a.beaconChainCfg.SlotsPerEpoch {
			return libcommon.Hash{}
		}

		dependentRoot, err := s.GetBlockRootAtSlot(dependentRootSlot)
		if err != nil {
			dependentRootSlot--
			continue
		}
		return dependentRoot
	}
	return libcommon.Hash{}
}

func (a *ApiHandler) getAttesterDuties(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	epoch, err := beaconhttp.EpochFromRequest(r)
	if err != nil {
		return nil, err
	}
	s := a.syncedData.HeadState()
	if s == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable, fmt.Errorf("node is syncing"))
	}
	dependentRoot := a.getDependentRoot(s, epoch)

	var idxsStr []string
	if err := json.NewDecoder(r.Body).Decode(&idxsStr); err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("could not decode request body: %w. request body is required", err))
	}
	if len(idxsStr) == 0 {
		return newBeaconResponse([]string{}).WithOptimistic(false).With("dependent_root", dependentRoot), nil
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

	tx, err := a.indiciesDB.BeginRo(r.Context())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	resp := []attesterDutyResponse{}

	// get the duties
	if a.forkchoiceStore.LowestAvaiableSlot() <= epoch*a.beaconChainCfg.SlotsPerEpoch {
		// non-finality case

		if s == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable, fmt.Errorf("node is syncing"))
		}

		if epoch > state.Epoch(s)+3 {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("epoch %d is too far in the future", epoch))
		}

		// get active validator indicies
		committeeCount := s.CommitteeCount(epoch)
		// now start obtaining the committees from the head state
		for currSlot := epoch * a.beaconChainCfg.SlotsPerEpoch; currSlot < (epoch+1)*a.beaconChainCfg.SlotsPerEpoch; currSlot++ {
			for committeeIndex := uint64(0); committeeIndex < committeeCount; committeeIndex++ {
				idxs, err := s.GetBeaconCommitee(currSlot, committeeIndex)
				if err != nil {
					return nil, err
				}
				for vIdx, idx := range idxs {
					if _, ok := idxSet[int(idx)]; !ok {
						continue
					}
					publicKey, err := s.ValidatorPublicKey(int(idx))
					if err != nil {
						return nil, err
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
		return newBeaconResponse(resp).WithOptimistic(false).With("dependent_root", dependentRoot), nil
	}

	stageStateProgress, err := state_accessors.GetStateProcessingProgress(tx)
	if err != nil {
		return nil, err
	}
	if (epoch)*a.beaconChainCfg.SlotsPerEpoch >= stageStateProgress {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("epoch %d is too far in the future", epoch))
	}
	// finality case
	activeIdxs, err := state_accessors.ReadActiveIndicies(tx, epoch*a.beaconChainCfg.SlotsPerEpoch)
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
	mix, err := a.stateReader.ReadRandaoMixBySlotAndIndex(tx, epoch*a.beaconChainCfg.SlotsPerEpoch, mixPosition)
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
				publicKey, err := state_accessors.ReadPublicKeyByIndex(tx, idx)
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
	return newBeaconResponse(resp).WithOptimistic(false).With("dependent_root", dependentRoot), nil
}
