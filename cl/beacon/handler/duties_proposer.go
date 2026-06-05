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
	"errors"
	"fmt"
	"net/http"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	shuffling2 "github.com/erigontech/erigon/cl/phase1/core/state/shuffling"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common"
	log "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync"
)

type proposerDuties struct {
	Pubkey         common.Bytes48 `json:"pubkey"`
	ValidatorIndex uint64         `json:"validator_index,string"`
	Slot           uint64         `json:"slot,string"`
}

// isProposerDutyInLookaheadVector checks if the proposer duty is within the lookahead vector.
func (a *ApiHandler) isProposerDutyInLookaheadVector(s *state.CachingBeaconState, epoch uint64) bool {
	return s.Version() >= clparams.FuluVersion && epoch >= state.Epoch(s) && epoch <= state.Epoch(s)+a.beaconChainCfg.MinSeedLookahead
}

func (a *ApiHandler) getDutiesProposer(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	epoch, err := beaconhttp.EpochFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}

	expectedSlot := epoch * a.beaconChainCfg.SlotsPerEpoch
	isFinalized := expectedSlot <= a.forkchoiceStore.FinalizedSlot()

	if isFinalized {
		tx, err := a.indiciesDB.BeginRo(r.Context())
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		var view *snapshotsync.CaplinStateView
		if a.caplinStateSnapshots != nil {
			view = a.caplinStateSnapshots.View()
			defer view.Close()
		}
		stateGetter := state_accessors.GetValFnTxAndSnapshot(tx, view)

		dependentRoot, err := a.getHistoricalProposerDependentRoot(tx, stateGetter, epoch)
		if err != nil {
			return nil, err
		}
		duties, err := a.getHistoricalProposerDuties(r.Context(), tx, stateGetter, epoch, expectedSlot)
		if err != nil {
			return nil, err
		}
		return newBeaconResponse(duties).WithFinalized(true).WithVersion(a.beaconChainCfg.GetCurrentStateVersion(epoch)).With("dependent_root", dependentRoot), nil
	}

	dependentRoot, err := a.getDependentRoot(epoch, false)
	if err != nil {
		return nil, err
	}

	duties := make([]proposerDuties, a.beaconChainCfg.SlotsPerEpoch)

	if err := a.syncedData.ViewHeadState(func(s *state.CachingBeaconState) error {
		headEpoch := state.Epoch(s)
		if epoch > headEpoch+maxEpochsLookaheadForDuties {
			return beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("proposer duties: epoch %d is too far in the future", epoch))
		}

		targetVersion := a.beaconChainCfg.GetCurrentStateVersion(epoch)
		if targetVersion.After(s.Version()) {
			advancedState, copyErr := a.copyHeadStateForDuties(s)
			if copyErr != nil {
				return copyErr
			}
			if processErr := transition.DefaultMachine.ProcessSlots(advancedState, expectedSlot); processErr != nil {
				log.Warn("getDutiesProposer: failed to advance state to target fork",
					"epoch", epoch, "headEpoch", headEpoch, "err", processErr)
				return processErr
			}
			return fillProposerDutiesFromState(duties, advancedState, epoch, expectedSlot)
		}
		if targetVersion.Before(s.Version()) {
			versionedState, copyErr := a.copyHeadStateForDuties(s)
			if copyErr != nil {
				return copyErr
			}
			versionedState.SetVersion(targetVersion)
			s = versionedState
		}

		// if the proposer duties are in the lookahead vector, we can use the lookahead vector to fetch the proposers
		if a.isProposerDutyInLookaheadVector(s, epoch) {
			lookaheadVector := s.GetProposerLookahead()
			stateEpoch := state.Epoch(s)
			startLookAheadIndex := (epoch - stateEpoch) * a.beaconChainCfg.SlotsPerEpoch
			for i := uint64(0); i < a.beaconChainCfg.SlotsPerEpoch; i++ {
				proposerIndex := lookaheadVector.Get(int(startLookAheadIndex + i))
				var pk common.Bytes48
				pk, err = s.ValidatorPublicKey(int(proposerIndex))
				if err != nil {
					panic(err)
				}
				duties[i] = proposerDuties{
					Pubkey:         pk,
					ValidatorIndex: proposerIndex,
					Slot:           (epoch * a.beaconChainCfg.SlotsPerEpoch) + i,
				}
			}
			return nil
		}

		// When the target epoch is beyond the lookahead range (e.g. head is far
		// behind), we must advance a copy of the state to the target epoch so
		// that RANDAO mixes and proposer lookahead are correct.
		if s.Version() >= clparams.FuluVersion && epoch > headEpoch+a.beaconChainCfg.MinSeedLookahead {
			advancedState, copyErr := a.copyHeadStateForDuties(s)
			if copyErr != nil {
				return copyErr
			}
			if processErr := transition.DefaultMachine.ProcessSlots(advancedState, expectedSlot); processErr != nil {
				log.Warn("getDutiesProposer: failed to advance state for epoch beyond lookahead",
					"epoch", epoch, "headEpoch", headEpoch, "err", processErr)
				return processErr
			}
			return fillProposerDutiesFromState(duties, advancedState, epoch, expectedSlot)
		}

		// Lets do proposer index computation (pre-Fulu or when epoch is within range)
		mixPosition := (epoch + a.beaconChainCfg.EpochsPerHistoricalVector - a.beaconChainCfg.MinSeedLookahead - 1) %
			a.beaconChainCfg.EpochsPerHistoricalVector

		mix := s.GetRandaoMix(int(mixPosition))

		input := shuffling2.GetSeed(a.beaconChainCfg, mix, epoch, a.beaconChainCfg.DomainBeaconProposer)
		seedArray := [32]byte{}
		copy(seedArray[:], input[:])
		indices := s.GetActiveValidatorsIndices(epoch)
		proposerIndices, err := shuffling2.ComputeProposerIndices(s.BeaconState, epoch, seedArray, indices)
		if err != nil {
			return err
		}
		return fillProposerDutiesFromIndices(duties, s, proposerIndices, expectedSlot)
	}); err != nil {
		return nil, err
	}

	return newBeaconResponse(duties).
		WithFinalized(false).
		WithOptimistic(a.forkchoiceStore.IsHeadOptimistic()).
		WithVersion(a.beaconChainCfg.GetCurrentStateVersion(epoch)).
		With("dependent_root", dependentRoot), nil
}

func (a *ApiHandler) copyHeadStateForDuties(s *state.CachingBeaconState) (*state.CachingBeaconState, error) {
	copied := state.New(a.beaconChainCfg)
	if err := s.CopyInto(copied); err != nil {
		return nil, err
	}
	return copied, nil
}

func fillProposerDutiesFromState(duties []proposerDuties, s *state.CachingBeaconState, epoch, expectedSlot uint64) error {
	proposerIndices, err := s.GetBeaconProposerIndices(epoch)
	if err != nil {
		return err
	}
	return fillProposerDutiesFromIndices(duties, s, proposerIndices, expectedSlot)
}

func fillProposerDutiesFromIndices(duties []proposerDuties, s *state.CachingBeaconState, proposerIndices []uint64, expectedSlot uint64) error {
	for i := uint64(0); i < s.BeaconConfig().SlotsPerEpoch; i++ {
		proposerIndex := proposerIndices[i]
		pk, err := s.ValidatorPublicKey(int(proposerIndex))
		if err != nil {
			return err
		}
		duties[i] = proposerDuties{
			Pubkey:         pk,
			ValidatorIndex: proposerIndex,
			Slot:           expectedSlot + i,
		}
	}
	return nil
}

func (a *ApiHandler) getHistoricalProposerDependentRoot(tx kv.Tx, stateGetter state_accessors.GetValFn, epoch uint64) (common.Hash, error) {
	if epoch == 0 {
		rootBytes, err := stateGetter(kv.BlockRoot, base_encoding.Encode64ToBytes4(0))
		if err != nil {
			return common.Hash{}, err
		}
		if len(rootBytes) == 32 {
			return common.BytesToHash(rootBytes), nil
		}
		root, err := beacon_indicies.ReadCanonicalBlockRoot(tx, 0)
		if err != nil {
			return common.Hash{}, err
		}
		if root == (common.Hash{}) {
			return common.Hash{}, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("genesis block not found"))
		}
		return root, nil
	}

	dependentRootSlot := epoch*a.beaconChainCfg.SlotsPerEpoch - 1
	dependentRootBytes, err := stateGetter(kv.BlockRoot, base_encoding.Encode64ToBytes4(dependentRootSlot))
	if err != nil {
		return common.Hash{}, err
	}
	if len(dependentRootBytes) == 32 {
		return common.BytesToHash(dependentRootBytes), nil
	}

	maxIterations := int(maxEpochsLookaheadForDuties * 2 * a.beaconChainCfg.SlotsPerEpoch)
	for i := 0; i < maxIterations; i++ {
		dependentRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, dependentRootSlot)
		if err != nil {
			return common.Hash{}, err
		}
		if dependentRoot != (common.Hash{}) {
			return dependentRoot, nil
		}
		if dependentRootSlot == 0 {
			break
		}
		dependentRootSlot--
	}
	return common.Hash{}, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("dependent root not found for epoch %d", epoch))
}

func (a *ApiHandler) getHistoricalProposerDuties(ctx context.Context, tx kv.Tx, stateGetter state_accessors.GetValFn, epoch, expectedSlot uint64) ([]proposerDuties, error) {
	stageStateProgress, err := state_accessors.GetStateProcessingProgress(tx)
	if err != nil {
		return nil, err
	}
	if a.caplinStateSnapshots != nil {
		stageStateProgress = max(stageStateProgress, a.caplinStateSnapshots.BlocksAvailable())
	}
	if expectedSlot > stageStateProgress {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("proposer duties: epoch %d is not yet reconstructed", epoch))
	}

	if expectedSlot == a.beaconChainCfg.GenesisSlot {
		genesisState, err := a.stateReader.ReadHistoricalState(ctx, tx, expectedSlot)
		if err != nil {
			return nil, err
		}
		if genesisState == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("state not found for slot %d", expectedSlot))
		}
		duties := make([]proposerDuties, a.beaconChainCfg.SlotsPerEpoch)
		if err := fillProposerDutiesFromState(duties, genesisState, epoch, expectedSlot); err != nil {
			return nil, err
		}
		return duties, nil
	}

	validatorSet, err := a.stateReader.ReadValidatorsForHistoricalState(tx, stateGetter, expectedSlot)
	if err != nil {
		return nil, err
	}
	if validatorSet == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("validators not found for slot %d", expectedSlot))
	}
	activeIndices := make([]uint64, 0, validatorSet.Length())
	validatorSet.Range(func(validatorIndex int, validator solid.Validator, _ int) bool {
		if validator.Active(epoch) {
			activeIndices = append(activeIndices, uint64(validatorIndex))
		}
		return true
	})
	if len(activeIndices) == 0 {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("active validators not found for slot %d", expectedSlot))
	}

	mixPosition := (epoch + a.beaconChainCfg.EpochsPerHistoricalVector - a.beaconChainCfg.MinSeedLookahead - 1) %
		a.beaconChainCfg.EpochsPerHistoricalVector
	mix, err := a.stateReader.ReadRandaoMixBySlotAndIndex(tx, stateGetter, expectedSlot, mixPosition)
	if err != nil {
		return nil, err
	}
	if mix == (common.Hash{}) {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("mix not found for slot %d and index %d. maybe block was not backfilled or range was pruned", expectedSlot, mixPosition))
	}

	historicalState := state.New(a.beaconChainCfg)
	historicalState.SetVersion(a.beaconChainCfg.GetCurrentStateVersion(epoch))
	historicalState.SetSlot(expectedSlot)
	historicalState.SetValidators(validatorSet)

	epochSeed := shuffling2.GetSeed(a.beaconChainCfg, mix, epoch, a.beaconChainCfg.DomainBeaconProposer)
	seedArray := [32]byte{}
	copy(seedArray[:], epochSeed[:])
	proposerIndices, err := shuffling2.ComputeProposerIndices(historicalState.BeaconState, epoch, seedArray, activeIndices)
	if err != nil {
		return nil, err
	}

	duties := make([]proposerDuties, a.beaconChainCfg.SlotsPerEpoch)
	if err := fillProposerDutiesFromIndices(duties, historicalState, proposerIndices, expectedSlot); err != nil {
		return nil, err
	}
	return duties, nil
}
