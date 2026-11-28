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
	"github.com/erigontech/erigon/cl/phase1/core/state/raw"
	shuffling2 "github.com/erigontech/erigon/cl/phase1/core/state/shuffling"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common"
	log "github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

type proposerDuties struct {
	Pubkey         common.Bytes48 `json:"pubkey"`
	ValidatorIndex uint64         `json:"validator_index,string"`
	Slot           uint64         `json:"slot,string"`
}

func (a *ApiHandler) getDutiesProposer(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	return a.getDutiesProposerForVersion(r, false)
}

func (a *ApiHandler) getDutiesProposerV2(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	return a.getDutiesProposerForVersion(r, true)
}

func (a *ApiHandler) getDutiesProposerForVersion(r *http.Request, v2 bool) (*beaconhttp.BeaconResponse, error) {
	if err := a.refuseLaggingHead(); err != nil {
		return nil, err
	}
	epoch, err := beaconhttp.EpochFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	if epochSlotOverflows(epoch, a.beaconChainCfg.SlotsPerEpoch) {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("epoch %d overflows slot computation", epoch))
	}
	if currentEpoch := a.ethClock.GetCurrentEpoch(); epoch > currentEpoch+1 {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("proposer duties: epoch %d is more than one epoch beyond clock epoch %d", epoch, currentEpoch))
	}

	expectedSlot := epoch * a.beaconChainCfg.SlotsPerEpoch
	isFinalized := expectedSlot <= a.forkchoiceStore.FinalizedSlot()
	lowestAvailableSlot := a.forkchoiceStore.LowestAvailableSlot()
	targetVersion := a.beaconChainCfg.GetCurrentStateVersion(epoch)
	var (
		dependentRoot  common.Hash
		recentRoot     common.Hash
		duties         []proposerDuties
		optimistic     bool
		useHistory     bool
		stateToAdvance *state.CachingBeaconState
	)

	if err := a.syncedData.ViewHeadStateWithIdentity(func(s *state.CachingBeaconState, headRoot common.Hash, headSlot uint64) error {
		stateEpoch := state.Epoch(s)
		if epoch > stateEpoch+maxEpochsLookaheadForDuties {
			return beaconhttp.NewEndpointError(http.StatusServiceUnavailable, fmt.Errorf("proposer duties for epoch %d are not available while the head is in epoch %d", epoch, stateEpoch))
		}
		useHistory = epoch < stateEpoch && (isFinalized || expectedSlot < lowestAvailableSlot)
		if useHistory {
			return nil
		}
		dependentRoot, err = a.getDependentRoot(s.GetBlockRootAtSlot, headRoot, headSlot, epoch, v2 && targetVersion >= clparams.FuluVersion)
		if err != nil {
			return err
		}
		optimistic = !isFinalized && a.forkchoiceStore.IsRootOptimistic(headRoot)
		duties = make([]proposerDuties, a.beaconChainCfg.SlotsPerEpoch)
		if epoch < stateEpoch {
			recentRoot, err = s.GetBlockRootAtSlot(expectedSlot + a.beaconChainCfg.SlotsPerEpoch - 1)
			if err != nil {
				return err
			}
			if recentRoot == (common.Hash{}) {
				return fmt.Errorf("canonical block root not found for epoch %d", epoch)
			}
			return nil
		}
		stateToAdvance, err = a.prepareProposerDutiesForTarget(duties, s, targetVersion, epoch, expectedSlot, false)
		return err
	}); err != nil {
		return nil, err
	}

	if useHistory {
		tx, err := a.indiciesDB.BeginRo(r.Context())
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		view := a.caplinStateSnapshots.View()
		defer view.Close()
		usesLookahead := expectedSlot != a.beaconChainCfg.GenesisSlot && targetVersion >= clparams.FuluVersion && epoch > a.beaconChainCfg.FuluForkEpoch
		readiness, err := newHistoricalReadiness(tx, view)
		if err != nil {
			return nil, err
		}
		stateGetter := state_accessors.GetValFnTxAndSnapshot(tx, view)
		if a.stateReader.StateHistoryEnabled() {
			stateGetter, err = historicalDutiesStateGetter(tx, readiness, a.beaconChainCfg)
			if err != nil {
				return nil, err
			}
			if expectedSlot != a.beaconChainCfg.GenesisSlot && !usesLookahead && !a.stateReader.ValidatorTableCovers(expectedSlot) {
				return nil, validatorReplayPending(expectedSlot)
			}
		}

		dependentRoot, err = a.getHistoricalProposerDependentRoot(tx, stateGetter, epoch, v2)
		if err != nil {
			return nil, err
		}
		if !a.stateReader.StateHistoryEnabled() {
			progress := readiness.progress
			if a.caplinStateSnapshots != nil {
				progress = max(progress, a.caplinStateSnapshots.BlocksAvailable())
			}
			if expectedSlot > progress || (expectedSlot != a.beaconChainCfg.GenesisSlot && !usesLookahead && !a.stateReader.ValidatorTableCovers(expectedSlot)) {
				return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("proposer duties: epoch %d is not yet reconstructed", epoch))
			}
		}
		duties, err = a.getHistoricalProposerDuties(r.Context(), tx, stateGetter, epoch, expectedSlot)
		if err != nil {
			return nil, err
		}
		return newBeaconResponse(duties).WithFinalized(isFinalized).WithVersion(targetVersion).With("dependent_root", dependentRoot), nil
	}

	if recentRoot != (common.Hash{}) {
		if err := a.forkchoiceStore.ViewStateAtBlockRoot(recentRoot, func(s *state.CachingBeaconState) error {
			stateToAdvance, err = a.prepareProposerDutiesForTarget(duties, s, targetVersion, epoch, expectedSlot, true)
			return err
		}); err != nil {
			return nil, err
		}
	}

	if stateToAdvance != nil {
		stateEpoch := state.Epoch(stateToAdvance)
		stateVersion := stateToAdvance.Version()
		if err := transition.DefaultMachine.ProcessSlots(stateToAdvance, expectedSlot); err != nil {
			log.Warn("getDutiesProposer: failed to advance state", "epoch", epoch, "stateEpoch", stateEpoch, "stateVersion", stateVersion, "targetVersion", targetVersion, "err", err)
			return nil, err
		}
		if err := fillProposerDutiesFromState(duties, stateToAdvance, epoch, expectedSlot); err != nil {
			return nil, err
		}
	}

	return newBeaconResponse(duties).
		WithFinalized(isFinalized).
		WithOptimistic(optimistic).
		WithVersion(targetVersion).
		With("dependent_root", dependentRoot), nil
}

func (a *ApiHandler) prepareProposerDutiesForTarget(duties []proposerDuties, s *state.CachingBeaconState, targetVersion clparams.StateVersion, epoch, expectedSlot uint64, recent bool) (*state.CachingBeaconState, error) {
	stateEpoch := state.Epoch(s)
	if targetVersion.Before(s.Version()) {
		versionedState, err := a.copyHeadStateForDuties(s)
		if err != nil {
			return nil, err
		}
		versionedState.SetVersion(targetVersion)
		s = versionedState
	}

	if targetVersion.After(s.Version()) || (s.Version() >= clparams.FuluVersion && epoch > stateEpoch+a.beaconChainCfg.MinSeedLookahead) || (s.Version() < clparams.FuluVersion && stateEpoch < epoch && (recent || epoch <= stateEpoch+a.beaconChainCfg.MinSeedLookahead+1)) {
		return a.copyHeadStateForDuties(s)
	}
	if !recent && s.Version() < clparams.FuluVersion && epoch > stateEpoch+a.beaconChainCfg.MinSeedLookahead+1 {
		return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable, fmt.Errorf("proposer duties for epoch %d are not available while the head is in epoch %d", epoch, stateEpoch))
	}
	return nil, fillProposerDutiesFromState(duties, s, epoch, expectedSlot)
}

// copyHeadStateForDuties copies a state using the handler's config to pick up fork schedule changes.
func (a *ApiHandler) copyHeadStateForDuties(s *state.CachingBeaconState) (*state.CachingBeaconState, error) {
	copied := state.New(a.beaconChainCfg)
	if err := s.CopyInto(copied); err != nil {
		return nil, err
	}
	return copied, nil
}

func fillProposerDutiesFromState(duties []proposerDuties, s *state.CachingBeaconState, epoch, expectedSlot uint64) error {
	if s.Version() >= clparams.FuluVersion {
		if _, ok := s.ProposerIndexFromLookahead(expectedSlot); !ok {
			return fmt.Errorf("proposer lookahead does not cover epoch %d", epoch)
		}
		return fillProposerDuties(duties, s.BeaconState, expectedSlot, func(i int) uint64 {
			proposerIndex, _ := s.ProposerIndexFromLookahead(expectedSlot + uint64(i))
			return proposerIndex
		})
	}
	proposerIndices, err := s.GetBeaconProposerIndices(epoch)
	if err != nil {
		return err
	}
	return fillProposerDuties(duties, s.BeaconState, expectedSlot, func(i int) uint64 { return proposerIndices[i] })
}

func fillProposerDuties(duties []proposerDuties, s *raw.BeaconState, expectedSlot uint64, proposerIndexAt func(int) uint64) error {
	for i := range duties {
		proposerIndex := proposerIndexAt(i)
		pk, err := s.ValidatorPublicKey(int(proposerIndex))
		if err != nil {
			return err
		}
		duties[i] = proposerDuties{
			Pubkey:         pk,
			ValidatorIndex: proposerIndex,
			Slot:           expectedSlot + uint64(i),
		}
	}
	return nil
}

func (a *ApiHandler) getHistoricalProposerDependentRoot(tx kv.Tx, stateGetter state_accessors.GetValFn, epoch uint64, v2 bool) (common.Hash, error) {
	targetVersion := a.beaconChainCfg.GetCurrentStateVersion(epoch)
	if epoch == 0 || (v2 && targetVersion >= clparams.FuluVersion && epoch <= 1) {
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
			return common.Hash{}, beaconhttp.NewEndpointError(a.historicalDutiesMissingDataStatus(), errors.New("genesis block not found"))
		}
		return root, nil
	}

	dependentRootEpoch := epoch
	if v2 && targetVersion >= clparams.FuluVersion {
		dependentRootEpoch = epoch - 1
	}
	dependentRootSlot := dependentRootEpoch*a.beaconChainCfg.SlotsPerEpoch - 1
	dependentRootBytes, err := stateGetter(kv.BlockRoot, base_encoding.Encode64ToBytes4(dependentRootSlot))
	if err != nil {
		return common.Hash{}, err
	}
	if len(dependentRootBytes) == 32 {
		return common.BytesToHash(dependentRootBytes), nil
	}

	maxIterations := int(maxEpochsLookaheadForDuties * 2 * a.beaconChainCfg.SlotsPerEpoch)
	for range maxIterations {
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
	return common.Hash{}, beaconhttp.NewEndpointError(a.historicalDutiesMissingDataStatus(), fmt.Errorf("dependent root not found for epoch %d", epoch))
}

func (a *ApiHandler) historicalDutiesMissingDataStatus() int {
	if !a.stateReader.StateHistoryEnabled() {
		return http.StatusBadRequest
	}
	return http.StatusInternalServerError
}

func validatorReplayPending(slot uint64) error {
	return beaconhttp.NewEndpointError(http.StatusServiceUnavailable, fmt.Errorf("static validator table does not yet cover slot %d", slot))
}

func (a *ApiHandler) getHistoricalProposerDuties(ctx context.Context, tx kv.Tx, stateGetter state_accessors.GetValFn, epoch, expectedSlot uint64) ([]proposerDuties, error) {
	if expectedSlot == a.beaconChainCfg.GenesisSlot {
		genesisState, err := a.stateReader.ReadHistoricalState(ctx, tx, expectedSlot)
		if err != nil {
			return nil, err
		}
		if genesisState == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, fmt.Errorf("state not found for slot %d", expectedSlot))
		}
		duties := make([]proposerDuties, a.beaconChainCfg.SlotsPerEpoch)
		if err := fillProposerDutiesFromState(duties, genesisState, epoch, expectedSlot); err != nil {
			return nil, err
		}
		return duties, nil
	}

	if a.beaconChainCfg.GetCurrentStateVersion(epoch) >= clparams.FuluVersion && epoch > a.beaconChainCfg.FuluForkEpoch {
		epochData, err := state_accessors.ReadEpochData(stateGetter, expectedSlot, a.beaconChainCfg)
		if err != nil {
			return nil, err
		}
		if epochData == nil {
			return nil, beaconhttp.NewEndpointError(a.historicalDutiesMissingDataStatus(), fmt.Errorf("epoch data not found for slot %d", expectedSlot))
		}
		duties := make([]proposerDuties, a.beaconChainCfg.SlotsPerEpoch)
		for i := range duties {
			index := epochData.ProposerLookahead.Get(i)
			pubkey, err := a.syncedData.ValidatorPublicKeyByIndex(int(index))
			if err != nil {
				return nil, err
			}
			duties[i] = proposerDuties{Pubkey: pubkey, ValidatorIndex: index, Slot: expectedSlot + uint64(i)}
		}
		return duties, nil
	}

	validatorSet, err := a.stateReader.ReadValidatorsForHistoricalState(tx, stateGetter, expectedSlot)
	if err != nil {
		return nil, err
	}
	if validatorSet == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, fmt.Errorf("validators not found for slot %d", expectedSlot))
	}
	duties := make([]proposerDuties, a.beaconChainCfg.SlotsPerEpoch)

	activeIndices := make([]uint64, 0, validatorSet.Length())
	validatorSet.Range(func(validatorIndex int, validator solid.Validator, _ int) bool {
		if validator.Active(epoch) {
			activeIndices = append(activeIndices, uint64(validatorIndex))
		}
		return true
	})
	if len(activeIndices) == 0 {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, fmt.Errorf("active validators not found for slot %d", expectedSlot))
	}

	mixPosition := (epoch + a.beaconChainCfg.EpochsPerHistoricalVector - a.beaconChainCfg.MinSeedLookahead - 1) %
		a.beaconChainCfg.EpochsPerHistoricalVector
	mix, err := a.stateReader.ReadRandaoMixBySlotAndIndex(stateGetter, expectedSlot, mixPosition)
	if err != nil {
		return nil, fmt.Errorf("could not read randao mix: %w", err)
	}
	historicalState := raw.New(a.beaconChainCfg)
	historicalState.SetVersion(a.beaconChainCfg.GetCurrentStateVersion(epoch))
	if epoch == a.beaconChainCfg.FuluForkEpoch && historicalState.Version() >= clparams.FuluVersion {
		historicalState.SetVersion(clparams.FuluVersion)
	}
	historicalState.SetValidators(validatorSet)

	epochSeed := shuffling2.GetSeed(mix, epoch, a.beaconChainCfg.DomainBeaconProposer)
	seedArray := [32]byte{}
	copy(seedArray[:], epochSeed[:])
	proposerIndices, err := shuffling2.ComputeProposerIndices(historicalState, epoch, seedArray, activeIndices)
	if err != nil {
		return nil, err
	}

	if err := fillProposerDuties(duties, historicalState, expectedSlot, func(i int) uint64 { return proposerIndices[i] }); err != nil {
		return nil, err
	}
	return duties, nil
}
