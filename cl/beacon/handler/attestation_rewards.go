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
	"errors"
	"io"
	"net/http"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/transition/impl/eth2/statechange"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/db/kv"
)

type IdealReward struct {
	EffectiveBalance int64 `json:"effective_balance,string"`
	Head             int64 `json:"head,string"`
	Target           int64 `json:"target,string"`
	Source           int64 `json:"source,string"`
	InclusionDelay   int64 `json:"inclusion_delay,string"`
	Inactivity       int64 `json:"inactivity,string"`
}

type TotalReward struct {
	ValidatorIndex int64 `json:"validator_index,string"`
	Head           int64 `json:"head,string"`
	Target         int64 `json:"target,string"`
	Source         int64 `json:"source,string"`
	InclusionDelay int64 `json:"inclusion_delay,string"`
	Inactivity     int64 `json:"inactivity,string"`
}

type attestationsRewardsResponse struct {
	IdealRewards []IdealReward `json:"ideal_rewards"`
	TotalRewards []TotalReward `json:"total_rewards"`
}

func (a *ApiHandler) PostEthV1BeaconRewardsAttestations(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	ctx := r.Context()

	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	epoch, err := beaconhttp.EpochFromRequest(r)

	// attestation rewards are always delayed by 1 extra epoch compared to other rewards
	epoch += 1

	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}

	req := []string{}
	// read the entire body
	jsonBytes, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	// parse json body request
	if len(jsonBytes) > 0 {
		if err := json.Unmarshal(jsonBytes, &req); err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
		}
	}

	filterIndicies, err := parseQueryValidatorIndicies(a.syncedData, req)
	if err != nil {
		return nil, err
	}
	_, headSlot, statusCode, err := a.getHead()
	if err != nil {
		return nil, beaconhttp.NewEndpointError(statusCode, err)
	}
	headEpoch := headSlot / a.beaconChainCfg.SlotsPerEpoch
	if epoch > headEpoch {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("epoch is in the future"))
	}
	// Few cases to handle:
	// 1) finalized data
	// 2) not finalized data
	version := a.beaconChainCfg.GetCurrentStateVersion(epoch)

	// finalized data
	if epoch > a.forkchoiceStore.LowestAvailableSlot()/a.beaconChainCfg.SlotsPerEpoch {
		minRange := epoch * a.beaconChainCfg.SlotsPerEpoch
		maxRange := (epoch + 1) * a.beaconChainCfg.SlotsPerEpoch
		var blockRoot common.Hash
		for i := maxRange - 1; i >= minRange; i-- {
			blockRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, i)
			if err != nil {
				return nil, err
			}
			if blockRoot == (common.Hash{}) {
				continue
			}
			if version == clparams.Phase0Version {
				return nil, beaconhttp.NewEndpointError(http.StatusHTTPVersionNotSupported, errors.New("phase0 state is not supported when there is no antiquation"))
			}
			inactivityScores, err := a.forkchoiceStore.GetInactivitiesScores(blockRoot)
			if err != nil {
				return nil, err
			}
			if inactivityScores == nil {
				return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("no inactivity scores found for this epoch"))
			}

			prevParticipation, err := a.forkchoiceStore.GetPreviousParticipationIndicies(blockRoot)
			if err != nil {
				return nil, err
			}
			if prevParticipation == nil {
				return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("no previous participation found for this epoch"))
			}
			validatorSet, err := a.forkchoiceStore.GetValidatorSet(blockRoot)
			if err != nil {
				return nil, err
			}
			if validatorSet == nil {
				return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("no validator set found for this epoch"))
			}

			finalizedCheckpoint, _, _, ok := a.forkchoiceStore.GetFinalityCheckpoints(blockRoot)
			if !ok {
				return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("no finalized checkpoint found for this epoch"))
			}

			resp, err := a.computeAttestationsRewardsForAltair(validatorSet, inactivityScores, prevParticipation, a.isInactivityLeaking(epoch, finalizedCheckpoint), filterIndicies, epoch)
			if err != nil {
				return nil, err
			}
			return resp.WithFinalized(true).WithOptimistic(a.forkchoiceStore.IsRootOptimistic(blockRoot)), nil
		}
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("no block found for this epoch"))
	}

	root, err := a.findEpochRoot(tx, epoch)
	if err != nil {
		return nil, err
	}
	lastSlotPtr, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
	if err != nil {
		return nil, err
	}
	if lastSlotPtr == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("no block found for this epoch"))
	}
	lastSlot := *lastSlotPtr

	stateProgress, err := state_accessors.GetStateProcessingProgress(tx)
	if err != nil {
		return nil, err
	}
	if lastSlot > stateProgress {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("requested range is not yet processed or the node is not archivial"))
	}
	snRoTx := a.caplinStateSnapshots.View()
	defer snRoTx.Close()

	stateGetter := state_accessors.GetValFnTxAndSnapshot(tx, snRoTx)

	epochData, err := state_accessors.ReadEpochData(stateGetter, a.beaconChainCfg.RoundSlotToEpoch(lastSlot), a.beaconChainCfg)
	if err != nil {
		return nil, err
	}

	validatorSet, err := a.stateReader.ReadValidatorsForHistoricalState(tx, stateGetter, lastSlot)
	if err != nil {
		return nil, err
	}
	if validatorSet == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("no validator set found for this epoch"))
	}

	_, previousIdx, err := a.stateReader.ReadParticipations(tx, stateGetter, lastSlot)
	if err != nil {
		return nil, err
	}

	_, _, finalizedCheckpoint, ok, err := state_accessors.ReadCheckpoints(stateGetter, epoch*a.beaconChainCfg.SlotsPerEpoch, a.beaconChainCfg)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("no finalized checkpoint found for this epoch"))
	}
	if version == clparams.Phase0Version {
		resp, err := a.computeAttestationsRewardsForPhase0(validatorSet, finalizedCheckpoint.Epoch-epoch, epochData.TotalActiveBalance, previousIdx, a.isInactivityLeaking(epoch, finalizedCheckpoint), filterIndicies, epoch)
		if err != nil {
			return nil, err
		}
		return resp.WithFinalized(true).WithOptimistic(a.forkchoiceStore.IsRootOptimistic(root)), nil
	}
	inactivityScores := solid.NewUint64ListSSZ(int(a.beaconChainCfg.ValidatorRegistryLimit))
	if err := a.stateReader.ReconstructUint64ListDump(stateGetter, lastSlot, kv.InactivityScores, validatorSet.Length(), inactivityScores); err != nil {
		return nil, err
	}
	resp, err := a.computeAttestationsRewardsForAltair(
		validatorSet,
		inactivityScores,
		previousIdx,
		a.isInactivityLeaking(epoch, finalizedCheckpoint),
		filterIndicies,
		epoch)
	if err != nil {
		return nil, err
	}
	return resp.WithFinalized(true).WithOptimistic(a.forkchoiceStore.IsRootOptimistic(root)), nil
}

func (a *ApiHandler) isInactivityLeaking(epoch uint64, finalityCheckpoint solid.Checkpoint) bool {
	prevEpoch := epoch
	if epoch > 0 {
		prevEpoch = epoch - 1
	}
	return prevEpoch-finalityCheckpoint.Epoch > a.beaconChainCfg.MinEpochsToInactivityPenalty
}

func (a *ApiHandler) baseReward(version clparams.StateVersion, effectiveBalance, activeBalanceRoot uint64) uint64 {
	basePerIncrement := a.beaconChainCfg.EffectiveBalanceIncrement * a.beaconChainCfg.BaseRewardFactor / activeBalanceRoot
	if version != clparams.Phase0Version {
		return (effectiveBalance / a.beaconChainCfg.EffectiveBalanceIncrement) * basePerIncrement
	}
	return effectiveBalance * a.beaconChainCfg.BaseRewardFactor / activeBalanceRoot / a.beaconChainCfg.BaseRewardsPerEpoch
}

func (a *ApiHandler) computeAttestationsRewardsForAltair(validatorSet *solid.ValidatorSet, inactivityScores solid.Uint64ListSSZ, previousParticipation *solid.ParticipationBitList, inactivityLeak bool, filterIndicies []uint64, epoch uint64) (*beaconhttp.BeaconResponse, error) {
	totalActiveBalance := uint64(0)
	prevEpoch := uint64(0)
	if epoch > 0 {
		prevEpoch = epoch - 1
	}
	flagsUnslashedIndiciesSet := statechange.GetUnslashedIndiciesSet(a.beaconChainCfg, prevEpoch, validatorSet, previousParticipation)
	weights := a.beaconChainCfg.ParticipationWeights()
	flagsTotalBalances := make([]uint64, len(weights))

	validatorSet.Range(func(validatorIndex int, v solid.Validator, l int) bool {
		if v.Active(epoch) {
			totalActiveBalance += v.EffectiveBalance()
		}

		for i := range weights {
			if flagsUnslashedIndiciesSet[i][validatorIndex] {
				flagsTotalBalances[i] += v.EffectiveBalance()
			}
		}
		return true
	})
	version := a.beaconChainCfg.GetCurrentStateVersion(epoch)
	inactivityPenaltyDenominator := a.beaconChainCfg.InactivityScoreBias * a.beaconChainCfg.GetPenaltyQuotient(version)
	rewardMultipliers := make([]uint64, len(weights))
	for i := range weights {
		rewardMultipliers[i] = weights[i] * (flagsTotalBalances[i] / a.beaconChainCfg.EffectiveBalanceIncrement)
	}

	rewardDenominator := (totalActiveBalance / a.beaconChainCfg.EffectiveBalanceIncrement) * a.beaconChainCfg.WeightDenominator
	var response *attestationsRewardsResponse
	if len(filterIndicies) > 0 {
		response = &attestationsRewardsResponse{
			IdealRewards: make([]IdealReward, 0, len(filterIndicies)),
			TotalRewards: make([]TotalReward, 0, len(filterIndicies)),
		}
	} else {
		response = &attestationsRewardsResponse{
			IdealRewards: make([]IdealReward, 0, validatorSet.Length()),
			TotalRewards: make([]TotalReward, 0, validatorSet.Length()),
		}
	}
	// make a map with the filter indicies
	totalActiveBalanceSqrt := utils.IntegerSquareRoot(totalActiveBalance)

	fn := func(index uint64, v solid.Validator) error {
		effectiveBalance := v.EffectiveBalance()
		baseReward := a.baseReward(version, effectiveBalance, totalActiveBalanceSqrt)
		// not eligible for rewards? then all empty
		if !(v.Active(prevEpoch) || (v.Slashed() && prevEpoch+1 < v.WithdrawableEpoch())) {
			response.IdealRewards = append(response.IdealRewards, IdealReward{EffectiveBalance: int64(effectiveBalance)})
			response.TotalRewards = append(response.TotalRewards, TotalReward{ValidatorIndex: int64(index)})
			return nil
		}
		idealReward := IdealReward{EffectiveBalance: int64(effectiveBalance)}
		totalReward := TotalReward{ValidatorIndex: int64(index)}
		if !inactivityLeak {
			idealReward.Head = int64(baseReward * rewardMultipliers[a.beaconChainCfg.TimelyHeadFlagIndex] / rewardDenominator)
			idealReward.Target = int64(baseReward * rewardMultipliers[a.beaconChainCfg.TimelyTargetFlagIndex] / rewardDenominator)
			idealReward.Source = int64(baseReward * rewardMultipliers[a.beaconChainCfg.TimelySourceFlagIndex] / rewardDenominator)
		}
		// Note: for altair, we don't have the inclusion delay, always 0.
		for flagIdx := range weights {
			if flagsUnslashedIndiciesSet[flagIdx][index] {
				if flagIdx == int(a.beaconChainCfg.TimelyHeadFlagIndex) {
					totalReward.Head = idealReward.Head
				} else if flagIdx == int(a.beaconChainCfg.TimelyTargetFlagIndex) {
					totalReward.Target = idealReward.Target
				} else if flagIdx == int(a.beaconChainCfg.TimelySourceFlagIndex) {
					totalReward.Source = idealReward.Source
				}
			} else if flagIdx != int(a.beaconChainCfg.TimelyHeadFlagIndex) {
				down := -int64(baseReward * weights[flagIdx] / a.beaconChainCfg.WeightDenominator)
				if flagIdx == int(a.beaconChainCfg.TimelyHeadFlagIndex) {
					totalReward.Head = down
				} else if flagIdx == int(a.beaconChainCfg.TimelyTargetFlagIndex) {
					totalReward.Target = down
				} else if flagIdx == int(a.beaconChainCfg.TimelySourceFlagIndex) {
					totalReward.Source = down
				}
			}
		}
		if !flagsUnslashedIndiciesSet[a.beaconChainCfg.TimelyTargetFlagIndex][index] {
			inactivityScore := inactivityScores.Get(int(index))
			totalReward.Inactivity = -int64((effectiveBalance * inactivityScore) / inactivityPenaltyDenominator)
		}
		response.IdealRewards = append(response.IdealRewards, idealReward)
		response.TotalRewards = append(response.TotalRewards, totalReward)
		return nil
	}

	if len(filterIndicies) > 0 {
		for _, index := range filterIndicies {
			if err := fn(index, validatorSet.Get(int(index))); err != nil {
				return nil, err
			}
		}
	} else {
		for index := uint64(0); index < uint64(validatorSet.Length()); index++ {
			if err := fn(index, validatorSet.Get(int(index))); err != nil {
				return nil, err
			}
		}
	}
	return newBeaconResponse(response), nil
}

// processRewardsAndPenaltiesPhase0 process rewards and penalties for phase0 state.
func (a *ApiHandler) computeAttestationsRewardsForPhase0(validatorSet *solid.ValidatorSet, finalityDelay, activeBalance uint64, previousParticipation *solid.ParticipationBitList, inactivityLeak bool, filterIndicies []uint64, epoch uint64) (*beaconhttp.BeaconResponse, error) {
	response := &attestationsRewardsResponse{}
	beaconConfig := a.beaconChainCfg
	if epoch == beaconConfig.GenesisEpoch {
		return newBeaconResponse(response), nil
	}
	prevEpoch := uint64(0)
	if epoch > 0 {
		prevEpoch = epoch - 1
	}
	if len(filterIndicies) > 0 {
		response = &attestationsRewardsResponse{
			IdealRewards: make([]IdealReward, 0, len(filterIndicies)),
			TotalRewards: make([]TotalReward, 0, len(filterIndicies)),
		}
	} else {
		response = &attestationsRewardsResponse{
			IdealRewards: make([]IdealReward, 0, validatorSet.Length()),
			TotalRewards: make([]TotalReward, 0, validatorSet.Length()),
		}
	}

	rewardDenominator := activeBalance / beaconConfig.EffectiveBalanceIncrement
	var unslashedMatchingSourceBalanceIncrements, unslashedMatchingTargetBalanceIncrements, unslashedMatchingHeadBalanceIncrements uint64
	var err error

	validatorSet.Range(func(i int, v solid.Validator, _ int) bool {
		if v.Slashed() {
			return true
		}
		var previousMatchingSourceAttester, previousMatchingTargetAttester, previousMatchingHeadAttester bool
		previousParticipation := cltypes.ParticipationFlags(previousParticipation.Get(i))
		previousMatchingHeadAttester = previousParticipation.HasFlag(int(beaconConfig.TimelyHeadFlagIndex))
		previousMatchingTargetAttester = previousParticipation.HasFlag(int(beaconConfig.TimelyTargetFlagIndex))
		previousMatchingSourceAttester = previousParticipation.HasFlag(int(beaconConfig.TimelySourceFlagIndex))

		if previousMatchingSourceAttester {
			unslashedMatchingSourceBalanceIncrements += v.EffectiveBalance()
		}
		if previousMatchingTargetAttester {
			unslashedMatchingTargetBalanceIncrements += v.EffectiveBalance()
		}
		if previousMatchingHeadAttester {
			unslashedMatchingHeadBalanceIncrements += v.EffectiveBalance()
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	// Then compute their total increment.
	unslashedMatchingSourceBalanceIncrements /= beaconConfig.EffectiveBalanceIncrement
	unslashedMatchingTargetBalanceIncrements /= beaconConfig.EffectiveBalanceIncrement
	unslashedMatchingHeadBalanceIncrements /= beaconConfig.EffectiveBalanceIncrement
	totalActiveBalanceSqrt := utils.IntegerSquareRoot(activeBalance)
	fn := func(index uint64, currentValidator solid.Validator) error {
		baseReward := a.baseReward(clparams.Phase0Version, currentValidator.EffectiveBalance(), totalActiveBalanceSqrt)

		if err != nil {
			return err
		}
		var previousMatchingSourceAttester, previousMatchingTargetAttester, previousMatchingHeadAttester bool

		previousParticipation := cltypes.ParticipationFlags(previousParticipation.Get(int(index)))
		previousMatchingHeadAttester = previousParticipation.HasFlag(int(beaconConfig.TimelyHeadFlagIndex))
		previousMatchingTargetAttester = previousParticipation.HasFlag(int(beaconConfig.TimelyTargetFlagIndex))
		previousMatchingSourceAttester = previousParticipation.HasFlag(int(beaconConfig.TimelySourceFlagIndex))

		totalReward := TotalReward{ValidatorIndex: int64(index)}
		idealReward := IdealReward{EffectiveBalance: int64(currentValidator.EffectiveBalance())}

		// TODO: check inclusion delay
		// if !currentValidator.Slashed() && previousMatchingSourceAttester {
		// 	var attestation *solid.PendingAttestation
		// 	if attestation, err = s.ValidatorMinPreviousInclusionDelayAttestation(int(index)); err != nil {
		// 		return err
		// 	}
		// 	proposerReward := (baseReward / beaconConfig.ProposerRewardQuotient)
		// 	maxAttesterReward := baseReward - proposerReward
		// 	idealReward.InclusionDelay = int64(maxAttesterReward / attestation.InclusionDelay())
		// 	totalReward.InclusionDelay = idealReward.InclusionDelay
		// }
		// if it is not eligible for rewards, then do not continue further
		if !(currentValidator.Active(prevEpoch) || (currentValidator.Slashed() && prevEpoch+1 < currentValidator.WithdrawableEpoch())) {
			response.IdealRewards = append(response.IdealRewards, idealReward)
			response.TotalRewards = append(response.TotalRewards, totalReward)
			return nil
		}
		if inactivityLeak {
			idealReward.Source = int64(baseReward)
			idealReward.Target = int64(baseReward)
			idealReward.Head = int64(baseReward)
		} else {
			idealReward.Source = int64(baseReward * unslashedMatchingSourceBalanceIncrements / rewardDenominator)
			idealReward.Target = int64(baseReward * unslashedMatchingTargetBalanceIncrements / rewardDenominator)
			idealReward.Head = int64(baseReward * unslashedMatchingHeadBalanceIncrements / rewardDenominator)
		}
		// we can use a multiplier to account for all attesting
		var attested, missed uint64
		if currentValidator.Slashed() {
			missed = 3
		} else {
			if previousMatchingSourceAttester {
				attested++
				totalReward.Source = idealReward.Source
			}
			if previousMatchingTargetAttester {
				attested++
				totalReward.Target = idealReward.Target
			}
			if previousMatchingHeadAttester {
				attested++
				totalReward.Head = idealReward.Head
			}
			missed = 3 - attested
		}
		// process inactivities
		if inactivityLeak {
			proposerReward := baseReward / beaconConfig.ProposerRewardQuotient
			totalReward.Inactivity = -int64(beaconConfig.BaseRewardsPerEpoch*baseReward - proposerReward)
			if currentValidator.Slashed() || !previousMatchingTargetAttester {
				totalReward.Inactivity -= int64(currentValidator.EffectiveBalance() * finalityDelay / beaconConfig.InactivityPenaltyQuotient)
			}
		}
		totalReward.Inactivity -= int64(baseReward * missed)
		response.IdealRewards = append(response.IdealRewards, idealReward)
		response.TotalRewards = append(response.TotalRewards, totalReward)
		return nil
	}
	if len(filterIndicies) > 0 {
		for _, index := range filterIndicies {
			v := validatorSet.Get(int(index))
			if err := fn(index, v); err != nil {
				return nil, err
			}
		}
	} else {
		for index := uint64(0); index < uint64(validatorSet.Length()); index++ {
			v := validatorSet.Get(int(index))
			if err := fn(index, v); err != nil {
				return nil, err
			}
		}
	}
	return newBeaconResponse(response), nil
}
