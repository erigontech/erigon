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
	"errors"
	"net/http"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/db/kv"
)

type LighthouseValidatorInclusionGlobal struct {
	CurrentEpochActiveGwei           uint64 `json:"current_epoch_active_gwei"`
	PreviousEpochActiveGwei          uint64 `json:"previous_epoch_active_gwei"`
	CurrentEpochTargetAttestingGwei  uint64 `json:"current_epoch_target_attesting_gwei"`
	PreviousEpochTargetAttestingGwei uint64 `json:"previous_epoch_target_attesting_gwei"`
	PreviousEpochHeadAttestingGwei   uint64 `json:"previous_epoch_head_attesting_gwei"`
}

// the block root hash of the highest numbered slot that actually exists
func (a *ApiHandler) findEpochRoot(tx kv.Tx, epoch uint64) (common.Hash, error) {
	var currentBlockRoot common.Hash
	var err error
	for i := (epoch+1)*a.beaconChainCfg.SlotsPerEpoch - 1; i >= epoch*a.beaconChainCfg.SlotsPerEpoch; i-- {
		// read the block roots from the back
		currentBlockRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, i)
		if err != nil {
			return common.Hash{}, err
		}
		if currentBlockRoot != (common.Hash{}) {
			// stop at the first valid one
			return currentBlockRoot, nil
		}
	}
	// no non-missed slot was found, return the all zero hash
	return common.Hash{}, nil
}

func (a *ApiHandler) GetLighthouseValidatorInclusionGlobal(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	epoch, err := beaconhttp.EpochFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	ret, ok := a.lighthouseInclusionCache.Load(epoch)
	if ok {
		return newBeaconResponse(ret.(*LighthouseValidatorInclusionGlobal)), nil
	}
	// otherwise take data from historical states
	prevEpoch := epoch
	if prevEpoch > 0 {
		prevEpoch--
	}
	tx, err := a.indiciesDB.BeginRo(r.Context())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	snRoTx := a.caplinStateSnapshots.View()
	defer snRoTx.Close()
	stateGetter := state_accessors.GetValFnTxAndSnapshot(tx, snRoTx)

	slot := epoch * a.beaconChainCfg.SlotsPerEpoch
	if slot >= a.forkchoiceStore.LowestAvailableSlot() {
		// Take data from forkchoice
		root, err := a.findEpochRoot(tx, epoch)
		if err != nil {
			return nil, err
		}
		prevRoot, err := a.findEpochRoot(tx, prevEpoch)
		if err != nil {
			return nil, err
		}
		activeBalance, ok := a.forkchoiceStore.TotalActiveBalance(root)
		if !ok {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("active balance not found for current epoch"))
		}
		prevActiveBalance, ok := a.forkchoiceStore.TotalActiveBalance(prevRoot)
		if !ok {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("active balance not found for previous epoch"))
		}
		validatorSet, err := a.forkchoiceStore.GetValidatorSet(root)
		if err != nil {
			return nil, err
		}
		if validatorSet == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("validator set not found for current epoch"))
		}
		currentEpochParticipation, err := a.forkchoiceStore.GetCurrentParticipationIndicies(root)
		if err != nil {
			return nil, err
		}
		if currentEpochParticipation == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("participation not found for current epoch"))
		}
		previousEpochParticipation, err := a.forkchoiceStore.GetPreviousParticipationIndicies(root)
		if err != nil {
			return nil, err
		}
		if previousEpochParticipation == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("participation not found for previous epoch"))
		}
		return newBeaconResponse(a.computeLighthouseValidatorInclusionGlobal(epoch, activeBalance, prevActiveBalance, validatorSet, currentEpochParticipation, previousEpochParticipation)), nil
	}

	// read the epoch datas first
	epochData, err := state_accessors.ReadEpochData(stateGetter, epoch*a.beaconChainCfg.SlotsPerEpoch, a.beaconChainCfg)
	if err != nil {
		return nil, err
	}
	if epochData == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("epoch data not found for current epoch"))
	}
	prevEpochData, err := state_accessors.ReadEpochData(stateGetter, prevEpoch*a.beaconChainCfg.SlotsPerEpoch, a.beaconChainCfg)
	if err != nil {
		return nil, err
	}
	if prevEpochData == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("epoch data not found for previous epoch"))
	}

	// read the validator set
	validatorSet, err := a.stateReader.ReadValidatorsForHistoricalState(tx, stateGetter, slot)
	if err != nil {
		return nil, err
	}
	if validatorSet == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("validator set not found for current epoch"))
	}
	currentEpochParticipation, previousEpochParticipation, err := a.stateReader.ReadParticipations(tx, stateGetter, slot+(a.beaconChainCfg.SlotsPerEpoch-1))
	if err != nil {
		return nil, err
	}
	if currentEpochParticipation == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("participation not found for current epoch"))
	}
	if previousEpochParticipation == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("participation not found for previous epoch"))
	}
	return newBeaconResponse(a.computeLighthouseValidatorInclusionGlobal(epoch, epochData.TotalActiveBalance, prevEpochData.TotalActiveBalance, validatorSet, currentEpochParticipation, previousEpochParticipation)), nil
}

func (a *ApiHandler) computeLighthouseValidatorInclusionGlobal(epoch, currentActiveGwei, previousActiveGwei uint64, validatorSet *solid.ValidatorSet, currentEpochParticipation, previousEpochParticipation *solid.ParticipationBitList) *LighthouseValidatorInclusionGlobal {
	var currentEpochTargetAttestingGwei, previousEpochTargetAttestingGwei, previousEpochHeadAttestingGwei uint64
	for i := 0; i < validatorSet.Length(); i++ {
		validatorBalance := validatorSet.Get(i).EffectiveBalance()
		prevFlags := cltypes.ParticipationFlags(previousEpochParticipation.Get(i))
		currFlags := cltypes.ParticipationFlags(currentEpochParticipation.Get(i))
		if prevFlags.HasFlag(int(a.beaconChainCfg.TimelyHeadFlagIndex)) {
			previousEpochHeadAttestingGwei += validatorBalance
		}
		if currFlags.HasFlag(int(a.beaconChainCfg.TimelyTargetFlagIndex)) {
			currentEpochTargetAttestingGwei += validatorBalance
		}
		if prevFlags.HasFlag(int(a.beaconChainCfg.TimelyTargetFlagIndex)) {
			previousEpochTargetAttestingGwei += validatorBalance
		}
	}
	ret := &LighthouseValidatorInclusionGlobal{
		CurrentEpochActiveGwei:           currentActiveGwei,
		PreviousEpochActiveGwei:          previousActiveGwei,
		CurrentEpochTargetAttestingGwei:  currentEpochTargetAttestingGwei,
		PreviousEpochTargetAttestingGwei: previousEpochTargetAttestingGwei,
		PreviousEpochHeadAttestingGwei:   previousEpochHeadAttestingGwei,
	}
	a.lighthouseInclusionCache.Store(epoch, ret)

	return ret
}

// {
// 	"data": {
// 	  "is_slashed": false,
// 	  "is_withdrawable_in_current_epoch": false,
// 	  "is_active_unslashed_in_current_epoch": true,
// 	  "is_active_unslashed_in_previous_epoch": true,
// 	  "current_epoch_effective_balance_gwei": 32000000000,
// 	  "is_current_epoch_target_attester": false,
// 	  "is_previous_epoch_target_attester": false,
// 	  "is_previous_epoch_head_attester": false
// 	}
//   }

type LighthouseValidatorInclusion struct {
	IsSlashed                        bool   `json:"is_slashed"`
	IsWithdrawableInCurrentEpoch     bool   `json:"is_withdrawable_in_current_epoch"`
	IsActiveUnslashedInCurrentEpoch  bool   `json:"is_active_unslashed_in_current_epoch"`
	IsActiveUnslashedInPreviousEpoch bool   `json:"is_active_unslashed_in_previous_epoch"`
	CurrentEpochEffectiveBalanceGwei uint64 `json:"current_epoch_effective_balance_gwei"`
	IsCurrentEpochTargetAttester     bool   `json:"is_current_epoch_target_attester"`
	IsPreviousEpochTargetAttester    bool   `json:"is_previous_epoch_target_attester"`
	IsPreviousEpochHeadAttester      bool   `json:"is_previous_epoch_head_attester"`
}

func (a *ApiHandler) GetLighthouseValidatorInclusion(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	epoch, err := beaconhttp.EpochFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	tx, err := a.indiciesDB.BeginRo(r.Context())
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	validatorId, err := beaconhttp.StringFromRequest(r, "validator_id")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}

	validatorIndex, err := parseQueryValidatorIndex(a.syncedData, validatorId)
	if err != nil {
		return nil, err
	}

	// otherwise take data from historical states
	prevEpoch := epoch
	if prevEpoch > 0 {
		prevEpoch--
	}

	slot := epoch * a.beaconChainCfg.SlotsPerEpoch
	if slot >= a.forkchoiceStore.LowestAvailableSlot() {
		// Take data from forkchoice
		root, err := a.findEpochRoot(tx, epoch)
		if err != nil {
			return nil, err
		}
		prevRoot, err := a.findEpochRoot(tx, prevEpoch)
		if err != nil {
			return nil, err
		}
		activeBalance, ok := a.forkchoiceStore.TotalActiveBalance(root)
		if !ok {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("active balance not found for current epoch"))
		}
		prevActiveBalance, ok := a.forkchoiceStore.TotalActiveBalance(prevRoot)
		if !ok {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("active balance not found for previous epoch"))
		}
		validatorSet, err := a.forkchoiceStore.GetValidatorSet(root)
		if err != nil {
			return nil, err
		}
		if validatorSet == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("validator set not found for current epoch"))
		}
		currentEpochParticipation, err := a.forkchoiceStore.GetCurrentParticipationIndicies(root)
		if err != nil {
			return nil, err
		}
		if currentEpochParticipation == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("participation not found for current epoch"))
		}
		previousEpochParticipation, err := a.forkchoiceStore.GetPreviousParticipationIndicies(root)
		if err != nil {
			return nil, err
		}
		if previousEpochParticipation == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("participation not found for previous epoch"))
		}
		return newBeaconResponse(a.computeLighthouseValidatorInclusion(int(validatorIndex), prevEpoch, epoch, activeBalance, prevActiveBalance, validatorSet, currentEpochParticipation, previousEpochParticipation)), nil
	}

	snRoTx := a.caplinStateSnapshots.View()
	defer snRoTx.Close()
	stateGetter := state_accessors.GetValFnTxAndSnapshot(tx, snRoTx)
	// read the epoch datas first
	epochData, err := state_accessors.ReadEpochData(stateGetter, epoch*a.beaconChainCfg.SlotsPerEpoch, a.beaconChainCfg)
	if err != nil {
		return nil, err
	}
	if epochData == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("epoch data not found for current epoch"))
	}
	prevEpochData, err := state_accessors.ReadEpochData(stateGetter, prevEpoch*a.beaconChainCfg.SlotsPerEpoch, a.beaconChainCfg)
	if err != nil {
		return nil, err
	}
	if prevEpochData == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("epoch data not found for previous epoch"))
	}
	// read the validator set
	validatorSet, err := a.stateReader.ReadValidatorsForHistoricalState(tx, stateGetter, slot)
	if err != nil {
		return nil, err
	}
	if validatorSet == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("validator set not found for current epoch"))
	}
	currentEpochParticipation, previousEpochParticipation, err := a.stateReader.ReadParticipations(tx, stateGetter, slot+(a.beaconChainCfg.SlotsPerEpoch-1))
	if err != nil {
		return nil, err
	}
	if currentEpochParticipation == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("participation not found for current epoch"))
	}
	if previousEpochParticipation == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, errors.New("participation not found for previous epoch"))
	}
	return newBeaconResponse(a.computeLighthouseValidatorInclusion(int(validatorIndex), prevEpoch, epoch, epochData.TotalActiveBalance, prevEpochData.TotalActiveBalance, validatorSet, currentEpochParticipation, previousEpochParticipation)), nil
}

func (a *ApiHandler) computeLighthouseValidatorInclusion(idx int, prevEpoch, epoch, currentActiveGwei, previousActiveGwei uint64, validatorSet *solid.ValidatorSet, currentEpochParticipation, previousEpochParticipation *solid.ParticipationBitList) *LighthouseValidatorInclusion {
	var currentEpochTargetAttestingGwei, previousEpochTargetAttestingGwei, previousEpochHeadAttestingGwei uint64
	for i := 0; i < validatorSet.Length(); i++ {
		validatorBalance := validatorSet.Get(i).EffectiveBalance()
		prevFlags := cltypes.ParticipationFlags(previousEpochParticipation.Get(i))
		currFlags := cltypes.ParticipationFlags(currentEpochParticipation.Get(i))
		if prevFlags.HasFlag(int(a.beaconChainCfg.TimelyHeadFlagIndex)) {
			previousEpochHeadAttestingGwei += validatorBalance
		}
		if currFlags.HasFlag(int(a.beaconChainCfg.TimelyTargetFlagIndex)) {
			currentEpochTargetAttestingGwei += validatorBalance
		}
		if prevFlags.HasFlag(int(a.beaconChainCfg.TimelyTargetFlagIndex)) {
			previousEpochTargetAttestingGwei += validatorBalance
		}
	}
	validator := validatorSet.Get(idx)
	prevFlags := cltypes.ParticipationFlags(previousEpochParticipation.Get(idx))
	currFlags := cltypes.ParticipationFlags(currentEpochParticipation.Get(idx))

	return &LighthouseValidatorInclusion{
		IsSlashed:                        validator.Slashed(),
		IsWithdrawableInCurrentEpoch:     epoch >= validator.WithdrawableEpoch(),
		IsActiveUnslashedInCurrentEpoch:  validator.Active(epoch) && !validator.Slashed(),
		IsActiveUnslashedInPreviousEpoch: validator.Active(prevEpoch) && !validator.Slashed(),
		CurrentEpochEffectiveBalanceGwei: validator.EffectiveBalance(),
		IsCurrentEpochTargetAttester:     currFlags.HasFlag(int(a.beaconChainCfg.TimelyTargetFlagIndex)),
		IsPreviousEpochTargetAttester:    prevFlags.HasFlag(int(a.beaconChainCfg.TimelyTargetFlagIndex)),
		IsPreviousEpochHeadAttester:      prevFlags.HasFlag(int(a.beaconChainCfg.TimelyHeadFlagIndex)),
	}
}
