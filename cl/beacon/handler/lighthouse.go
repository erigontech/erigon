package handler

import (
	"fmt"
	"net/http"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
)

type LighthouseValidatorInclusionGlobal struct {
	CurrentEpochActiveGwei           uint64 `json:"current_epoch_active_gwei"`
	PreviousEpochActiveGwei          uint64 `json:"previous_epoch_active_gwei"`
	CurrentEpochTargetAttestingGwei  uint64 `json:"current_epoch_target_attesting_gwei"`
	PreviousEpochTargetAttestingGwei uint64 `json:"previous_epoch_target_attesting_gwei"`
	PreviousEpochHeadAttestingGwei   uint64 `json:"previous_epoch_head_attesting_gwei"`
}

func (a *ApiHandler) findEpochRoot(tx kv.Tx, epoch uint64) (common.Hash, error) {
	var currentBlockRoot common.Hash
	var err error
	for i := epoch * a.beaconChainCfg.SlotsPerEpoch; i < (epoch+1)*a.beaconChainCfg.SlotsPerEpoch; i++ {
		// read the block root
		currentBlockRoot, err = beacon_indicies.ReadCanonicalBlockRoot(tx, i)
		if err != nil {
			return common.Hash{}, err
		}
	}
	return currentBlockRoot, nil

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

	slot := epoch * a.beaconChainCfg.SlotsPerEpoch
	if slot >= a.forkchoiceStore.LowestAvaiableSlot() {
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
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("active balance not found for current epoch"))
		}
		prevActiveBalance, ok := a.forkchoiceStore.TotalActiveBalance(prevRoot)
		if !ok {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("active balance not found for previous epoch"))
		}
		validatorSet, err := a.forkchoiceStore.GetValidatorSet(root)
		if err != nil {
			return nil, err
		}
		if validatorSet == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("validator set not found for current epoch"))
		}
		currentEpochPartecipation, err := a.forkchoiceStore.GetCurrentPartecipationIndicies(root)
		if err != nil {
			return nil, err
		}
		if currentEpochPartecipation == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("partecipation not found for current epoch"))
		}
		previousEpochPartecipation, err := a.forkchoiceStore.GetPreviousPartecipationIndicies(root)
		if err != nil {
			return nil, err
		}
		if previousEpochPartecipation == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("partecipation not found for previous epoch"))
		}
		return newBeaconResponse(a.computeLighthouseValidatorInclusionGlobal(epoch, activeBalance, prevActiveBalance, validatorSet, currentEpochPartecipation, previousEpochPartecipation)), nil
	}

	// read the epoch datas first
	epochData, err := state_accessors.ReadEpochData(tx, epoch*a.beaconChainCfg.SlotsPerEpoch)
	if err != nil {
		return nil, err
	}
	if epochData == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("epoch data not found for current epoch"))
	}
	prevEpochData, err := state_accessors.ReadEpochData(tx, prevEpoch*a.beaconChainCfg.SlotsPerEpoch)
	if err != nil {
		return nil, err
	}
	if prevEpochData == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("epoch data not found for previous epoch"))
	}
	// read the validator set
	validatorSet, err := a.stateReader.ReadValidatorsForHistoricalState(tx, slot)
	if err != nil {
		return nil, err
	}
	if validatorSet == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("validator set not found for current epoch"))
	}
	currentEpochPartecipation, previousEpochPartecipation, err := a.stateReader.ReadPartecipations(tx, slot+(a.beaconChainCfg.SlotsPerEpoch-1))
	if err != nil {
		return nil, err
	}
	if currentEpochPartecipation == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("partecipation not found for current epoch"))
	}
	if previousEpochPartecipation == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("partecipation not found for previous epoch"))
	}
	return newBeaconResponse(a.computeLighthouseValidatorInclusionGlobal(epoch, epochData.TotalActiveBalance, prevEpochData.TotalActiveBalance, validatorSet, currentEpochPartecipation, previousEpochPartecipation)), nil
}

func (a *ApiHandler) computeLighthouseValidatorInclusionGlobal(epoch, currentActiveGwei, previousActiveGwei uint64, validatorSet *solid.ValidatorSet, currentEpochPartecipation, previousEpochPartecipation *solid.BitList) *LighthouseValidatorInclusionGlobal {
	var currentEpochTargetAttestingGwei, previousEpochTargetAttestingGwei, previousEpochHeadAttestingGwei uint64
	for i := 0; i < validatorSet.Length(); i++ {
		validatorBalance := validatorSet.Get(i).EffectiveBalance()
		prevFlags := cltypes.ParticipationFlags(previousEpochPartecipation.Get(i))
		currFlags := cltypes.ParticipationFlags(currentEpochPartecipation.Get(i))
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

	validatorIndex, err := parseQueryValidatorIndex(tx, validatorId)
	if err != nil {
		return nil, err
	}

	// otherwise take data from historical states
	prevEpoch := epoch
	if prevEpoch > 0 {
		prevEpoch--
	}

	slot := epoch * a.beaconChainCfg.SlotsPerEpoch
	if slot >= a.forkchoiceStore.LowestAvaiableSlot() {
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
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("active balance not found for current epoch"))
		}
		prevActiveBalance, ok := a.forkchoiceStore.TotalActiveBalance(prevRoot)
		if !ok {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("active balance not found for previous epoch"))
		}
		validatorSet, err := a.forkchoiceStore.GetValidatorSet(root)
		if err != nil {
			return nil, err
		}
		if validatorSet == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("validator set not found for current epoch"))
		}
		currentEpochPartecipation, err := a.forkchoiceStore.GetCurrentPartecipationIndicies(root)
		if err != nil {
			return nil, err
		}
		if currentEpochPartecipation == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("partecipation not found for current epoch"))
		}
		previousEpochPartecipation, err := a.forkchoiceStore.GetPreviousPartecipationIndicies(root)
		if err != nil {
			return nil, err
		}
		if previousEpochPartecipation == nil {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("partecipation not found for previous epoch"))
		}
		return newBeaconResponse(a.computeLighthouseValidatorInclusion(int(validatorIndex), prevEpoch, epoch, activeBalance, prevActiveBalance, validatorSet, currentEpochPartecipation, previousEpochPartecipation)), nil
	}

	// read the epoch datas first
	epochData, err := state_accessors.ReadEpochData(tx, epoch*a.beaconChainCfg.SlotsPerEpoch)
	if err != nil {
		return nil, err
	}
	if epochData == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("epoch data not found for current epoch"))
	}
	prevEpochData, err := state_accessors.ReadEpochData(tx, prevEpoch*a.beaconChainCfg.SlotsPerEpoch)
	if err != nil {
		return nil, err
	}
	if prevEpochData == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("epoch data not found for previous epoch"))
	}
	// read the validator set
	validatorSet, err := a.stateReader.ReadValidatorsForHistoricalState(tx, slot)
	if err != nil {
		return nil, err
	}
	if validatorSet == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("validator set not found for current epoch"))
	}
	currentEpochPartecipation, previousEpochPartecipation, err := a.stateReader.ReadPartecipations(tx, slot+(a.beaconChainCfg.SlotsPerEpoch-1))
	if err != nil {
		return nil, err
	}
	if currentEpochPartecipation == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("partecipation not found for current epoch"))
	}
	if previousEpochPartecipation == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("partecipation not found for previous epoch"))
	}
	return newBeaconResponse(a.computeLighthouseValidatorInclusion(int(validatorIndex), prevEpoch, epoch, epochData.TotalActiveBalance, prevEpochData.TotalActiveBalance, validatorSet, currentEpochPartecipation, previousEpochPartecipation)), nil
}

func (a *ApiHandler) computeLighthouseValidatorInclusion(idx int, prevEpoch, epoch, currentActiveGwei, previousActiveGwei uint64, validatorSet *solid.ValidatorSet, currentEpochPartecipation, previousEpochPartecipation *solid.BitList) *LighthouseValidatorInclusion {
	var currentEpochTargetAttestingGwei, previousEpochTargetAttestingGwei, previousEpochHeadAttestingGwei uint64
	for i := 0; i < validatorSet.Length(); i++ {
		validatorBalance := validatorSet.Get(i).EffectiveBalance()
		prevFlags := cltypes.ParticipationFlags(previousEpochPartecipation.Get(i))
		currFlags := cltypes.ParticipationFlags(currentEpochPartecipation.Get(i))
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
	prevFlags := cltypes.ParticipationFlags(previousEpochPartecipation.Get(idx))
	currFlags := cltypes.ParticipationFlags(currentEpochPartecipation.Get(idx))

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
