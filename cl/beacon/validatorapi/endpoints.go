package validatorapi

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"unicode"

	"github.com/go-chi/chi/v5"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/utils"
)

func (v *ValidatorApiHandler) GetEthV1NodeSyncing(r *http.Request) (any, error) {
	_, slot, err := v.FC.GetHead()
	if err != nil {
		return nil, err
	}

	realHead := utils.GetCurrentSlot(v.GenesisCfg.GenesisTime, v.BeaconChainCfg.SecondsPerSlot)

	isSyncing := realHead > slot

	syncDistance := 0
	if isSyncing {
		syncDistance = int(realHead) - int(slot)
	}

	elOffline := true
	if v.FC.Engine() != nil {
		val, err := v.FC.Engine().Ready()
		if err == nil {
			elOffline = !val
		}
	}

	return map[string]any{
		"head_slot":     strconv.FormatUint(slot, 10),
		"sync_distance": syncDistance,
		"is_syncing":    isSyncing,
		"el_offline":    elOffline,
		// TODO: figure out how to populat this field
		"is_optimistic": true,
	}, nil
}

func (v *ValidatorApiHandler) EventSourceGetV1Events(w http.ResponseWriter, r *http.Request) {
}

func (v *ValidatorApiHandler) GetEthV1ConfigSpec(r *http.Request) (*clparams.BeaconChainConfig, error) {
	if v.BeaconChainCfg == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, "beacon config not found")
	}
	return v.BeaconChainCfg, nil
}

func (v *ValidatorApiHandler) GetEthV1BeaconGenesis(r *http.Request) (any, error) {
	if v.GenesisCfg == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, "genesis config not found")
	}
	digest, err := fork.ComputeForkDigest(v.BeaconChainCfg, v.GenesisCfg)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err.Error())
	}
	return map[string]any{
		"genesis_time":           v.GenesisCfg.GenesisTime,
		"genesis_validator_root": v.GenesisCfg.GenesisValidatorRoot,
		"genesis_fork_version":   hexutility.Bytes(digest[:]),
	}, nil
}

func (v *ValidatorApiHandler) GetEthV1BeaconStatesStateIdFork(r *http.Request) (any, error) {
	stateId := chi.URLParam(r, "state_id")
	state, err := v.privateGetStateFromStateId(stateId)
	if err != nil {
		return nil, err
	}
	isFinalized := state.Slot() <= v.FC.FinalizedSlot()
	forkData := state.BeaconState.Fork()
	return map[string]any{
		// TODO: this "True if the response references an unverified execution payload. "
		// figure out the condition where this happens
		"execution_optimistic": false,
		"finalized":            isFinalized,
		"data": map[string]any{
			"previous_version": hexutility.Bytes(forkData.PreviousVersion[:]),
			"current_version":  hexutility.Bytes(forkData.CurrentVersion[:]),
			"epoch":            strconv.Itoa(int(forkData.Epoch)),
		},
	}, nil
}
func (v *ValidatorApiHandler) GetEthV1BeaconStatesStateIdValidatorsValidatorId(r *http.Request) (any, error) {
	stateId := chi.URLParam(r, "state_id")
	// grab the correct state for the given state id
	beaconState, err := v.privateGetStateFromStateId(stateId)
	if err != nil {
		return nil, err
	}

	var validatorIndex uint64
	validatorId := chi.URLParam(r, "validator_id")
	switch {
	case strings.HasPrefix(validatorId, "0x"):
		// assume is hex has, so try to parse
		hsh := common.Bytes48{}
		err := hsh.UnmarshalText([]byte(stateId))
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Sprintf("Invalid validator ID: %s", validatorId))
		}
		val, ok := beaconState.ValidatorIndexByPubkey(hsh)
		if !ok {
			return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Sprintf("validator not found: %s", validatorId))
		}
		validatorIndex = val
	case isInt(validatorId):
		val, err := strconv.ParseUint(validatorId, 10, 64)
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Sprintf("Invalid validator ID: %s", validatorId))
		}
		validatorIndex = val
	default:
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Sprintf("Invalid validator ID: %s", validatorId))
	}
	// at this point validatorIndex is neccesarily assigned, so we can trust the zero value
	validator, err := beaconState.ValidatorForValidatorIndex(int(validatorIndex))
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Sprintf("validator not found at %s: %s ", stateId, validatorId))
	}
	validatorBalance, err := beaconState.ValidatorBalance(int(validatorIndex))
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Sprintf("balance not found at %s: %s ", stateId, validatorId))
	}

	//pending_initialized - When the first deposit is processed, but not enough funds are available (or not yet the end of the first epoch) to get validator into the activation queue.
	//pending_queued - When validator is waiting to get activated, and have enough funds etc. while in the queue, validator activation epoch keeps changing until it gets to the front and make it through (finalization is a requirement here too).
	//active_ongoing - When validator must be attesting, and have not initiated any exit.
	//active_exiting - When validator is still active, but filed a voluntary request to exit.
	//active_slashed - When validator is still active, but have a slashed status and is scheduled to exit.
	//exited_unslashed - When validator has reached regular exit epoch, not being slashed, and doesn't have to attest any more, but cannot withdraw yet.
	//exited_slashed - When validator has reached regular exit epoch, but was slashed, have to wait for a longer withdrawal period.
	//withdrawal_possible - After validator has exited, a while later is permitted to move funds, and is truly out of the system.
	//withdrawal_done - (not possible in phase0, except slashing full balance) - actually having moved funds away

	epoch := state.GetEpochAtSlot(v.BeaconChainCfg, beaconState.Slot())
	// TODO: figure out what is wrong and missing here
	validator_status := func() string {
		// see if validator has exited
		if validator.ExitEpoch() >= epoch {
			if validator.WithdrawableEpoch() >= epoch {
				// TODO: is this right? not sure if correct way to check for withdrawal_done
				if validatorBalance == 0 {
					return "withdrawal_done"
				}
				return "withdrawal_possible"
			}
			if validator.Slashed() {
				return "exited_slashed"
			}
			return "exited_unslashed"
		}
		// at this point we know they have not exited, so they are either active or pending
		if validator.Active(epoch) {
			// if active, figure out if they are slashed
			if validator.Slashed() {
				return "active_slashed"
			}
			if validator.ExitEpoch() != v.BeaconChainCfg.FarFutureEpoch {
				return "active_exiting"
			}
			return "active_ongoing"
		}
		// check if enough funds (TODO: or end of first epoch??)
		if validatorBalance >= v.BeaconChainCfg.MinDepositAmount {
			return "pending_initialized"
		}
		return "pending_queued"
	}()

	isFinalized := beaconState.Slot() <= v.FC.FinalizedSlot()
	return map[string]any{
		// TODO: this "True if the response references an unverified execution payload. "
		// figure out the condition where this happens
		"execution_optimistic": false,
		"finalized":            isFinalized,
		"data": map[string]any{
			"index":   strconv.FormatUint(validatorIndex, 10),
			"balance": strconv.FormatUint(validatorBalance, 10),
			"status":  validator_status,
			"data": map[string]any{
				"pubkey":                       hexutility.Bytes(validator.PublicKeyBytes()),
				"withdraw_credentials":         hexutility.Bytes(validator.WithdrawalCredentials().Bytes()),
				"effective_balance":            strconv.FormatUint(validator.EffectiveBalance(), 10),
				"slashed":                      validator.Slashed(),
				"activation_eligibility_epoch": strconv.FormatUint(validator.ActivationEligibilityEpoch(), 10),
				"activation_epoch":             strconv.FormatUint(validator.ActivationEpoch(), 10),
				"exit_epoch":                   strconv.FormatUint(validator.ActivationEpoch(), 10),
				"withdrawable_epoch":           strconv.FormatUint(validator.WithdrawableEpoch(), 10),
			},
		},
	}, nil
}

func (v *ValidatorApiHandler) privateGetStateFromStateId(stateId string) (*state.CachingBeaconState, error) {
	switch {
	case stateId == "head":
		// Now check the head
		headRoot, _, err := v.FC.GetHead()
		if err != nil {
			return nil, err
		}
		return v.FC.GetStateAtBlockRoot(headRoot, true)
	case stateId == "genesis":
		// not supported
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, "genesis block not found")
	case stateId == "finalized":
		return v.FC.GetStateAtBlockRoot(v.FC.FinalizedCheckpoint().BlockRoot(), true)
	case stateId == "justified":
		return v.FC.GetStateAtBlockRoot(v.FC.JustifiedCheckpoint().BlockRoot(), true)
	case strings.HasPrefix(stateId, "0x"):
		// assume is hex has, so try to parse
		hsh := common.Hash{}
		err := hsh.UnmarshalText([]byte(stateId))
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Sprintf("Invalid state ID: %s", stateId))
		}
		return v.FC.GetStateAtStateRoot(hsh, true)
	case isInt(stateId):
		// ignore the error bc isInt check succeeded. yes this doesn't protect for overflow, they will request slot 0 and it will fail. good
		val, _ := strconv.ParseUint(stateId, 10, 64)
		return v.FC.GetStateAtSlot(val, true)
	default:
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Sprintf("Invalid state ID: %s", stateId))
	}
}

func isInt(s string) bool {
	for _, c := range s {
		if !unicode.IsDigit(c) {
			return false
		}
	}
	return true
}
