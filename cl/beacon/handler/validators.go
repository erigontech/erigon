package handler

import (
	"encoding/hex"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"golang.org/x/exp/slices"
)

type validatorStatus int

const (
	validatorPendingInitialized validatorStatus = 1  //"pending_initialized"
	validatorPendingQueued      validatorStatus = 2  //"pending_queued"
	validatorActiveOngoing      validatorStatus = 3  //"active_ongoing"
	validatorActiveExiting      validatorStatus = 4  //"active_exiting"
	validatorActiveSlashed      validatorStatus = 5  //"active_slashed"
	validatorExitedUnslashed    validatorStatus = 6  //"exited_unslashed"
	validatorExitedSlashed      validatorStatus = 7  //"exited_slashed"
	validatorWithdrawalPossible validatorStatus = 8  //"withdrawal_possible"
	validatorWithdrawalDone     validatorStatus = 9  //"withdrawal_done"
	validatorActive             validatorStatus = 10 //"active"
	validatorPending            validatorStatus = 11 //"pending"
	validatorExited             validatorStatus = 12 //"exited"
	validatorWithdrawal         validatorStatus = 13 //"withdrawal"
)

func validatorStatusFromString(s string) (validatorStatus, error) {
	switch s {
	case "pending_initialized":
		return validatorPendingInitialized, nil
	case "pending_queued":
		return validatorPendingQueued, nil
	case "active_ongoing":
		return validatorActiveOngoing, nil
	case "active_exiting":
		return validatorActiveExiting, nil
	case "active_slashed":
		return validatorActiveSlashed, nil
	case "exited_unslashed":
		return validatorExitedUnslashed, nil
	case "exited_slashed":
		return validatorExitedSlashed, nil
	case "withdrawal_possible":
		return validatorWithdrawalPossible, nil
	case "withdrawal_done":
		return validatorWithdrawalDone, nil
	case "active":
		return validatorActive, nil
	case "pending":
		return validatorPending, nil
	case "exited":
		return validatorExited, nil
	case "withdrawal":
		return validatorWithdrawal, nil
	default:
		return 0, fmt.Errorf("invalid validator status %s", s)
	}
}

func validatorStatusFromValidator(v solid.Validator, currentEpoch uint64, balance uint64) validatorStatus {
	activationEpoch := v.ActivationEpoch()
	// pending section
	if activationEpoch > currentEpoch {
		activationEligibilityEpoch := v.ActivationEligibilityEpoch()
		if activationEligibilityEpoch == math.MaxUint64 {
			return validatorPendingInitialized
		}
		return validatorPendingQueued
	}

	exitEpoch := v.ExitEpoch()
	// active section
	if activationEpoch <= currentEpoch && currentEpoch < exitEpoch {
		if exitEpoch == math.MaxUint64 {
			return validatorActiveOngoing
		}
		slashed := v.Slashed()
		if slashed {
			return validatorActiveSlashed
		}
		return validatorActiveExiting
	}

	withdrawableEpoch := v.WithdrawableEpoch()
	// exited section
	if exitEpoch <= currentEpoch && currentEpoch < withdrawableEpoch {
		if v.Slashed() {
			return validatorExitedSlashed
		}
		return validatorExitedUnslashed
	}

	if balance == 0 {
		return validatorWithdrawalDone
	}
	return validatorWithdrawalPossible

}

func (s validatorStatus) String() string {
	switch s {
	case validatorPendingInitialized:
		return "pending_initialized"
	case validatorPendingQueued:
		return "pending_queued"
	case validatorActiveOngoing:
		return "active_ongoing"
	case validatorActiveExiting:
		return "active_exiting"
	case validatorActiveSlashed:
		return "active_slashed"
	case validatorExitedUnslashed:
		return "exited_unslashed"
	case validatorExitedSlashed:
		return "exited_slashed"
	case validatorWithdrawalPossible:
		return "withdrawal_possible"
	case validatorWithdrawalDone:
		return "withdrawal_done"
	case validatorActive:
		return "active"
	case validatorPending:
		return "pending"
	case validatorExited:
		return "exited"
	case validatorWithdrawal:
		return "withdrawal"
	default:
		panic("invalid validator status")
	}
}

const maxValidatorsLookupFilter = 32

func parseStatuses(s []string) ([]validatorStatus, error) {
	seenAlready := make(map[validatorStatus]struct{})
	statuses := make([]validatorStatus, 0, len(s))

	if len(s) > maxValidatorsLookupFilter {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, "too many statuses requested")
	}

	for _, status := range s {
		s, err := validatorStatusFromString(status)
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err.Error())
		}
		if _, ok := seenAlready[s]; ok {
			continue
		}
		seenAlready[s] = struct{}{}
		statuses = append(statuses, s)
	}
	return statuses, nil
}

func checkValidValidatorId(s string) (bool, *beaconhttp.EndpointError) {
	// If it starts with 0x, then it must a 48bytes 0x prefixed string
	if len(s) == 98 && s[:2] == "0x" {
		// check if it is a valid hex string
		if _, err := hex.DecodeString(s[2:]); err != nil {
			return false, beaconhttp.NewEndpointError(http.StatusBadRequest, "invalid validator id")
		}
		return true, nil
	}
	// If it is not 0x prefixed, then it must be a number, check if it is a base-10 number
	if _, err := strconv.ParseUint(s, 10, 64); err != nil {
		return false, beaconhttp.NewEndpointError(http.StatusBadRequest, "invalid validator id")
	}
	return false, nil
}

func (a *ApiHandler) getAllValidators(r *http.Request) (*beaconResponse, error) {
	ctx := r.Context()

	tx, err := a.indiciesDB.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockId, err := stateIdFromRequest(r)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err.Error())
	}

	blockRoot, httpStatus, err := a.blockRootFromStateId(ctx, tx, blockId)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(httpStatus, err.Error())
	}

	queryFilters, err := stringListFromQueryParams(r, "status")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err.Error())
	}

	validatorIds, err := stringListFromQueryParams(r, "id")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err.Error())
	}

	if len(validatorIds) > maxValidatorsLookupFilter {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, "too many validators requested")
	}
	filterIndicies := make([]uint64, 0, len(validatorIds))

	for _, id := range validatorIds {
		isPublicKey, err := checkValidValidatorId(id)
		if err != nil {
			return nil, err
		}
		if isPublicKey {
			var b48 libcommon.Bytes48
			if err := b48.UnmarshalText([]byte(id[2:])); err != nil {
				return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err.Error())
			}
			idx, err := state_accessors.ReadValidatorIndexByPublicKey(tx, b48)
			if err != nil {
				return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err.Error())
			}
			filterIndicies = append(filterIndicies, idx)
		} else {
			idx, err := strconv.ParseUint(id, 10, 64)
			if err != nil {
				return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err.Error())
			}
			filterIndicies = append(filterIndicies, idx)
		}
	}
	// Check the filters' validity
	statusFilters, err := parseStatuses(queryFilters)
	if err != nil {
		return nil, err
	}

	if blockId.head() { // Lets see if we point to head, if yes then we need to look at the head state we always keep.
		s, cn := a.syncedData.HeadState()
		defer cn()
		return responseValidators(filterIndicies, statusFilters, state.Epoch(s), s.Balances(), s.Validators(), false)
	}
	slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
	if err != nil {
		return nil, err
	}

	if slot == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, "state not found")
	}
	stateEpoch := *slot / a.beaconChainCfg.SlotsPerEpoch
	state, err := a.forkchoiceStore.GetStateAtBlockRoot(blockRoot, true)
	if err != nil {
		return nil, err
	}
	if state == nil {
		validatorSet, balances, err := a.stateReader.ReadValidatorsData(tx, *slot)
		if err != nil {
			return nil, err
		}
		return responseValidators(filterIndicies, statusFilters, stateEpoch, balances, validatorSet, true)
	}
	return responseValidators(filterIndicies, statusFilters, stateEpoch, state.Balances(), state.Validators(), *slot <= a.forkchoiceStore.FinalizedSlot())
}

type directString string

func (d directString) MarshalJSON() ([]byte, error) {
	return []byte(d), nil
}

func responseValidators(filterIndicies []uint64, filterStatuses []validatorStatus, stateEpoch uint64, balances solid.Uint64ListSSZ, validators *solid.ValidatorSet, finalized bool) (*beaconResponse, error) {
	var b strings.Builder
	b.WriteString("[")
	jsonTemplate := "{\"index\":%d,\"status\":\"%s\",\"balance\":%d,\"validator\":{\"pubkey\":\"%x\",\"withdrawal_credentials\":\"%x\",\"effective_balance\":%d,\"slashed\":%t,\"activation_eligibility_epoch\":%d,\"activation_epoch\":%d,\"exit_epoch\":%d,\"withdrawable_epoch\":%d}}"
	first := true
	var err error
	validators.Range(func(i int, v solid.Validator, l int) bool {
		if len(filterIndicies) > 0 && !slices.Contains(filterIndicies, uint64(i)) {
			return true
		}
		status := validatorStatusFromValidator(v, stateEpoch, balances.Get(i))
		if shouldStatusBeFiltered(status, filterStatuses) {
			return true
		}
		if !first {
			if _, err = b.WriteString(","); err != nil {
				return false
			}
		}
		first = false
		if _, err = b.WriteString(fmt.Sprintf(jsonTemplate, i, status.String(), balances.Get(i), v.PublicKey(), v.WithdrawalCredentials().String(), v.EffectiveBalance(), v.Slashed(), v.ActivationEligibilityEpoch(), v.ActivationEpoch(), v.ExitEpoch(), v.WithdrawableEpoch())); err != nil {
			return false
		}
		return true
	})
	if err != nil {
		return nil, err
	}

	_, err = b.WriteString("]\n")

	return newBeaconResponse(directString(b.String())).withFinalized(finalized), err
}

func shouldStatusBeFiltered(status validatorStatus, statuses []validatorStatus) bool {
	if len(statuses) == 0 {
		return false
	}
	for _, s := range statuses {
		if s == validatorActive && (status == validatorActiveOngoing || status == validatorActiveExiting || status == validatorActiveSlashed) {
			return false
		}
		if s == validatorPending && (status == validatorPendingInitialized || status == validatorPendingQueued) {
			return false
		}
		if s == validatorExited && (status == validatorExitedUnslashed || status == validatorExitedSlashed) {
			return false
		}
		if s == validatorWithdrawal && (status == validatorWithdrawalPossible || status == validatorWithdrawalDone) {
			return false
		}
		if s == status {
			return false
		}
	}
	return true // filter if no filter condition is met
}
