package handler

import (
	"encoding/hex"
	"net/http"
	"strconv"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
)

type validatorStatus string

const (
	validatorPendingInitialized validatorStatus = "pending_initialized"
	validatorPendingQueued      validatorStatus = "pending_queued"
	validatorActiveOngoing      validatorStatus = "active_ongoing"
	validatorActiveExiting      validatorStatus = "active_exiting"
	validatorActiveSlashed      validatorStatus = "active_slashed"
	validatorExitedUnslashed    validatorStatus = "exited_unslashed"
	validatorExitedSlashed      validatorStatus = "exited_slashed"
	validatorWithdrawalPossible validatorStatus = "withdrawal_possible"
	validatorWithdrawalDone     validatorStatus = "withdrawal_done"
	validatorActive             validatorStatus = "active"
	validatorPending            validatorStatus = "pending"
	validatorExited             validatorStatus = "exited"
	validatorWithdrawal         validatorStatus = "withdrawal"
)

const maxValidatorsLookupFilter = 32

func checkValidStatus(s string) *beaconhttp.EndpointError {
	switch validatorStatus(s) {
	case validatorPendingInitialized, validatorPendingQueued, validatorActiveOngoing, validatorActiveExiting, validatorActiveSlashed, validatorExitedUnslashed, validatorExitedSlashed, validatorWithdrawalPossible, validatorWithdrawalDone, validatorActive, validatorPending, validatorExited, validatorWithdrawal:
		return nil
	default:
		return beaconhttp.NewEndpointError(http.StatusBadRequest, "invalid validator status")
	}
}

func checkValidValidatorId(s string) (bool, *beaconhttp.EndpointError) {
	// If it starts with 0x, then it must a 48bytes 0x prefixed string
	if len(s) == 66 && s[:2] == "0x" {
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

	statusFilters, err := stringListFromQueryParams(r, "status")
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
	for _, status := range statusFilters {
		if err := checkValidStatus(status); err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err.Error())
		}
	}
	_ = blockRoot
	panic("implement me")
	// time to begin motherfucker
	// if blockId.head() { // Lets see if we point to head, if yes then we need to look at the head state we always keep.
	// }
	// return nil, nil
}
