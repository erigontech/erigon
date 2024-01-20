package validatorapi

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"unicode"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
)

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
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("genesis block not found"))
	case stateId == "finalized":
		return v.FC.GetStateAtBlockRoot(v.FC.FinalizedCheckpoint().BlockRoot(), true)
	case stateId == "justified":
		return v.FC.GetStateAtBlockRoot(v.FC.JustifiedCheckpoint().BlockRoot(), true)
	case strings.HasPrefix(stateId, "0x"):
		// assume is hex has, so try to parse
		hsh := common.Hash{}
		err := hsh.UnmarshalText([]byte(stateId))
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("Invalid state ID: %s", stateId))
		}
		return v.FC.GetStateAtStateRoot(hsh, true)
	case isInt(stateId):
		// ignore the error bc isInt check succeeded. yes this doesn't protect for overflow, they will request slot 0 and it will fail. good
		val, _ := strconv.ParseUint(stateId, 10, 64)
		return v.FC.GetStateAtSlot(val, true)
	default:
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("Invalid state ID: %s", stateId))
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
