package handler

import (
	"fmt"
	"net/http"

	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
)

func (a *ApiHandler) GetEthV1ValidatorAttestationData(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	slot, err := beaconhttp.Uint64FromQueryParams(r, "slot")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	committeeIndex, err := beaconhttp.Uint64FromQueryParams(r, "committee_index")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	if slot == nil || committeeIndex == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("slot and committee_index url params are required"))
	}
	headState := a.syncedData.HeadState()
	if headState == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable, fmt.Errorf("beacon node is still syncing"))
	}

	attestationData, err := a.attestationProducer.ProduceAndCacheAttestationData(headState, *slot, *committeeIndex)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusInternalServerError, err)
	}
	return newBeaconResponse(attestationData), nil
}
