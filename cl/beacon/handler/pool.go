package handler

import (
	"net/http"

	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
)

func (a *ApiHandler) GetEthV1BeaconPoolVoluntaryExits(w http.ResponseWriter, r *http.Request) (*beaconResponse, error) {
	return newBeaconResponse(a.operationsPool.VoluntaryExistsPool.Raw()), nil
}

func (a *ApiHandler) GetEthV1BeaconPoolAttesterSlashings(w http.ResponseWriter, r *http.Request) (*beaconResponse, error) {
	return newBeaconResponse(a.operationsPool.AttesterSlashingsPool.Raw()), nil
}

func (a *ApiHandler) GetEthV1BeaconPoolProposerSlashings(w http.ResponseWriter, r *http.Request) (*beaconResponse, error) {
	return newBeaconResponse(a.operationsPool.ProposerSlashingsPool.Raw()), nil
}

func (a *ApiHandler) GetEthV1BeaconPoolBLSExecutionChanges(w http.ResponseWriter, r *http.Request) (*beaconResponse, error) {
	return newBeaconResponse(a.operationsPool.BLSToExecutionChangesPool.Raw()), nil
}

func (a *ApiHandler) GetEthV1BeaconPoolAttestations(w http.ResponseWriter, r *http.Request) (*beaconResponse, error) {
	slot, err := uint64FromQueryParams(r, "slot")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err.Error())
	}
	committeeIndex, err := uint64FromQueryParams(r, "committee_index")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err.Error())
	}
	atts := a.operationsPool.AttestationsPool.Raw()
	if slot == nil && committeeIndex == nil {
		return newBeaconResponse(atts), nil
	}
	ret := make([]any, 0, len(atts))
	for i := range atts {
		if slot != nil && atts[i].AttestantionData().Slot() != *slot {
			continue
		}
		if committeeIndex != nil && atts[i].AttestantionData().ValidatorIndex() != *committeeIndex {
			continue
		}
		ret = append(ret, atts[i])
	}

	return newBeaconResponse(ret), nil
}
