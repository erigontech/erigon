package handler

import (
	"net/http"
)

func (a *ApiHandler) poolVoluntaryExits(r *http.Request) *beaconResponse {
	return newBeaconResponse(a.operationsPool.VoluntaryExistsPool.Raw())
}

func (a *ApiHandler) poolAttesterSlashings(r *http.Request) *beaconResponse {
	return newBeaconResponse(a.operationsPool.AttesterSlashingsPool.Raw())
}

func (a *ApiHandler) poolProposerSlashings(r *http.Request) *beaconResponse {
	return newBeaconResponse(a.operationsPool.ProposerSlashingsPool.Raw())
}

func (a *ApiHandler) poolBlsToExecutionChanges(r *http.Request) *beaconResponse {
	return newBeaconResponse(a.operationsPool.BLSToExecutionChangesPool.Raw())
}

func (a *ApiHandler) poolAttestations(r *http.Request) *beaconResponse {
	return newBeaconResponse(a.operationsPool.AttestationsPool.Raw())
}
