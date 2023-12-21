package handler

import (
	"net/http"
)

func (a *ApiHandler) poolVoluntaryExits(r *http.Request) (*beaconResponse, error) {
	return newBeaconResponse(a.operationsPool.VoluntaryExistsPool.Raw()), nil
}

func (a *ApiHandler) poolAttesterSlashings(r *http.Request) (*beaconResponse, error) {
	return newBeaconResponse(a.operationsPool.AttesterSlashingsPool.Raw()), nil
}

func (a *ApiHandler) poolProposerSlashings(r *http.Request) (*beaconResponse, error) {
	return newBeaconResponse(a.operationsPool.ProposerSlashingsPool.Raw()), nil
}

func (a *ApiHandler) poolBlsToExecutionChanges(r *http.Request) (*beaconResponse, error) {
	return newBeaconResponse(a.operationsPool.BLSToExecutionChangesPool.Raw()), nil
}

func (a *ApiHandler) poolAttestations(r *http.Request) (*beaconResponse, error) {
	return newBeaconResponse(a.operationsPool.AttestationsPool.Raw()), nil
}
