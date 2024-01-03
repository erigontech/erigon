package handler

import (
	"net/http"
)

func (a *ApiHandler) poolVoluntaryExits(w http.ResponseWriter, r *http.Request) (*beaconResponse, error) {
	return newBeaconResponse(a.operationsPool.VoluntaryExistsPool.Raw()), nil
}

func (a *ApiHandler) poolAttesterSlashings(w http.ResponseWriter, r *http.Request) (*beaconResponse, error) {
	return newBeaconResponse(a.operationsPool.AttesterSlashingsPool.Raw()), nil
}

func (a *ApiHandler) poolProposerSlashings(w http.ResponseWriter, r *http.Request) (*beaconResponse, error) {
	return newBeaconResponse(a.operationsPool.ProposerSlashingsPool.Raw()), nil
}

func (a *ApiHandler) poolBlsToExecutionChanges(w http.ResponseWriter, r *http.Request) (*beaconResponse, error) {
	return newBeaconResponse(a.operationsPool.BLSToExecutionChangesPool.Raw()), nil
}

func (a *ApiHandler) poolAttestations(w http.ResponseWriter, r *http.Request) (*beaconResponse, error) {
	return newBeaconResponse(a.operationsPool.AttestationsPool.Raw()), nil
}
