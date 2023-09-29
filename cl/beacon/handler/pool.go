package handler

import (
	"net/http"

	"github.com/ledgerwatch/erigon/cl/clparams"
)

func (a *ApiHandler) poolVoluntaryExits(r *http.Request) (data any, finalized *bool, version *clparams.StateVersion, httpStatus int, err error) {
	httpStatus = http.StatusAccepted
	data = a.operationsPool.VoluntaryExistsPool.Raw()
	return
}

func (a *ApiHandler) poolAttesterSlashings(r *http.Request) (data any, finalized *bool, version *clparams.StateVersion, httpStatus int, err error) {
	httpStatus = http.StatusAccepted
	data = a.operationsPool.AttesterSlashingsPool.Raw()
	return
}

func (a *ApiHandler) poolProposerSlashings(r *http.Request) (data any, finalized *bool, version *clparams.StateVersion, httpStatus int, err error) {
	httpStatus = http.StatusAccepted
	data = a.operationsPool.ProposerSlashingsPool.Raw()
	return
}

func (a *ApiHandler) poolBlsToExecutionChanges(r *http.Request) (data any, finalized *bool, version *clparams.StateVersion, httpStatus int, err error) {
	httpStatus = http.StatusAccepted
	data = a.operationsPool.BLSToExecutionChangesPool.Raw()
	return
}

func (a *ApiHandler) poolAttestations(r *http.Request) (data any, finalized *bool, version *clparams.StateVersion, httpStatus int, err error) {
	httpStatus = http.StatusAccepted
	data = a.operationsPool.AttestationsPool.Raw()
	return
}
