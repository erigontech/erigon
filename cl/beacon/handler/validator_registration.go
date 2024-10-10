package handler

import (
	"encoding/json"
	"net/http"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

type ValidatorPreparationPayload struct {
	ValidatorIndex uint64            `json:"validator_index,string"`
	FeeRecipient   libcommon.Address `json:"fee_recipient"`
}

func (a *ApiHandler) PostEthV1ValidatorPrepareBeaconProposal(w http.ResponseWriter, r *http.Request) {
	req := []ValidatorPreparationPayload{}
	// decode request with json
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	for _, v := range req {
		a.logger.Debug("[Caplin] Registred new validator", "index", v.ValidatorIndex, "fee_recipient", v.FeeRecipient.String())
		a.validatorParams.SetFeeRecipient(v.ValidatorIndex, v.FeeRecipient)
	}
	w.WriteHeader(http.StatusOK)
}
