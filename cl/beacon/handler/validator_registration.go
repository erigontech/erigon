// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package handler

import (
	"encoding/json"
	"net/http"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
)

type ValidatorPreparationPayload struct {
	ValidatorIndex uint64         `json:"validator_index,string"`
	FeeRecipient   common.Address `json:"fee_recipient"`
}

func (a *ApiHandler) PostEthV1ValidatorPrepareBeaconProposal(w http.ResponseWriter, r *http.Request) {
	req := []ValidatorPreparationPayload{}
	// decode request with json
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}
	for _, v := range req {
		a.logger.Trace("[Caplin] Registered new proposer", "index", v.ValidatorIndex, "fee_recipient", v.FeeRecipient.String())
		a.validatorParams.SetFeeRecipient(v.ValidatorIndex, v.FeeRecipient)
	}
	w.WriteHeader(http.StatusOK)
}
