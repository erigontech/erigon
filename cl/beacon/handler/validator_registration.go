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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/common"
)

type ValidatorPreparationPayload struct {
	ValidatorIndex uint64         `json:"validator_index,string"`
	FeeRecipient   common.Address `json:"fee_recipient"`
}

func (p *ValidatorPreparationPayload) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for _, name := range []string{"validator_index", "fee_recipient"} {
		if raw, ok := fields[name]; !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("missing %s", name)
		}
	}
	type payload ValidatorPreparationPayload
	return json.Unmarshal(data, (*payload)(p))
}

func (a *ApiHandler) PostEthV1ValidatorPrepareBeaconProposal(w http.ResponseWriter, r *http.Request) {
	req, err := decodeJSONList[ValidatorPreparationPayload](r.Body)
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}
	for _, v := range req {
		a.logger.Trace("[Caplin] Registered new proposer", "index", v.ValidatorIndex, "fee_recipient", v.FeeRecipient.String())
		a.validatorParams.SetFeeRecipient(v.ValidatorIndex, v.FeeRecipient)
	}
	w.WriteHeader(http.StatusOK)
}
