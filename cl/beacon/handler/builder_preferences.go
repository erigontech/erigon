// Copyright 2026 The Erigon Authors
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
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
)

const maxBuilderPreferencesRequestSize = 64 << 20

func (a *ApiHandler) PostEthV1ValidatorBuilderPreferences(w http.ResponseWriter, r *http.Request) {
	version, err := clparams.StringToClVersion(r.Header.Get("Eth-Consensus-Version"))
	if err != nil || version != clparams.GloasVersion {
		beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("Gloas Eth-Consensus-Version header is required")).WriteTo(w)
		return
	}
	if a.builderClient == nil {
		beaconhttp.NewEndpointError(http.StatusServiceUnavailable, errors.New("builder client is unavailable")).WriteTo(w)
		return
	}
	entries := cltypes.BuilderPreferencesEntries{}
	contentType, err := requestContentType(r)
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusUnsupportedMediaType, err).WriteTo(w)
		return
	}
	switch contentType {
	case "application/json":
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBuilderPreferencesRequestSize))
		if err := decoder.Decode(&entries); err != nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
			return
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("request body contains trailing data")).WriteTo(w)
			return
		}
	case "application/octet-stream":
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBuilderPreferencesRequestSize))
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
			return
		}
		if err := entries.DecodeSSZStrict(body, int(clparams.GloasVersion)); err != nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
			return
		}
	default:
		beaconhttp.NewEndpointError(http.StatusUnsupportedMediaType, fmt.Errorf("unsupported content type: %s", contentType)).WriteTo(w)
		return
	}

	failures := make([]poolingFailure, 0)
	for i, entry := range entries {
		request := &cltypes.BuilderPreferencesRequest{
			Preferences: &cltypes.BuilderPreferences{MaxExecutionPayment: entry.MaxExecutionPayment},
			Auth:        entry.Auth,
		}
		if err := a.builderClient.SubmitBuilderPreferences(r.Context(), entry.URL, entry.ProposerPubkey, request); err != nil {
			failures = append(failures, poolingFailure{Index: i, Message: err.Error()})
		}
	}
	if len(failures) != 0 {
		a.writePoolingFailures(w, failures)
		return
	}
	w.WriteHeader(http.StatusOK)
}
