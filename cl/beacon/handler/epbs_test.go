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
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/common/log/v3"
)

func TestPostPayloadAttestationsRejectsNullMessage(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	request := httptest.NewRequest(http.MethodPost, "/eth/v1/beacon/pool/payload_attestations", strings.NewReader(`[null]`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconPoolPayloadAttestations(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "missing payload attestation message data")
}

func TestPostExecutionPayloadEnvelopeReturnsForkchoiceError(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	fcu.OnExecutionPayloadErr = errors.New("invalid execution payload")

	request := httptest.NewRequest(http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "invalid execution payload")
}
