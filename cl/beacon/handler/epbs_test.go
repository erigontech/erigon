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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/common"
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

func TestPostValidatorProposerPreferencesAcceptsBatchJSON(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.epbsPool = pool.NewEpbsPool()
	body, err := json.Marshal([]*cltypes.SignedProposerPreferences{
		{
			Message: &cltypes.ProposerPreferences{
				DependentRoot:  common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"),
				ProposalSlot:   32,
				ValidatorIndex: 1,
				FeeRecipient:   common.HexToAddress("0x2222222222222222222222222222222222222222"),
				TargetGasLimit: 30_000_000,
			},
		},
		{
			Message: &cltypes.ProposerPreferences{
				DependentRoot:  common.HexToHash("0x3333333333333333333333333333333333333333333333333333333333333333"),
				ProposalSlot:   33,
				ValidatorIndex: 2,
				FeeRecipient:   common.HexToAddress("0x4444444444444444444444444444444444444444"),
				TargetGasLimit: 30_000_001,
			},
		},
	})
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodPost, "/eth/v1/validator/proposer_preferences", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	_, ok := handler.epbsPool.ProposerPreferences.Get(pool.ProposerPreferencesKey{
		Slot:          32,
		DependentRoot: common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"),
	})
	require.True(t, ok)
	_, ok = handler.epbsPool.ProposerPreferences.Get(pool.ProposerPreferencesKey{
		Slot:          33,
		DependentRoot: common.HexToHash("0x3333333333333333333333333333333333333333333333333333333333333333"),
	})
	require.True(t, ok)
}

func TestPostBeaconPoolProposerPreferencesAcceptsBatchJSON(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.epbsPool = pool.NewEpbsPool()
	body, err := json.Marshal([]*cltypes.SignedProposerPreferences{
		{
			Message: &cltypes.ProposerPreferences{
				DependentRoot:  common.HexToHash("0x5555555555555555555555555555555555555555555555555555555555555555"),
				ProposalSlot:   34,
				ValidatorIndex: 3,
				FeeRecipient:   common.HexToAddress("0x6666666666666666666666666666666666666666"),
				TargetGasLimit: 30_000_002,
			},
		},
	})
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodPost, "/eth/v1/beacon/pool/proposer_preferences", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	_, ok := handler.epbsPool.ProposerPreferences.Get(pool.ProposerPreferencesKey{
		Slot:          34,
		DependentRoot: common.HexToHash("0x5555555555555555555555555555555555555555555555555555555555555555"),
	})
	require.True(t, ok)
}

func TestGetValidatorExecutionPayloadEnvelopesBySlot(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.beaconChainCfg.GloasForkEpoch = 0
	slot := uint64(3)
	envelope := cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)
	envelope.BuilderIndex = 7
	handler.selfBuildEnvelopes.Add(slot, envelope)

	request := httptest.NewRequest(http.MethodGet, "/eth/v1/validator/execution_payload_envelopes/3", nil)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), `"builder_index":"7"`)
}
