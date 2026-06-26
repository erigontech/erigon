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
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/bls"
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

	require.Equal(t, http.StatusInternalServerError, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "invalid execution payload")
}

func TestPostExecutionPayloadBidAcceptsSSZ(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	bid := &cltypes.SignedExecutionPayloadBid{
		Message: newTestExecutionPayloadBid(12, 3, 1000),
	}
	body, err := bid.EncodeSSZ(nil)
	require.NoError(t, err)

	request := httptest.NewRequest(http.MethodPost, "/eth/v1/beacon/execution_payload_bid", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/octet-stream")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadBid(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}

func TestGetValidatorExecutionPayloadBidReturnsUnsignedBid(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.beaconChainCfg.GloasForkEpoch = 0
	handler.epbsPool = pool.NewEpbsPool()
	bid := newTestExecutionPayloadBid(12, 3, 1000)
	handler.epbsPool.HighestBids.Add(pool.HighestBidKey{
		Slot:            bid.Slot,
		ParentBlockHash: bid.ParentBlockHash,
		ParentBlockRoot: bid.ParentBlockRoot,
	}, &cltypes.SignedExecutionPayloadBid{Message: bid})

	request := httptest.NewRequest(http.MethodGet, "/eth/v1/validator/execution_payload_bid/12/3", nil)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), `"builder_index":"3"`)
	require.NotContains(t, recorder.Body.String(), `"signature"`)
	require.NotContains(t, recorder.Body.String(), `"message"`)
}

func TestAggregatePayloadAttestationMessagesFiltersAndLimits(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.PtcSize = 5
	cfg.MaxPayloadAttestations = 2
	ptc := fixedPTCProvider{ptc: map[uint64][]uint64{12: {10, 11, 12, 13, 14}}}

	first := common.HexToHash("0x1111")
	second := common.HexToHash("0x2222")
	third := common.HexToHash("0x3333")
	messages := []*cltypes.PayloadAttestationMessage{
		newTestPayloadAttestationMessage(t, 10, first),
		newTestPayloadAttestationMessage(t, 10, first),
		newTestPayloadAttestationMessage(t, 11, first),
		newTestPayloadAttestationMessage(t, 12, first),
		newTestPayloadAttestationMessage(t, 12, second),
		newTestPayloadAttestationMessage(t, 99, second),
		newTestPayloadAttestationMessage(t, 14, third),
	}

	attestations, err := aggregatePayloadAttestationMessages(&cfg, ptc, messages)

	require.NoError(t, err)
	require.Equal(t, 2, attestations.Len())
	require.Equal(t, first, attestations.Get(0).Data.BeaconBlockRoot)
	require.Equal(t, []int{0, 1, 2}, attestations.Get(0).AggregationBits.GetOnIndices())
	require.Equal(t, second, attestations.Get(1).Data.BeaconBlockRoot)
	require.Equal(t, []int{2}, attestations.Get(1).AggregationBits.GetOnIndices())
}

type fixedPTCProvider struct {
	ptc map[uint64][]uint64
}

func (p fixedPTCProvider) GetPTC(slot uint64) ([]uint64, error) {
	return p.ptc[slot], nil
}

func newTestPayloadAttestationMessage(t *testing.T, validatorIndex uint64, beaconBlockRoot common.Hash) *cltypes.PayloadAttestationMessage {
	t.Helper()
	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)
	var signingMessage [32]byte
	signingMessage[0] = byte(validatorIndex)
	signature := privateKey.Sign(signingMessage[:])
	var signatureBytes common.Bytes96
	copy(signatureBytes[:], signature.Bytes())
	return &cltypes.PayloadAttestationMessage{
		ValidatorIndex: validatorIndex,
		Data: &cltypes.PayloadAttestationData{
			BeaconBlockRoot:   beaconBlockRoot,
			Slot:              12,
			PayloadPresent:    true,
			BlobDataAvailable: true,
		},
		Signature: signatureBytes,
	}
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

func newTestExecutionPayloadBid(slot, builderIndex, value uint64) *cltypes.ExecutionPayloadBid {
	return &cltypes.ExecutionPayloadBid{
		ParentBlockHash:    common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"),
		ParentBlockRoot:    common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222"),
		BlockHash:          common.HexToHash("0x3333333333333333333333333333333333333333333333333333333333333333"),
		PrevRandao:         common.HexToHash("0x4444444444444444444444444444444444444444444444444444444444444444"),
		FeeRecipient:       common.HexToAddress("0x5555555555555555555555555555555555555555"),
		GasLimit:           30_000_000,
		BuilderIndex:       builderIndex,
		Slot:               slot,
		Value:              value,
		ExecutionPayment:   0,
		BlobKzgCommitments: *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
	}
}
