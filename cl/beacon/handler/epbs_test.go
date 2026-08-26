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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	gossip_mock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	mock_services "github.com/erigontech/erigon/cl/phase1/network/services/mock_services"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

func TestPostPayloadAttestationsRejectsNullMessage(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/payload_attestations", strings.NewReader(`[null]`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconPoolPayloadAttestations(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "missing payload attestation message data")
}

func TestPostPayloadAttestationsRequiresGloasVersion(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	for _, version := range []string{"", "fulu"} {
		request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/payload_attestations", strings.NewReader(`[]`))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Eth-Consensus-Version", version)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}
}

func TestPostPayloadAttestationsRejectsOversizedSSZ(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	msgSize := (&cltypes.PayloadAttestationMessage{Data: new(cltypes.PayloadAttestationData)}).EncodingSizeSSZ()
	maxSize := int(handler.beaconChainCfg.PtcSize) * msgSize

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/payload_attestations", strings.NewReader(strings.Repeat("\x00", maxSize+1)))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconPoolPayloadAttestations(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}

func TestPostPayloadAttestationsAcceptsMoreThanBlockAggregateLimitSSZ(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	msg := &cltypes.PayloadAttestationMessage{
		Data: new(cltypes.PayloadAttestationData),
	}
	encoded, err := msg.EncodeSSZ(nil)
	require.NoError(t, err)
	body := strings.Repeat(string(encoded), int(handler.beaconChainCfg.MaxPayloadAttestations)+1)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/payload_attestations", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconPoolPayloadAttestations(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}

func TestPostPayloadAttestationsAcceptsSSZContentTypeParameters(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	msg := &cltypes.PayloadAttestationMessage{
		Data: new(cltypes.PayloadAttestationData),
	}
	body, err := msg.EncodeSSZ(nil)
	require.NoError(t, err)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/payload_attestations", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/octet-stream; charset=utf-8")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconPoolPayloadAttestations(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}

func TestPostPayloadAttestationsRejectsNonCanonicalSSZAndTrailingJSON(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	message := &cltypes.PayloadAttestationMessage{Data: new(cltypes.PayloadAttestationData)}
	validSSZ, err := message.EncodeSSZ(nil)
	require.NoError(t, err)
	sszBody := append([]byte(nil), validSSZ...)
	sszBody[8+32+8] = 2
	jsonBody, err := json.Marshal([]*cltypes.PayloadAttestationMessage{message})
	require.NoError(t, err)

	for _, tc := range []struct {
		name        string
		contentType string
		body        []byte
	}{
		{name: "invalid boolean", contentType: "application/octet-stream", body: sszBody},
		{name: "trailing SSZ", contentType: "application/octet-stream", body: append(append([]byte(nil), validSSZ...), 0)},
		{name: "trailing JSON", contentType: "application/json", body: append(jsonBody, []byte(`{}`)...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/payload_attestations", bytes.NewReader(tc.body))
			request.Header.Set("Content-Type", tc.contentType)
			request.Header.Set("Eth-Consensus-Version", "gloas")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		})
	}
}

func TestPostPayloadAttestationsCapsJSONCardinality(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	maxItems := int(handler.beaconChainCfg.PtcSize)
	for _, tc := range []struct {
		name        string
		count       int
		wantIndexed bool
	}{
		{name: "maximum", count: maxItems, wantIndexed: true},
		{name: "maximum plus one", count: maxItems + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := "[" + strings.TrimSuffix(strings.Repeat("null,", tc.count), ",") + "]"
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/payload_attestations", strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Eth-Consensus-Version", "gloas")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			if tc.wantIndexed {
				require.Contains(t, recorder.Body.String(), `"failures"`)
			} else {
				require.NotContains(t, recorder.Body.String(), `"failures"`)
				require.Contains(t, recorder.Body.String(), "exceeds")
			}
		})
	}
}

func TestPostPayloadAttestationsAcceptsQueuedWithoutPooling(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	msg := newTestPayloadAttestationMessage(t, 12, common.HexToHash("0x1234"))
	attestationService := mock_services.NewMockPayloadAttestationService(ctrl)
	attestationService.EXPECT().ProcessMessage(gomock.Any(), gomock.Nil(), gomock.Any()).Return(fmt.Errorf("%w: %w", services.ErrIgnore, services.ErrAttestationQueued))
	handler.payloadAttestationService = attestationService
	handler.epbsPool = pool.NewEpbsPool()

	body, err := json.Marshal([]*cltypes.PayloadAttestationMessage{msg})
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/payload_attestations", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconPoolPayloadAttestations(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	_, found := handler.epbsPool.PayloadAttestations.Get(pool.PayloadAttestationKey{Slot: msg.Data.Slot, ValidatorIndex: msg.ValidatorIndex})
	require.False(t, found)
}

func TestPostPayloadAttestationsRejectsMalformedContentType(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/payload_attestations", strings.NewReader(`[]`))
	request.Header.Set("Content-Type", "application/octet-stream; bad")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconPoolPayloadAttestations(recorder, request)

	require.Equal(t, http.StatusUnsupportedMediaType, recorder.Code, recorder.Body.String())
}

func TestPostPayloadAttestationsRejectsUnsupportedContentType(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/payload_attestations", strings.NewReader(`[]`))
	request.Header.Set("Content-Type", "text/plain")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconPoolPayloadAttestations(recorder, request)

	require.Equal(t, http.StatusUnsupportedMediaType, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadEnvelopeAcceptsGossipIntegrationError(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	fcu.OnExecutionPayloadErr = errors.New("invalid execution payload")

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadEnvelopesRequiresBlobDataHeader(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelopes", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "Eth-Blob-Data-Included")
}

func TestPostExecutionPayloadEnvelopesRejectsMalformedContents(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelopes", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Blob-Data-Included", "true")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadEnvelopesRejectsTrailingJSON(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelopes", strings.NewReader(string(body)+`{}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Blob-Data-Included", "false")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadEnvelopesEmitsImportedAndAvailableEvents(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.emitters = beaconevents.NewEventEmitter()
	events := make(chan *beaconevents.EventStream, 3)
	subscription := handler.emitters.Operation().Subscribe(events)
	defer subscription.Unsubscribe()

	root := common.Hash{1}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	envelope.Message.BeaconBlockRoot = root
	envelope.Message.BuilderIndex = 3
	fcu.Blocks = map[common.Hash]*cltypes.SignedBeaconBlock{
		root: {Block: &cltypes.BeaconBlock{Slot: 12, Body: cltypes.NewBeaconBody(handler.beaconChainCfg, clparams.GloasVersion)}},
	}
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelopes", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Blob-Data-Included", "false")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, beaconevents.OpExecutionPayloadGossip, (<-events).Event)
	require.Equal(t, beaconevents.OpExecutionPayload, (<-events).Event)
	select {
	case event := <-events:
		require.Equal(t, beaconevents.OpExecutionPayloadAvailable, event.Event)
	case <-time.After(time.Second):
		t.Fatal("execution_payload_available event was not emitted")
	}
}

func TestPostExecutionPayloadEnvelopesRejectsStatefulEnvelopeWhenColumnsAreMissing(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.emitters = beaconevents.NewEventEmitter()
	events := make(chan *beaconevents.EventStream, 2)
	subscription := handler.emitters.Operation().Subscribe(events)
	defer subscription.Unsubscribe()

	root := common.Hash{2}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	envelope.Message.BeaconBlockRoot = root
	envelope.Message.BuilderIndex = 4
	fcu.Blocks = map[common.Hash]*cltypes.SignedBeaconBlock{
		root: {Block: &cltypes.BeaconBlock{Slot: 13, Body: cltypes.NewBeaconBody(handler.beaconChainCfg, clparams.GloasVersion)}},
	}
	fcu.OnExecutionPayloadErr = forkchoice.ErrEIP7594ColumnDataNotAvailable
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelopes", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Blob-Data-Included", "false")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	select {
	case event := <-events:
		t.Fatalf("unexpected event %s", event.Event)
	default:
	}
}

func TestPostExecutionPayloadEnvelopesRejectsInvalidBroadcastValidation(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelopes?broadcast_validation=fast", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Blob-Data-Included", "false")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadEnvelopesRejectsEquivocatingBlockBeforeGossip(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	root := common.Hash{1}
	fcu.Blocks = map[common.Hash]*cltypes.SignedBeaconBlock{
		root: {Block: &cltypes.BeaconBlock{Slot: 12, ProposerIndex: 3, Body: cltypes.NewBeaconBody(handler.beaconChainCfg, clparams.GloasVersion)}},
		{2}:  {Block: &cltypes.BeaconBlock{Slot: 12, ProposerIndex: 3, Body: cltypes.NewBeaconBody(handler.beaconChainCfg, clparams.GloasVersion)}},
	}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	envelope.Message.BeaconBlockRoot = root
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelopes?broadcast_validation=consensus_and_equivocation", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Blob-Data-Included", "false")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "equivocation")
}

func TestPostExecutionPayloadEnvelopesReturnsErrorWhenGossipPublishFails(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	gossipManager.EXPECT().Publish(gomock.Any(), gossip.TopicNameExecutionPayload, gomock.Any()).Return(errors.New("gossip unavailable"))
	handler.gossipManager = gossipManager
	root := common.Hash{1}
	fcu.Blocks = map[common.Hash]*cltypes.SignedBeaconBlock{
		root: {Block: &cltypes.BeaconBlock{Slot: 12, Body: cltypes.NewBeaconBody(handler.beaconChainCfg, clparams.GloasVersion)}},
	}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	envelope.Message.BeaconBlockRoot = root
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelopes", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Blob-Data-Included", "false")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusInternalServerError, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "gossip unavailable")
}

func TestPostExecutionPayloadEnvelopesClassifiesIntegrationFailureByValidationMode(t *testing.T) {
	for _, test := range []struct {
		validation string
		status     int
	}{
		{validation: "gossip", status: http.StatusAccepted},
		{validation: "consensus", status: http.StatusBadRequest},
	} {
		t.Run(test.validation, func(t *testing.T) {
			_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
			root := common.Hash{1}
			fcu.Blocks = map[common.Hash]*cltypes.SignedBeaconBlock{
				root: {Block: &cltypes.BeaconBlock{Slot: 12, Body: cltypes.NewBeaconBody(handler.beaconChainCfg, clparams.GloasVersion)}},
			}
			fcu.OnExecutionPayloadErr = errors.New("integration failed")
			envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
			envelope.Message.BeaconBlockRoot = root
			body, err := json.Marshal(envelope)
			require.NoError(t, err)
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelopes?broadcast_validation="+test.validation, strings.NewReader(string(body)))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Eth-Blob-Data-Included", "false")
			request.Header.Set("Eth-Consensus-Version", "gloas")
			recorder := httptest.NewRecorder()

			handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)
			require.Equal(t, test.status, recorder.Code, recorder.Body.String())
		})
	}
}

func TestPostPtcDutiesDoesNotCapValidatorCount(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.beaconChainCfg.GloasForkEpoch = 0
	indices := make([]string, 2049)
	for i := range indices {
		indices[i] = `"1"`
	}
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/duties/ptc/5", strings.NewReader("["+strings.Join(indices, ",")+"]"))
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("epoch", "5")
	request = request.WithContext(context.WithValue(request.Context(), chi.RouteCtxKey, rctx))

	_, err := handler.PostEthV1ValidatorDutiesPtc(httptest.NewRecorder(), request)
	require.NoError(t, err)
}

func TestPostExecutionPayloadBidAcceptsSSZ(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	bid := &cltypes.SignedExecutionPayloadBid{
		Message: newTestExecutionPayloadBid(12, 3, 1000),
	}
	body, err := bid.EncodeSSZ(nil)
	require.NoError(t, err)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_bid", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadBid(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadBidCanonicalPluralRequiresVersionButSingularAliasDoesNot(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	bid := &cltypes.SignedExecutionPayloadBid{Message: newTestExecutionPayloadBid(12, 3, 1000)}
	body, err := json.Marshal(bid)
	require.NoError(t, err)

	canonical := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_bids", bytes.NewReader(body))
	canonical.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, canonical)
	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())

	legacy := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_bid", bytes.NewReader(body))
	legacy.Header.Set("Content-Type", "application/json")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, legacy)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadEnvelopesCanonicalPluralRequiresHeadersAndValidation(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	body, err := json.Marshal(envelope)
	require.NoError(t, err)

	for _, tc := range []struct {
		name       string
		version    string
		blobHeader string
		query      string
		message    string
	}{
		{name: "missing version", message: "Eth-Consensus-Version"},
		{name: "missing blob header", version: "gloas", message: "Eth-Blob-Data-Included"},
		{name: "invalid broadcast validation", version: "gloas", blobHeader: "false", query: "?broadcast_validation=fast", message: "broadcast_validation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelopes"+tc.query, bytes.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			if tc.version != "" {
				request.Header.Set("Eth-Consensus-Version", tc.version)
			}
			if tc.blobHeader != "" {
				request.Header.Set("Eth-Blob-Data-Included", tc.blobHeader)
			}
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			require.Contains(t, recorder.Body.String(), tc.message)
		})
	}
}

func TestPostExecutionPayloadBidsRejectsTrailingJSON(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	bid := &cltypes.SignedExecutionPayloadBid{Message: newTestExecutionPayloadBid(12, 3, 1000)}
	body, err := json.Marshal(bid)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_bids", strings.NewReader(string(body)+`{}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadBid(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "trailing data")
}

func TestPostExecutionPayloadBidRejectsIgnoredBid(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bidService := mock_services.NewMockExecutionPayloadBidService(ctrl)
	bidService.EXPECT().ProcessMessage(gomock.Any(), gomock.Nil(), gomock.Any()).Return(fmt.Errorf("%w: proposer preferences unavailable", services.ErrIgnore))
	handler.executionPayloadBidService = bidService

	bid := &cltypes.SignedExecutionPayloadBid{
		Message: newTestExecutionPayloadBid(12, 3, 1000),
	}
	body, err := bid.EncodeSSZ(nil)
	require.NoError(t, err)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_bid", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadBid(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadBidRejectsHardIgnore(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bidService := mock_services.NewMockExecutionPayloadBidService(ctrl)
	bidService.EXPECT().ProcessMessage(gomock.Any(), gomock.Nil(), gomock.Any()).Return(services.ErrIgnore)
	handler.executionPayloadBidService = bidService

	bid := &cltypes.SignedExecutionPayloadBid{
		Message: newTestExecutionPayloadBid(12, 3, 1000),
	}
	body, err := bid.EncodeSSZ(nil)
	require.NoError(t, err)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_bid", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadBid(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadBidRejectsOversizedSSZ(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_bid", strings.NewReader(strings.Repeat("\x00", int(maxSignedExecutionPayloadBidSSZSize())+1)))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadBid(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadBidRejectsMissingMessage(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_bid", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadBid(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "missing message")
}

func TestPostExecutionPayloadBidRejectsMalformedContentType(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_bid", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/octet-stream; bad")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadBid(recorder, request)

	require.Equal(t, http.StatusUnsupportedMediaType, recorder.Code, recorder.Body.String())
}

func TestGetValidatorExecutionPayloadBidReturnsUnsignedBid(t *testing.T) {
	_, _, _, _, postState, handler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	handler.beaconChainCfg.GloasForkEpoch = 0
	require.NoError(t, postState.UpgradeToFulu())
	require.NoError(t, postState.UpgradeToGloas())
	for range 4 {
		postState.GetBuilders().Append(&cltypes.Builder{})
	}
	require.NoError(t, handler.syncedData.OnHeadState(postState))
	handler.epbsPool = pool.NewEpbsPool()
	slot := handler.ethClock.GetCurrentSlot()
	bid := newTestExecutionPayloadBid(slot, 3, 1000)
	handler.epbsPool.StoreHighestBid(pool.HighestBidKey{
		Slot:            bid.Slot,
		ParentBlockHash: bid.ParentBlockHash,
		ParentBlockRoot: bid.ParentBlockRoot,
	}, &cltypes.SignedExecutionPayloadBid{Message: bid})

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf("/eth/v1/validator/execution_payload_bid/%d/3", slot), http.NoBody)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), `"builder_index":"3"`)
	require.NotContains(t, recorder.Body.String(), `"signature"`)
	require.NotContains(t, recorder.Body.String(), `"message"`)

	for _, path := range []string{
		fmt.Sprintf("/eth/v1/validator/execution_payload_bid/%d/3", slot-1),
		fmt.Sprintf("/eth/v1/validator/execution_payload_bid/%d/3", slot+2),
		fmt.Sprintf("/eth/v1/validator/execution_payload_bid/%d/4", slot),
	} {
		recorder = httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody))
		require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}
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

func TestAggregatePayloadAttestationMessagesIncludesDuplicatePTCPositions(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.PtcSize = 3
	cfg.MaxPayloadAttestations = 1
	ptc := fixedPTCProvider{ptc: map[uint64][]uint64{12: {10, 10, 11}}}
	root := common.HexToHash("0x1111")

	attestations, err := aggregatePayloadAttestationMessages(&cfg, ptc, []*cltypes.PayloadAttestationMessage{
		newTestPayloadAttestationMessage(t, 10, root),
		newTestPayloadAttestationMessage(t, 11, root),
	})

	require.NoError(t, err)
	require.Equal(t, 1, attestations.Len())
	require.Equal(t, []int{0, 1, 2}, attestations.Get(0).AggregationBits.GetOnIndices())
}

func TestAggregatePayloadAttestationMessagesSkipsMalformedSignatureGroup(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.PtcSize = 2
	cfg.MaxPayloadAttestations = 2
	ptc := fixedPTCProvider{ptc: map[uint64][]uint64{12: {10, 11}}}
	badRoot := common.HexToHash("0x1111")
	goodRoot := common.HexToHash("0x2222")
	bad := newTestPayloadAttestationMessage(t, 10, badRoot)
	bad.Signature = common.Bytes96{0x01}

	attestations, err := aggregatePayloadAttestationMessages(&cfg, ptc, []*cltypes.PayloadAttestationMessage{
		bad,
		newTestPayloadAttestationMessage(t, 11, goodRoot),
	})

	require.NoError(t, err)
	require.Equal(t, 1, attestations.Len())
	require.Equal(t, goodRoot, attestations.Get(0).Data.BeaconBlockRoot)
	require.Equal(t, []int{1}, attestations.Get(0).AggregationBits.GetOnIndices())
}

func TestAggregatePayloadAttestationMessagesSkipsUnavailablePTC(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.PtcSize = 2
	cfg.MaxPayloadAttestations = 2
	ptc := fixedPTCProvider{
		ptc: map[uint64][]uint64{12: {10, 11}},
		err: map[uint64]error{13: errors.New("ptc unavailable")},
	}
	goodRoot := common.HexToHash("0x1111")
	skipped := newTestPayloadAttestationMessage(t, 10, common.HexToHash("0x2222"))
	skipped.Data.Slot = 13

	attestations, err := aggregatePayloadAttestationMessages(&cfg, ptc, []*cltypes.PayloadAttestationMessage{
		skipped,
		newTestPayloadAttestationMessage(t, 10, goodRoot),
	})

	require.NoError(t, err)
	require.Equal(t, 1, attestations.Len())
	require.Equal(t, goodRoot, attestations.Get(0).Data.BeaconBlockRoot)
}

func TestSnapshotPayloadAttestationPTCsCopiesMessageSlots(t *testing.T) {
	source := []uint64{10, 11}
	provider := fixedPTCProvider{
		ptc: map[uint64][]uint64{
			12: source,
			13: {20, 21},
		},
	}
	msg := newTestPayloadAttestationMessage(t, 10, common.Hash{})

	snapshot := snapshotPayloadAttestationPTCs(provider, []*cltypes.PayloadAttestationMessage{
		nil,
		{Data: nil},
		msg,
	})

	source[0] = 99
	ptc, err := snapshot.GetPTC(12)
	require.NoError(t, err)
	require.Equal(t, []uint64{10, 11}, ptc)
	_, err = snapshot.GetPTC(13)
	require.Error(t, err)
}

type fixedPTCProvider struct {
	ptc map[uint64][]uint64
	err map[uint64]error
}

func (p fixedPTCProvider) GetPTC(slot uint64) ([]uint64, error) {
	if err := p.err[slot]; err != nil {
		return nil, err
	}
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

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/proposer_preferences", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", "gloas")
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

func TestPostValidatorProposerPreferencesRequiresVersionAndReportsIndexedFailures(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.epbsPool = pool.NewEpbsPool()
	preferences := []*cltypes.SignedProposerPreferences{
		{Message: &cltypes.ProposerPreferences{ProposalSlot: 32, DependentRoot: common.Hash{1}}},
		{Message: &cltypes.ProposerPreferences{ProposalSlot: 33, DependentRoot: common.Hash{2}}},
	}
	body, err := json.Marshal(preferences)
	require.NoError(t, err)

	missingVersion := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/proposer_preferences", bytes.NewReader(body))
	missingVersion.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, missingVersion)
	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())

	ctrl := gomock.NewController(t)
	service := mock_services.NewMockProposerPreferencesService(ctrl)
	service.EXPECT().ProcessMessage(gomock.Any(), gomock.Nil(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *uint64, preference *cltypes.SignedProposerPreferences) error {
			if preference.Message.ProposalSlot == 32 {
				return errors.New("invalid first preference")
			}
			return nil
		},
	).Times(2)
	handler.proposerPreferencesService = service
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/proposer_preferences", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	var response poolingError
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
	require.Equal(t, []poolingFailure{{Index: 0, Message: "invalid first preference"}}, response.Failures)
	_, found := handler.epbsPool.ProposerPreferences.Get(pool.ProposerPreferencesKey{Slot: 33, DependentRoot: common.Hash{2}})
	require.True(t, found)
}

func TestPostValidatorProposerPreferencesAcceptsBatchSSZ(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.epbsPool = pool.NewEpbsPool()
	preferences := []*cltypes.SignedProposerPreferences{
		{Message: &cltypes.ProposerPreferences{ProposalSlot: 32, DependentRoot: common.Hash{1}}},
		{Message: &cltypes.ProposerPreferences{ProposalSlot: 33, DependentRoot: common.Hash{2}}},
	}
	body := make([]byte, 0)
	for _, preference := range preferences {
		var err error
		body, err = preference.EncodeSSZ(body)
		require.NoError(t, err)
	}
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/proposer_preferences", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}

func TestPostValidatorProposerPreferencesRequiresJSONArrayButPoolRetainsSingletonCompatibility(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.epbsPool = pool.NewEpbsPool()
	preference := &cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{ProposalSlot: 32, DependentRoot: common.Hash{1}}}
	body, err := json.Marshal(preference)
	require.NoError(t, err)

	validatorRequest := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/proposer_preferences", bytes.NewReader(body))
	validatorRequest.Header.Set("Content-Type", "application/json")
	validatorRequest.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, validatorRequest)
	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())

	poolRequest := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/proposer_preferences", bytes.NewReader(body))
	poolRequest.Header.Set("Content-Type", "application/json")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, poolRequest)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}

func TestPostValidatorProposerPreferencesCapsJSONCardinality(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	for _, tc := range []struct {
		name        string
		count       int
		wantIndexed bool
	}{
		{name: "maximum", count: maxProposerPreferencesRequestItems, wantIndexed: true},
		{name: "maximum plus one", count: maxProposerPreferencesRequestItems + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := "[" + strings.TrimSuffix(strings.Repeat("null,", tc.count), ",") + "]"
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/proposer_preferences", strings.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Eth-Consensus-Version", "gloas")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			if tc.wantIndexed {
				require.Contains(t, recorder.Body.String(), `"failures"`)
			} else {
				require.NotContains(t, recorder.Body.String(), `"failures"`)
				require.Contains(t, recorder.Body.String(), "exceeds")
			}
		})
	}
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

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/proposer_preferences", strings.NewReader(string(body)))
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
	handler.selfBuildEnvelopes.Add(selfBuildEnvelopeKey{Slot: slot, BeaconBlockRoot: envelope.BeaconBlockRoot}, envelope)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/validator/execution_payload_envelopes/3", http.NoBody)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), `"builder_index":"7"`)
}

func TestGetValidatorExecutionPayloadEnvelopeByBlockRoot(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.beaconChainCfg.GloasForkEpoch = 0
	slot := handler.ethClock.GetCurrentSlot()
	root := common.HexToHash("0x1234")
	envelope := cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)
	envelope.BeaconBlockRoot = root
	handler.selfBuildEnvelopes.Add(selfBuildEnvelopeKey{Slot: slot, BeaconBlockRoot: root}, envelope)
	otherRoot := common.HexToHash("0x5678")
	otherEnvelope := cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)
	otherEnvelope.BeaconBlockRoot = otherRoot
	handler.selfBuildEnvelopes.Add(selfBuildEnvelopeKey{Slot: slot, BeaconBlockRoot: otherRoot}, otherEnvelope)

	tests := []struct {
		name string
		slot uint64
		root common.Hash
		want int
	}{
		{name: "matching current slot and root", slot: slot, root: root, want: http.StatusOK},
		{name: "same slot alternate root", slot: slot, root: otherRoot, want: http.StatusOK},
		{name: "wrong root", slot: slot, root: common.HexToHash("0x9999"), want: http.StatusNotFound},
		{name: "old slot", slot: slot - 1, root: root, want: http.StatusNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequestWithContext(t.Context(), http.MethodGet,
				fmt.Sprintf("/eth/v1/validator/execution_payload_envelopes/%d/%s", tt.slot, tt.root), http.NoBody)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)
			require.Equal(t, tt.want, recorder.Code, recorder.Body.String())
		})
	}
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
