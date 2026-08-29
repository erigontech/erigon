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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/cl/gossip"
	blob_storage_mock "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	forkchoice_mock "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	gossip_mock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	mock_services "github.com/erigontech/erigon/cl/phase1/network/services/mock_services"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
)

type nonNilSentinelClient struct {
	sentinelproto.SentinelClient
}

func TestPostPayloadAttestationsRejectsNullMessage(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/payload_attestations", strings.NewReader(`[null]`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconPoolPayloadAttestations(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "missing payload attestation message data")
}

func TestPostPayloadAttestationsRejectsOversizedSSZ(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	msgSize := (&cltypes.PayloadAttestationMessage{Data: new(cltypes.PayloadAttestationData)}).EncodingSizeSSZ()
	maxSize := int(handler.beaconChainCfg.PtcSize) * msgSize

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/payload_attestations", strings.NewReader(strings.Repeat("\x00", maxSize+1)))
	request.Header.Set("Content-Type", "application/octet-stream")
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
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconPoolPayloadAttestations(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
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
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconPoolPayloadAttestations(recorder, request)

	require.Equal(t, http.StatusUnsupportedMediaType, recorder.Code, recorder.Body.String())
}

func TestPostPayloadAttestationsRejectsUnsupportedContentType(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/payload_attestations", strings.NewReader(`[]`))
	request.Header.Set("Content-Type", "text/plain")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconPoolPayloadAttestations(recorder, request)

	require.Equal(t, http.StatusUnsupportedMediaType, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadEnvelopeReturnsAcceptedAfterIntegrationError(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	handler.sentinel = &nonNilSentinelClient{}
	fcu.OnExecutionPayloadErr = errors.New("invalid execution payload")
	handler.gossipManager.(*gossip_mock.MockGossip).EXPECT().Publish(gomock.Any(), gossip.TopicNameExecutionPayload, gomock.Any()).Return(nil)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadEnvelopeRejectsDuplicateAfterBroadcast(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	handler.sentinel = &nonNilSentinelClient{}
	fcu.OnExecutionPayloadErr = errors.New("integration unavailable")
	handler.gossipManager.(*gossip_mock.MockGossip).EXPECT().Publish(
		gomock.Any(), gossip.TopicNameExecutionPayload, gomock.Any(),
	).Return(nil)

	post := func() *httptest.ResponseRecorder {
		request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", strings.NewReader(`{}`))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
		request.Header.Set("Eth-Blob-Data-Included", "false")
		recorder := httptest.NewRecorder()
		handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)
		return recorder
	}

	first := post()
	require.Equal(t, http.StatusAccepted, first.Code, first.Body.String())
	second := post()
	require.Equal(t, http.StatusBadRequest, second.Code, second.Body.String())
	require.Contains(t, second.Body.String(), "already seen")
}

func TestPostExecutionPayloadEnvelopeRejectsEnvelopeAlreadyStoredByP2P(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	handler.sentinel = &nonNilSentinelClient{}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	envelope.Message.BeaconBlockRoot = common.HexToHash("0x1234")
	fcu.SetEnvelope(envelope.Message.BeaconBlockRoot, envelope)
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "already seen")
	require.False(t, fcu.OnExecutionPayloadCalled)
}

func TestPostExecutionPayloadEnvelopeCoalescesConcurrentStoredDuplicates(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	handler.sentinel = &nonNilSentinelClient{}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	envelope.Message.BeaconBlockRoot = common.HexToHash("0x1234")
	fcu.SetEnvelope(envelope.Message.BeaconBlockRoot, envelope)
	readEntered := make(chan struct{})
	releaseRead := make(chan struct{})
	var reads atomic.Int32
	fcu.ReadEnvelopeFromDiskFunc = func(common.Hash) (*cltypes.SignedExecutionPayloadEnvelope, error) {
		if reads.Add(1) == 1 {
			close(readEntered)
		}
		<-releaseRead
		return envelope, nil
	}
	body, err := json.Marshal(envelope)
	require.NoError(t, err)

	const requests = 16
	start := make(chan struct{})
	responses := make(chan int, requests)
	for range requests {
		go func() {
			<-start
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", bytes.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
			request.Header.Set("Eth-Blob-Data-Included", "false")
			recorder := httptest.NewRecorder()
			handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)
			responses <- recorder.Code
		}()
	}
	close(start)
	<-readEntered
	require.Never(t, func() bool { return reads.Load() > 1 }, 100*time.Millisecond, 10*time.Millisecond)
	close(releaseRead)
	badRequest, unavailable := 0, 0
	for range requests {
		switch <-responses {
		case http.StatusBadRequest:
			badRequest++
		case http.StatusServiceUnavailable:
			unavailable++
		}
	}
	require.Equal(t, 2, badRequest)
	require.Equal(t, requests-2, unavailable)
	require.EqualValues(t, 1, reads.Load())
}

func TestPostExecutionPayloadEnvelopeRetriesAfterBroadcastFailure(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	handler.sentinel = &nonNilSentinelClient{}
	injected := errors.New("gossip unavailable")
	gomock.InOrder(
		handler.gossipManager.(*gossip_mock.MockGossip).EXPECT().Publish(
			gomock.Any(), gossip.TopicNameExecutionPayload, gomock.Any(),
		).Return(injected),
		handler.gossipManager.(*gossip_mock.MockGossip).EXPECT().Publish(
			gomock.Any(), gossip.TopicNameExecutionPayload, gomock.Any(),
		).Return(nil),
	)
	var integrations atomic.Int32
	fcu.OnExecutionPayloadFunc = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error {
		integrations.Add(1)
		return nil
	}

	post := func() *httptest.ResponseRecorder {
		request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", strings.NewReader(`{}`))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
		request.Header.Set("Eth-Blob-Data-Included", "false")
		recorder := httptest.NewRecorder()
		handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)
		return recorder
	}

	first := post()
	require.Equal(t, http.StatusInternalServerError, first.Code, first.Body.String())
	second := post()
	require.Equal(t, http.StatusOK, second.Code, second.Body.String())
	require.EqualValues(t, 1, integrations.Load())
}

func TestExecutionPayloadEnvelopeAdmissionsCoalesceConcurrentClaims(t *testing.T) {
	var admissions forkchoice.ExecutionPayloadEnvelopeAdmissions
	root := common.HexToHash("0x1234")
	token, err := admissions.Claim(t.Context(), root, 42)
	require.NoError(t, err)

	const contenders = 1
	type admissionResult struct {
		token forkchoice.ExecutionPayloadEnvelopeAdmissionToken
		err   error
	}
	results := make(chan admissionResult, contenders)
	var ready sync.WaitGroup
	ready.Add(contenders)
	start := make(chan struct{})
	for range contenders {
		go func() {
			ready.Done()
			<-start
			token, err := admissions.Claim(t.Context(), root, 42)
			results <- admissionResult{token: token, err: err}
		}()
	}
	ready.Wait()
	close(start)
	admissions.Finish(token, false)
	retry := <-results
	require.NoError(t, retry.err)
	admissions.Finish(retry.token, true)
	_, err = admissions.Claim(t.Context(), root, 42)
	require.ErrorContains(t, err, "already seen")
}

func TestPostExecutionPayloadEnvelopeClaimsBeforeConcurrentValidation(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	handler.sentinel = &nonNilSentinelClient{}
	handler.gossipManager.(*gossip_mock.MockGossip).EXPECT().Publish(
		gomock.Any(), gossip.TopicNameExecutionPayload, gomock.Any(),
	).Return(nil)

	validationEntered := make(chan struct{})
	releaseValidation := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseValidation) }) }
	defer release()
	var validations atomic.Int32
	fcu.ValidateExecutionPayloadEnvelopeForGossipFunc = func(*cltypes.SignedExecutionPayloadEnvelope) error {
		if validations.Add(1) == 1 {
			close(validationEntered)
		}
		<-releaseValidation
		return nil
	}

	const requests = 16
	start := make(chan struct{})
	responses := make(chan int, requests)
	for range requests {
		go func() {
			<-start
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", strings.NewReader(`{}`))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
			request.Header.Set("Eth-Blob-Data-Included", "false")
			recorder := httptest.NewRecorder()
			handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)
			responses <- recorder.Code
		}()
	}
	close(start)
	<-validationEntered
	require.Never(t, func() bool { return validations.Load() > 1 }, 100*time.Millisecond, 10*time.Millisecond)
	require.EqualValues(t, 1, validations.Load())
	release()
	ok, badRequest, unavailable := 0, 0, 0
	for range requests {
		switch <-responses {
		case http.StatusOK:
			ok++
		case http.StatusBadRequest:
			badRequest++
		case http.StatusServiceUnavailable:
			unavailable++
		}
	}
	require.Equal(t, 1, ok)
	require.Equal(t, 1, badRequest)
	require.Equal(t, requests-2, unavailable)
}

func TestPostExecutionPayloadEnvelopeConcurrentWaiterRetriesRejectedOwner(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	handler.sentinel = &nonNilSentinelClient{}
	handler.gossipManager.(*gossip_mock.MockGossip).EXPECT().Publish(
		gomock.Any(), gossip.TopicNameExecutionPayload, gomock.Any(),
	).Return(nil)

	firstValidationEntered := make(chan struct{})
	releaseFirstValidation := make(chan struct{})
	var validations atomic.Int32
	fcu.ValidateExecutionPayloadEnvelopeForGossipFunc = func(*cltypes.SignedExecutionPayloadEnvelope) error {
		if validations.Add(1) == 1 {
			close(firstValidationEntered)
			<-releaseFirstValidation
			return errors.New("invalid envelope")
		}
		return nil
	}
	post := func(responses chan<- int) {
		request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", strings.NewReader(`{}`))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
		request.Header.Set("Eth-Blob-Data-Included", "false")
		recorder := httptest.NewRecorder()
		handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)
		responses <- recorder.Code
	}
	responses := make(chan int, 2)
	go post(responses)
	<-firstValidationEntered
	go post(responses)
	close(releaseFirstValidation)

	statuses := []int{<-responses, <-responses}
	require.ElementsMatch(t, []int{http.StatusBadRequest, http.StatusOK}, statuses)
	require.EqualValues(t, 2, validations.Load())
}

func TestPostExecutionPayloadEnvelopeRequiresBlobDataIncludedHeader(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	for _, value := range []string{"", "sometimes", "TRUE"} {
		t.Run(value, func(t *testing.T) {
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", strings.NewReader(`{}`))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
			if value != "" {
				request.Header.Set("Eth-Blob-Data-Included", value)
			}
			recorder := httptest.NewRecorder()

			handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		})
	}
}

func TestPostExecutionPayloadEnvelopeRequiresConsensusVersion(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.False(t, fcu.OnExecutionPayloadCheckBlobData)
}

func TestPostExecutionPayloadEnvelopeRejectsUnknownBroadcastValidation(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope?broadcast_validation=fast", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.False(t, fcu.OnExecutionPayloadCheckBlobData)
}

func TestPostExecutionPayloadEnvelopeRejectsConsensusEquivocationBeforeApplyOrPublish(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	handler.sentinel = &nonNilSentinelClient{}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	envelope.Message.BeaconBlockRoot = common.HexToHash("0x1234")
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	fcu.HasEquivocatingBlockFunc = func(root common.Hash) (bool, bool) {
		require.Equal(t, envelope.Message.BeaconBlockRoot, root)
		return true, true
	}
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope?broadcast_validation=consensus_and_equivocation", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.False(t, fcu.OnExecutionPayloadCheckBlobData)
}

func TestPostExecutionPayloadEnvelopeEquivocationCheckOnlyAppliesToRequestedMode(t *testing.T) {
	for _, validation := range []string{"", "gossip", "consensus"} {
		t.Run(validation, func(t *testing.T) {
			_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
			fcu.HasEquivocatingBlockFunc = func(common.Hash) (bool, bool) {
				t.Fatal("equivocation check called for weaker validation mode")
				return false, false
			}
			envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
			body, err := json.Marshal(envelope)
			require.NoError(t, err)
			path := "/eth/v1/beacon/execution_payload_envelope"
			if validation != "" {
				path += "?broadcast_validation=" + validation
			}
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, bytes.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
			request.Header.Set("Eth-Blob-Data-Included", "false")
			recorder := httptest.NewRecorder()

			handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
		})
	}
}

func TestPostExecutionPayloadEnvelopeRejectsUnavailableEquivocationContext(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	fcu.HasEquivocatingBlockFunc = func(common.Hash) (bool, bool) { return false, false }
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope?broadcast_validation=consensus_and_equivocation", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.False(t, fcu.OnExecutionPayloadCheckBlobData)
}

func emptyExecutionPayloadEnvelopeContents(t *testing.T, handler *ApiHandler, fcu *forkchoice_mock.ForkChoiceStorageMock) *executionPayloadEnvelopeContents {
	t.Helper()
	block := cltypes.NewSignedBeaconBlock(handler.beaconChainCfg, clparams.GloasVersion)
	return executionPayloadEnvelopeContentsForBlock(t, handler, fcu, block)
}

func executionPayloadEnvelopeContentsForBlock(t *testing.T, handler *ApiHandler, fcu *forkchoice_mock.ForkChoiceStorageMock, block *cltypes.SignedBeaconBlock) *executionPayloadEnvelopeContents {
	t.Helper()
	contents := newExecutionPayloadEnvelopeContents(handler.beaconChainCfg)
	envelope := contents.SignedExecutionPayloadEnvelope.Message
	envelope.ParentBeaconBlockRoot = block.Block.ParentRoot
	envelope.Payload.SlotNumber = block.Block.Slot
	envelope.Payload.Extra = solid.NewExtraData()
	envelope.Payload.Transactions = solid.NewTransactionsSSZFromTransactions(nil)
	envelope.Payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(handler.beaconChainCfg.MaxWithdrawalsPerPayload), 44)
	envelope.Payload.BlockAccessList = solid.NewByteListSSZ(handler.beaconChainCfg.MaxBytesPerTransaction)
	requestsRoot, err := envelope.ExecutionRequests.HashSSZ()
	require.NoError(t, err)
	requestsHash := cltypes.ComputeExecutionRequestHash(cltypes.GetExecutionRequestsList(handler.beaconChainCfg, envelope.ExecutionRequests))
	envelope.Payload.BlockHash, err = envelope.Payload.ComputeBlockHash(&envelope.ParentBeaconBlockRoot, requestsHash, nil)
	require.NoError(t, err)
	bid := block.Block.Body.GetSignedExecutionPayloadBid().Message
	bid.ParentBlockHash = envelope.Payload.ParentHash
	bid.BlockHash = envelope.Payload.BlockHash
	bid.PrevRandao = envelope.Payload.PrevRandao
	bid.GasLimit = envelope.Payload.GasLimit
	bid.BuilderIndex = envelope.BuilderIndex
	bid.ExecutionRequestsRoot = requestsRoot
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	fcu.Blocks[root] = block
	envelope.BeaconBlockRoot = root
	return contents
}

func TestPostExecutionPayloadEnvelopeAcceptsBlobContentsJSONAndStrictSSZ(t *testing.T) {
	for _, contentType := range []string{"application/json", "application/octet-stream"} {
		t.Run(contentType, func(t *testing.T) {
			_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
			contents := emptyExecutionPayloadEnvelopeContents(t, handler, fcu)
			var body []byte
			var err error
			if contentType == "application/json" {
				body, err = json.Marshal(contents)
			} else {
				body, err = contents.EncodeSSZ(nil)
				require.NoError(t, err)
				require.Len(t, body, contents.EncodingSizeSSZ())
			}
			require.NoError(t, err)
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", bytes.NewReader(body))
			request.Header.Set("Content-Type", contentType)
			request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
			request.Header.Set("Eth-Blob-Data-Included", "true")
			recorder := httptest.NewRecorder()

			handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			require.True(t, fcu.OnExecutionPayloadCheckBlobData)
		})
	}
}

func TestPostExecutionPayloadEnvelopeRejectsBlobContentsCardinalityMismatch(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	contents := emptyExecutionPayloadEnvelopeContents(t, handler, fcu)
	contents.KZGProofs.Append(new(cltypes.KZGProof))
	body, err := json.Marshal(contents)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "true")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadEnvelopeRejectsBlobContentsAboveConfiguredBound(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	proofs := make([]*cltypes.KZGProof, int(handler.beaconChainCfg.MaxBlobsPerBlockUpperBound())+1)
	for i := range proofs {
		proofs[i] = new(cltypes.KZGProof)
	}
	body, err := json.Marshal(struct {
		SignedExecutionPayloadEnvelope *cltypes.SignedExecutionPayloadEnvelope `json:"signed_execution_payload_envelope"`
		KZGProofs                      []*cltypes.KZGProof                     `json:"kzg_proofs"`
		Blobs                          []*cltypes.Blob                         `json:"blobs"`
	}{
		SignedExecutionPayloadEnvelope: &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)},
		KZGProofs:                      proofs,
		Blobs:                          []*cltypes.Blob{},
	})
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "true")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "list exceeds decoder resource limit")
}

func TestPostExecutionPayloadEnvelopeRejectsNonCanonicalBlobContentsSSZ(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	contents := emptyExecutionPayloadEnvelopeContents(t, handler, fcu)
	body, err := contents.EncodeSSZ(nil)
	require.NoError(t, err)
	body = append(body, 0)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "true")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadEnvelopePersistsVerifiedBlobColumnsBeforeDataAvailability(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	if clparams.GetBeaconConfig() == nil {
		clparams.InitGlobalStaticConfig(&clparams.MainnetBeaconConfig, &clparams.CaplinConfig{})
	}
	ctrl := gomock.NewController(t)
	columnStorage := blob_storage_mock.NewMockDataColumnStorage(ctrl)
	handler.columnStorage = columnStorage
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	handler.sentinel = &nonNilSentinelClient{}
	block := cltypes.NewSignedBeaconBlock(handler.beaconChainCfg, clparams.GloasVersion)
	blob := new(cltypes.Blob)
	commitment, err := kzg.Ctx().BlobToKZGCommitment((*goethkzg.Blob)(blob), 0)
	require.NoError(t, err)
	proof, err := kzg.Ctx().ComputeBlobKZGProof((*goethkzg.Blob)(blob), commitment, 0)
	require.NoError(t, err)
	commitmentValue := cltypes.KZGCommitment(commitment)
	block.Block.Body.GetSignedExecutionPayloadBid().Message.BlobKzgCommitments.Append(&commitmentValue)
	contents := executionPayloadEnvelopeContentsForBlock(t, handler, fcu, block)
	root := contents.SignedExecutionPayloadEnvelope.Message.BeaconBlockRoot
	proofValue := cltypes.KZGProof(proof)
	contents.KZGProofs.Append(&proofValue)
	contents.Blobs.Append(blob)
	body, err := json.Marshal(contents)
	require.NoError(t, err)
	writes := 0
	columnStorage.EXPECT().WriteColumnSidecars(gomock.Any(), root, gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, common.Hash, int64, *cltypes.DataColumnSidecar) error {
			writes++
			return nil
		},
	).Times(int(handler.beaconChainCfg.NumberOfColumns))
	fcu.OnExecutionPayloadFunc = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error {
		require.Equal(t, int(handler.beaconChainCfg.NumberOfColumns), writes)
		return nil
	}
	columnPublishes := 0
	envelopePublishes := 0
	handler.gossipManager.(*gossip_mock.MockGossip).EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, topic string, _ []byte) error {
			if topic == gossip.TopicNameExecutionPayload {
				require.Equal(t, int(handler.beaconChainCfg.NumberOfColumns), columnPublishes)
				envelopePublishes++
				return nil
			}
			require.True(t, gossip.IsTopicDataColumnSidecar(topic))
			columnPublishes++
			return nil
		},
	).Times(int(handler.beaconChainCfg.NumberOfColumns) + 1)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "true")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, int(handler.beaconChainCfg.NumberOfColumns), columnPublishes)
	require.Equal(t, 1, envelopePublishes)
}

func TestPostExecutionPayloadEnvelopeUsesCachedBlobsWhenBodyOmitsBlobData(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	if clparams.GetBeaconConfig() == nil {
		clparams.InitGlobalStaticConfig(&clparams.MainnetBeaconConfig, &clparams.CaplinConfig{})
	}
	ctrl := gomock.NewController(t)
	columnStorage := blob_storage_mock.NewMockDataColumnStorage(ctrl)
	handler.columnStorage = columnStorage
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	handler.sentinel = &nonNilSentinelClient{}
	block := cltypes.NewSignedBeaconBlock(handler.beaconChainCfg, clparams.GloasVersion)
	blob := new(cltypes.Blob)
	commitment, err := kzg.Ctx().BlobToKZGCommitment((*goethkzg.Blob)(blob), 0)
	require.NoError(t, err)
	commitmentValue := cltypes.KZGCommitment(commitment)
	block.Block.Body.GetSignedExecutionPayloadBid().Message.BlobKzgCommitments.Append(&commitmentValue)
	contents := executionPayloadEnvelopeContentsForBlock(t, handler, fcu, block)
	root := contents.SignedExecutionPayloadEnvelope.Message.BeaconBlockRoot
	_, proofs, err := peerdasutils.ComputeCellsAndKZGProofs(blob[:])
	require.NoError(t, err)
	cachedProofs := make([]common.Bytes48, len(proofs))
	for i := range proofs {
		cachedProofs[i] = common.Bytes48(proofs[i])
	}
	handler.blobBundles.Add(common.Bytes48(commitment), BlobBundle{
		Blob:       blob,
		Commitment: common.Bytes48(commitment),
		KzgProofs:  cachedProofs,
	})
	body, err := json.Marshal(contents.SignedExecutionPayloadEnvelope)
	require.NoError(t, err)
	columnStorage.EXPECT().WriteColumnSidecars(gomock.Any(), root, gomock.Any(), gomock.Any()).Return(nil).Times(int(handler.beaconChainCfg.NumberOfColumns))
	handler.gossipManager.(*gossip_mock.MockGossip).EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).Times(int(handler.beaconChainCfg.NumberOfColumns) + 1)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadEnvelopeRejectsMissingCachedBlobData(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	handler.sentinel = &nonNilSentinelClient{}
	block := cltypes.NewSignedBeaconBlock(handler.beaconChainCfg, clparams.GloasVersion)
	commitment := cltypes.KZGCommitment{1}
	block.Block.Body.GetSignedExecutionPayloadBid().Message.BlobKzgCommitments.Append(&commitment)
	contents := executionPayloadEnvelopeContentsForBlock(t, handler, fcu, block)
	body, err := json.Marshal(contents.SignedExecutionPayloadEnvelope)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.False(t, fcu.OnExecutionPayloadCalled)
}

func TestPostExecutionPayloadEnvelopeRejectsInvalidEnvelopeBeforeBlobColumns(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*cltypes.SignedExecutionPayloadEnvelope)
	}{
		{name: "nil payload", mutate: func(envelope *cltypes.SignedExecutionPayloadEnvelope) { envelope.Message.Payload = nil }},
		{name: "commitment mismatch", mutate: func(envelope *cltypes.SignedExecutionPayloadEnvelope) { envelope.Message.Payload.BlockHash[0]++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
			if clparams.GetBeaconConfig() == nil {
				clparams.InitGlobalStaticConfig(&clparams.MainnetBeaconConfig, &clparams.CaplinConfig{})
			}
			ctrl := gomock.NewController(t)
			handler.columnStorage = blob_storage_mock.NewMockDataColumnStorage(ctrl)
			handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
			handler.sentinel = &nonNilSentinelClient{}
			block := cltypes.NewSignedBeaconBlock(handler.beaconChainCfg, clparams.GloasVersion)
			blob := new(cltypes.Blob)
			commitment, err := kzg.Ctx().BlobToKZGCommitment((*goethkzg.Blob)(blob), 0)
			require.NoError(t, err)
			proof, err := kzg.Ctx().ComputeBlobKZGProof((*goethkzg.Blob)(blob), commitment, 0)
			require.NoError(t, err)
			commitmentValue := cltypes.KZGCommitment(commitment)
			block.Block.Body.GetSignedExecutionPayloadBid().Message.BlobKzgCommitments.Append(&commitmentValue)
			contents := executionPayloadEnvelopeContentsForBlock(t, handler, fcu, block)
			tc.mutate(contents.SignedExecutionPayloadEnvelope)
			proofValue := cltypes.KZGProof(proof)
			contents.KZGProofs.Append(&proofValue)
			contents.Blobs.Append(blob)
			body, err := json.Marshal(contents)
			require.NoError(t, err)
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", bytes.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
			request.Header.Set("Eth-Blob-Data-Included", "true")
			recorder := httptest.NewRecorder()

			handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			require.False(t, fcu.OnExecutionPayloadCheckBlobData)
		})
	}
}

func TestPostExecutionPayloadEnvelopeRejectsUnpersistableEnvelopeBeforeBlobColumns(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	if clparams.GetBeaconConfig() == nil {
		clparams.InitGlobalStaticConfig(&clparams.MainnetBeaconConfig, &clparams.CaplinConfig{})
	}
	ctrl := gomock.NewController(t)
	handler.columnStorage = blob_storage_mock.NewMockDataColumnStorage(ctrl)
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	handler.sentinel = &nonNilSentinelClient{}
	block := cltypes.NewSignedBeaconBlock(handler.beaconChainCfg, clparams.GloasVersion)
	blob := new(cltypes.Blob)
	commitment, err := kzg.Ctx().BlobToKZGCommitment((*goethkzg.Blob)(blob), 0)
	require.NoError(t, err)
	proof, err := kzg.Ctx().ComputeBlobKZGProof((*goethkzg.Blob)(blob), commitment, 0)
	require.NoError(t, err)
	commitmentValue := cltypes.KZGCommitment(commitment)
	block.Block.Body.GetSignedExecutionPayloadBid().Message.BlobKzgCommitments.Append(&commitmentValue)
	contents := executionPayloadEnvelopeContentsForBlock(t, handler, fcu, block)
	envelope := contents.SignedExecutionPayloadEnvelope.Message
	envelope.Payload.BlockAccessList = solid.NewByteListSSZ(handler.beaconChainCfg.MaxBytesPerTransaction)
	require.NoError(t, envelope.Payload.BlockAccessList.DecodeSSZ(make([]byte, int(clparams.MaxChunkSize)+1024), int(clparams.GloasVersion)))
	requestsHash := cltypes.ComputeExecutionRequestHash(cltypes.GetExecutionRequestsList(handler.beaconChainCfg, envelope.ExecutionRequests))
	envelope.Payload.BlockHash, err = envelope.Payload.ComputeBlockHash(&envelope.ParentBeaconBlockRoot, requestsHash, nil)
	require.NoError(t, err)
	block.Block.Body.GetSignedExecutionPayloadBid().Message.BlockHash = envelope.Payload.BlockHash
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	envelope.BeaconBlockRoot = root
	fcu.Blocks[root] = block
	proofValue := cltypes.KZGProof(proof)
	contents.KZGProofs.Append(&proofValue)
	contents.Blobs.Append(blob)
	require.Greater(t, contents.SignedExecutionPayloadEnvelope.EncodingSizeSSZ(), int(clparams.MaxChunkSize))
	body, err := json.Marshal(contents)
	require.NoError(t, err)
	require.LessOrEqual(t, int64(len(body)), maxExecutionPayloadEnvelopeRequestSize)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "true")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "encoding size")
	require.False(t, fcu.OnExecutionPayloadCheckBlobData)
}

func TestDataColumnSidecarsGloasUseProgressiveColumnLists(t *testing.T) {
	if clparams.GetBeaconConfig() == nil {
		clparams.InitGlobalStaticConfig(&clparams.MainnetBeaconConfig, &clparams.CaplinConfig{})
	}
	cfg := clparams.MainnetBeaconConfig
	cfg.NumberOfColumns = 1
	cfg.MaxBlobCommittmentsPerBlock = 4
	cell := cltypes.Cell{1}
	proof := cltypes.KZGProof{2}

	sidecars, err := dataColumnSidecarsGloas(&cfg, 3, common.HexToHash("0x1234"), []peerdasutils.CellsAndKZGProofs{{Blobs: []cltypes.Cell{cell}, Proofs: []cltypes.KZGProof{proof}}})
	require.NoError(t, err)
	expected := solid.NewStaticProgressiveListSSZ[*cltypes.Cell](int(cfg.MaxBlobCommittmentsPerBlock), cltypes.BytesPerCell)
	expected.Append(&cell)
	wantRoot, err := expected.HashSSZ()
	require.NoError(t, err)
	gotRoot, err := sidecars[0].Column.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, wantRoot, gotRoot)
}

func TestPostExecutionPayloadEnvelopeChecksDataAvailability(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.True(t, fcu.OnExecutionPayloadCheckBlobData)
}

func TestPostExecutionPayloadEnvelopeGossipsAndReturnsAcceptedWhenFullIntegrationFails(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	handler.sentinel = &nonNilSentinelClient{}
	fcu.OnExecutionPayloadErr = forkchoice.ErrEIP7594ColumnDataNotAvailable
	handler.gossipManager.(*gossip_mock.MockGossip).EXPECT().Publish(
		gomock.Any(),
		gossip.TopicNameExecutionPayload,
		gomock.Any(),
	).Return(nil)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
	require.True(t, fcu.ValidateExecutionPayloadEnvelopeForGossipCalled)
}

func TestPostExecutionPayloadEnvelopePublishesBeforeFullIntegration(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	handler.sentinel = &nonNilSentinelClient{}
	var published atomic.Bool
	handler.gossipManager.(*gossip_mock.MockGossip).EXPECT().Publish(
		gomock.Any(),
		gossip.TopicNameExecutionPayload,
		gomock.Any(),
	).DoAndReturn(func(context.Context, string, []byte) error {
		published.Store(true)
		return nil
	})
	fcu.OnExecutionPayloadFunc = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error {
		require.True(t, published.Load())
		return nil
	}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadEnvelopeRequestedValidationFailureIsNotBroadcast(t *testing.T) {
	for _, validation := range []string{"gossip", "consensus", "consensus_and_equivocation"} {
		t.Run(validation, func(t *testing.T) {
			_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
			ctrl := gomock.NewController(t)
			handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
			handler.sentinel = &nonNilSentinelClient{}
			fcu.ValidateExecutionPayloadEnvelopeForGossipErr = errors.New("gossip validation failed")
			envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
			body, err := json.Marshal(envelope)
			require.NoError(t, err)
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope?broadcast_validation="+validation, bytes.NewReader(body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
			request.Header.Set("Eth-Blob-Data-Included", "false")
			recorder := httptest.NewRecorder()

			handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			require.True(t, fcu.ValidateExecutionPayloadEnvelopeForGossipCalled)
			require.False(t, fcu.OnExecutionPayloadCalled)
		})
	}
}

func TestPostExecutionPayloadEnvelopeConsensusFailureIsNotBroadcast(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	handler.sentinel = &nonNilSentinelClient{}
	fcu.ValidateExecutionPayloadEnvelopeForConsensusErr = forkchoice.ErrIgnore
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope?broadcast_validation=consensus", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.True(t, fcu.ValidateExecutionPayloadEnvelopeForGossipCalled)
	require.True(t, fcu.ValidateExecutionPayloadEnvelopeForConsensusCalled)
	require.False(t, fcu.OnExecutionPayloadCalled)
}

func TestPostExecutionPayloadEnvelopeGossipsIgnoredLocalEnvelope(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	handler.sentinel = &nonNilSentinelClient{}
	fcu.OnExecutionPayloadErr = forkchoice.ErrIgnore
	handler.gossipManager.(*gossip_mock.MockGossip).EXPECT().Publish(
		gomock.Any(),
		gossip.TopicNameExecutionPayload,
		gomock.Any(),
	).Return(nil)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadEnvelopeGossipsPersistenceFailureBeforeReturningAccepted(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	handler.sentinel = &nonNilSentinelClient{}
	fcu.OnExecutionPayloadErr = fmt.Errorf("disk unavailable: %w", forkchoice.ErrExecutionPayloadEnvelopePersistenceFailed)

	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg),
	}
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	handler.gossipManager.(*gossip_mock.MockGossip).EXPECT().Publish(
		gomock.Any(),
		gossip.TopicNameExecutionPayload,
		gomock.Any(),
	).DoAndReturn(func(context.Context, string, []byte) error {
		require.Empty(t, recorder.Body.String(), "HTTP error was written before gossip")
		return nil
	})
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "false")

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadEnvelopeReturnsGossipPublishFailure(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	handler.sentinel = &nonNilSentinelClient{}
	injected := errors.New("gossip unavailable")
	handler.gossipManager.(*gossip_mock.MockGossip).EXPECT().Publish(
		gomock.Any(),
		gossip.TopicNameExecutionPayload,
		gomock.Any(),
	).Return(injected)

	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg),
	}
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusInternalServerError, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), injected.Error())
}

func TestPostExecutionPayloadEnvelopeGossipsWhenIndicesAreQueued(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	handler.sentinel = &nonNilSentinelClient{}
	fcu.OnExecutionPayloadErr = fmt.Errorf("index write unavailable: %w", forkchoice.ErrExecutionPayloadEnvelopeIndicesPending)

	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg),
	}
	envelope.Message.BeaconBlockRoot = common.HexToHash("0x1234")
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	handler.gossipManager.(*gossip_mock.MockGossip).EXPECT().Publish(
		gomock.Any(),
		gossip.TopicNameExecutionPayload,
		gomock.Any(),
	).Return(nil)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadEnvelopeRejectsPreGloasVersion(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(&clparams.MainnetBeaconConfig),
	}
	body, err := envelope.EncodeSSZ(nil)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Eth-Consensus-Version", clparams.FuluVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "consensus version")
}

func TestPostExecutionPayloadEnvelopeDoesNotApplyLegacyJSONTransactionLimit(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.beaconChainCfg.MaxTransactionsPerPayload = 2
	fcu.OnExecutionPayloadErr = errors.New("reached forkchoice")
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg),
	}
	envelope.Message.BeaconBlockRoot = common.HexToHash("0x1234")
	envelope.Message.Payload.Transactions = solid.NewTransactionsSSZFromTransactions([][]byte{{1}, {2}, {3}})
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
	require.True(t, fcu.OnExecutionPayloadCalled)
}

func TestPostExecutionPayloadEnvelopeRejectsSecondJSONValueBeforeForkchoice(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg),
	}
	envelope.Message.BeaconBlockRoot = common.HexToHash("0x5678")
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", strings.NewReader(string(body)+`{}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.False(t, fcu.HasEnvelope(envelope.Message.BeaconBlockRoot))
}

func TestPostExecutionPayloadEnvelopeAcceptsTrailingJSONWhitespace(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg),
	}
	envelope.Message.BeaconBlockRoot = common.HexToHash("0x9abc")
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", strings.NewReader(string(body)+" \n\t"))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", clparams.GloasVersion.String())
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.True(t, fcu.HasEnvelope(envelope.Message.BeaconBlockRoot))
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
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadBid(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadBidAcceptsQueuedBid(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bidService := mock_services.NewMockExecutionPayloadBidService(ctrl)
	bidService.EXPECT().ProcessMessage(gomock.Any(), gomock.Nil(), gomock.Any()).Return(fmt.Errorf("%w: %w", services.ErrIgnore, services.ErrBidQueued))
	handler.executionPayloadBidService = bidService

	bid := &cltypes.SignedExecutionPayloadBid{
		Message: newTestExecutionPayloadBid(12, 3, 1000),
	}
	body, err := bid.EncodeSSZ(nil)
	require.NoError(t, err)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_bid", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/octet-stream")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadBid(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
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
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadBid(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadBidRejectsOversizedSSZ(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_bid", strings.NewReader(strings.Repeat("\x00", int(maxSignedExecutionPayloadBidSSZSize())+1)))
	request.Header.Set("Content-Type", "application/octet-stream")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadBid(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadBidRejectsNonCanonicalSSZBeforeProcessingOrPublishing(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	handler.executionPayloadBidService = mock_services.NewMockExecutionPayloadBidService(ctrl)
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	handler.sentinel = &nonNilSentinelClient{}

	bid := &cltypes.SignedExecutionPayloadBid{
		Message: newTestExecutionPayloadBid(12, 3, 1000),
	}
	body, err := bid.EncodeSSZ(nil)
	require.NoError(t, err)

	const signedBidFixedSize = 4 + 96
	binary.LittleEndian.PutUint32(body, signedBidFixedSize+1)
	body = append(body, 0)
	copy(body[signedBidFixedSize+1:], body[signedBidFixedSize:])
	body[signedBidFixedSize] = 0

	var lax cltypes.SignedExecutionPayloadBid
	require.NoError(t, lax.DecodeSSZ(body, int(clparams.GloasVersion)))

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_bid", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/octet-stream")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadBid(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadBidRejectsMissingMessage(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_bid", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadBid(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "missing message")
}

func TestPostExecutionPayloadBidRejectsNullCommitmentBeforeProcessingOrPublishing(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	handler.executionPayloadBidService = mock_services.NewMockExecutionPayloadBidService(ctrl)
	handler.gossipManager = gossip_mock.NewMockGossip(ctrl)
	handler.sentinel = &nonNilSentinelClient{}

	body, err := json.Marshal(&cltypes.SignedExecutionPayloadBid{Message: newTestExecutionPayloadBid(12, 3, 1000)})
	require.NoError(t, err)
	const emptyCommitments = `"blob_kzg_commitments":[]`
	require.Contains(t, string(body), emptyCommitments)
	body = []byte(strings.Replace(string(body), emptyCommitments, `"blob_kzg_commitments":[null]`, 1))

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_bid", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadBid(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadBidRejectsMalformedContentType(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_bid", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/octet-stream; bad")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadBid(recorder, request)

	require.Equal(t, http.StatusUnsupportedMediaType, recorder.Code, recorder.Body.String())
}

func TestGetValidatorExecutionPayloadBidReturnsUnsignedBid(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.beaconChainCfg.GloasForkEpoch = 0
	handler.epbsPool = pool.NewEpbsPool()
	bid := newTestExecutionPayloadBid(12, 3, 1000)
	handler.epbsPool.StoreHighestBid(pool.HighestBidKey{
		Slot:            bid.Slot,
		ParentBlockHash: bid.ParentBlockHash,
		ParentBlockRoot: bid.ParentBlockRoot,
	}, &cltypes.SignedExecutionPayloadBid{Message: bid})

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/validator/execution_payload_bid/12/3", http.NoBody)
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
	handler.selfBuildEnvelopes.Add(slot, envelope)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/validator/execution_payload_envelopes/3", http.NoBody)
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
