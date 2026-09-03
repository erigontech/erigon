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
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	sync_mock_services "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	blob_storage_mock "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	gossip_mock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	mock_services "github.com/erigontech/erigon/cl/phase1/network/services/mock_services"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

func TestGetPayloadAttestationDataAcceptsCanonicalSlotQuery(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.beaconChainCfg.GloasForkEpoch = 2
	fcu.HeadSlotVal = 64
	fcu.HeadVal = common.HexToHash("0x1234")

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/validator/payload_attestation_data?slot=64", http.NoBody)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, "gloas", recorder.Header().Get("Eth-Consensus-Version"))
	require.Contains(t, recorder.Body.String(), `"slot":"64"`)
}

func TestGetPayloadAttestationDataSupportsSSZ(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.beaconChainCfg.GloasForkEpoch = 2
	fcu.HeadSlotVal = 64
	fcu.HeadVal = common.HexToHash("0x1234")

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/validator/payload_attestation_data?slot=64", http.NoBody)
	request.Header.Set("Accept", "application/octet-stream")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, "application/octet-stream", recorder.Header().Get("Content-Type"))
	require.Equal(t, "gloas", recorder.Header().Get("Eth-Consensus-Version"))
	data := new(cltypes.PayloadAttestationData)
	require.NoError(t, data.DecodeSSZStrict(recorder.Body.Bytes(), int(clparams.GloasVersion)))
	require.Equal(t, uint64(64), data.Slot)
	require.Equal(t, fcu.HeadVal, data.BeaconBlockRoot)
}

func TestGetPayloadAttestationDataRejectsPreGloasSlot(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.beaconChainCfg.GloasForkEpoch = 2
	fcu.HeadSlotVal = 63

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/validator/payload_attestation_data?slot=63", http.NoBody)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}

func TestGetPayloadAttestationDataRejectsUnsupportedResponseType(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.beaconChainCfg.GloasForkEpoch = 0
	fcu.HeadSlotVal = 64

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/validator/payload_attestation_data?slot=64", http.NoBody)
	request.Header.Set("Accept", "text/plain")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusNotAcceptable, recorder.Code, recorder.Body.String())
}

func TestGetPayloadAttestationDataReturnsNoContentWithoutSlotBlock(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.beaconChainCfg.GloasForkEpoch = 0
	fcu.HeadSlotVal = 63

	for _, accept := range []string{"application/json", "application/octet-stream"} {
		t.Run(accept, func(t *testing.T) {
			request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/validator/payload_attestation_data?slot=64", http.NoBody)
			request.Header.Set("Accept", accept)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)

			require.Equal(t, http.StatusNoContent, recorder.Code, recorder.Body.String())
			require.Empty(t, recorder.Body.String())
		})
	}
}

func TestGetPayloadAttestationDataReportsSyncing(t *testing.T) {
	for _, path := range []string{
		"/eth/v1/validator/payload_attestation_data?slot=64",
		"/eth/v1/validator/payload_attestation_data?slot=invalid",
		"/eth/v1/validator/payload_attestation_data",
	} {
		t.Run(path, func(t *testing.T) {
			_, _, _, _, _, handler, _, syncedData, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), false)
			handler.beaconChainCfg.GloasForkEpoch = 0
			syncedData.(*sync_mock_services.MockSyncedData).EXPECT().Syncing().Return(true)

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody))

			require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
		})
	}
}

func TestGetPayloadAttestationDataPreservesUnavailableHeadStatus(t *testing.T) {
	_, _, _, _, _, handler, _, syncedData, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), false)
	handler.beaconChainCfg.GloasForkEpoch = 0
	handler.enableMemoizedHeadState = true
	mockSyncedData := syncedData.(*sync_mock_services.MockSyncedData)
	mockSyncedData.EXPECT().Syncing().Return(false)
	mockSyncedData.EXPECT().StateHead().Return(common.Hash{}, uint64(0), false)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/validator/payload_attestation_data?slot=64", http.NoBody)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
}

func TestGetPayloadAttestationDataRejectsInvalidCanonicalSlotQuery(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.beaconChainCfg.GloasForkEpoch = 0

	for _, path := range []string{
		"/eth/v1/validator/payload_attestation_data",
		"/eth/v1/validator/payload_attestation_data?slot=",
		"/eth/v1/validator/payload_attestation_data?slot=64&slot=64",
		"/eth/v1/validator/payload_attestation_data?slot=invalid",
		"/eth/v1/validator/payload_attestation_data?slot=18446744073709551616",
	} {
		t.Run(path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody))
			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
		})
	}
}

func TestGetPayloadAttestationDataPreservesLegacyPathAlias(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.beaconChainCfg.GloasForkEpoch = 0
	fcu.HeadSlotVal = 64

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/validator/payload_attestation_data/64", http.NoBody)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}

func TestGetPayloadAttestationDataAcceptsMaximumSlot(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.beaconChainCfg.GloasForkEpoch = 0
	fcu.HeadSlotVal = ^uint64(0)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/validator/payload_attestation_data?slot=18446744073709551615", http.NoBody)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}

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

func TestPostPayloadAttestationsAcceptsEmptySSZ(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/payload_attestations", http.NoBody)
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconPoolPayloadAttestations(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}

func TestPostPayloadAttestationsAcceptsMoreThanBlockAggregateLimitSSZ(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	attestationService := mock_services.NewMockPayloadAttestationService(ctrl)
	handler.payloadAttestationService = attestationService
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	handler.gossipManager = gossipManager
	msg := &cltypes.PayloadAttestationMessage{
		Data: new(cltypes.PayloadAttestationData),
	}
	encoded, err := msg.EncodeSSZ(nil)
	require.NoError(t, err)
	body := strings.Repeat(string(encoded), int(handler.beaconChainCfg.MaxPayloadAttestations)+1)
	count := int(handler.beaconChainCfg.MaxPayloadAttestations) + 1
	attestationService.EXPECT().ProcessRESTMessage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *cltypes.PayloadAttestationMessage, publish func() error) error {
			return publish()
		},
	).Times(count)
	gossipManager.EXPECT().Publish(gomock.Any(), gossip.TopicNamePayloadAttestation, gomock.Any()).Return(nil).Times(count)

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/payload_attestations", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconPoolPayloadAttestations(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
}

func TestPostPayloadAttestationsAcceptsSSZContentTypeParameters(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	attestationService := mock_services.NewMockPayloadAttestationService(ctrl)
	attestationService.EXPECT().ProcessRESTMessage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *cltypes.PayloadAttestationMessage, publish func() error) error {
			return publish()
		},
	)
	handler.payloadAttestationService = attestationService
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	gossipManager.EXPECT().Publish(gomock.Any(), gossip.TopicNamePayloadAttestation, gomock.Any()).Return(nil)
	handler.gossipManager = gossipManager
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

func TestPostPayloadAttestationsRejectsQueuedWithoutPooling(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	msg := newTestPayloadAttestationMessage(t, 12, common.HexToHash("0x1234"))
	attestationService := mock_services.NewMockPayloadAttestationService(ctrl)
	attestationService.EXPECT().ProcessRESTMessage(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("%w: %w", services.ErrIgnore, services.ErrAttestationQueued))
	handler.payloadAttestationService = attestationService
	handler.epbsPool = pool.NewEpbsPool()

	body, err := json.Marshal([]*cltypes.PayloadAttestationMessage{msg})
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/payload_attestations", strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconPoolPayloadAttestations(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	_, found := handler.epbsPool.PayloadAttestations.Get(pool.PayloadAttestationKey{Slot: msg.Data.Slot, ValidatorIndex: msg.ValidatorIndex})
	require.False(t, found)
}

func TestPostPayloadAttestationsAcceptsDuplicateWithoutPooling(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	msg := newTestPayloadAttestationMessage(t, 12, common.HexToHash("0x1234"))
	attestationService := mock_services.NewMockPayloadAttestationService(ctrl)
	attestationService.EXPECT().ProcessRESTMessage(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("%w: %w", services.ErrIgnore, services.ErrAttestationDuplicate))
	handler.payloadAttestationService = attestationService
	handler.epbsPool = pool.NewEpbsPool()
	body, err := json.Marshal([]*cltypes.PayloadAttestationMessage{msg})
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/payload_attestations", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconPoolPayloadAttestations(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	_, found := handler.epbsPool.PayloadAttestations.Get(pool.PayloadAttestationKey{Slot: msg.Data.Slot, ValidatorIndex: msg.ValidatorIndex})
	require.False(t, found)
}

func TestPostPayloadAttestationsReturnsPublishFailure(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	msg := newTestPayloadAttestationMessage(t, 12, common.HexToHash("0x1234"))
	attestationService := mock_services.NewMockPayloadAttestationService(ctrl)
	attestationService.EXPECT().ProcessRESTMessage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *cltypes.PayloadAttestationMessage, publish func() error) error {
			return publish()
		},
	)
	handler.payloadAttestationService = attestationService
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	gossipManager.EXPECT().Publish(gomock.Any(), gossip.TopicNamePayloadAttestation, gomock.Any()).Return(errors.New("gossip unavailable"))
	handler.gossipManager = gossipManager
	body, err := json.Marshal([]*cltypes.PayloadAttestationMessage{msg})
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/payload_attestations", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconPoolPayloadAttestations(recorder, request)

	require.Equal(t, http.StatusInternalServerError, recorder.Code, recorder.Body.String())
}

func TestPostPayloadAttestationsRejectsRetryableValidation(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	msg := newTestPayloadAttestationMessage(t, 12, common.HexToHash("0x1234"))
	attestationService := mock_services.NewMockPayloadAttestationService(ctrl)
	attestationService.EXPECT().ProcessRESTMessage(gomock.Any(), gomock.Any(), gomock.Any()).Return(fmt.Errorf("%w: %w", services.ErrIgnore, services.ErrAttestationRetryable))
	handler.payloadAttestationService = attestationService
	body, err := json.Marshal([]*cltypes.PayloadAttestationMessage{msg})
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/payload_attestations", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconPoolPayloadAttestations(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
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
	handler.emitters = beaconevents.NewEventEmitter()
	events := make(chan *beaconevents.EventStream, 1)
	subscription := handler.emitters.Operation().Subscribe(events)
	defer subscription.Unsubscribe()
	fcu.Blocks[common.Hash{}] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 12, Body: cltypes.NewBeaconBody(handler.beaconChainCfg, clparams.GloasVersion)}}

	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelope", strings.NewReader(`{}`))
	request.Header.Set("Content-Type", "application/json; charset=utf-8")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusAccepted, recorder.Code, recorder.Body.String())
	require.Equal(t, beaconevents.OpExecutionPayloadGossip, (<-events).Event)
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

func TestPostExecutionPayloadEnvelopesRejectsNullEnvelope(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelopes",
		strings.NewReader(`{"signed_execution_payload_envelope":null,"kzg_proofs":[],"blobs":[]}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Blob-Data-Included", "true")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	require.NotPanics(t, func() { handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request) })
	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}

func TestPostExecutionPayloadEnvelopeAttachesPendingLocalBlobData(t *testing.T) {
	ctrl := gomock.NewController(t)
	if clparams.GetBeaconConfig() == nil {
		cfg := clparams.MainnetBeaconConfig
		clparams.InitGlobalStaticConfig(&cfg, &clparams.CaplinConfig{})
	}
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	currentSlot := handler.ethClock.GetCurrentSlot()
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(currentSlot).AnyTimes()
	handler.ethClock = clock
	handler.beaconChainCfg.GloasForkEpoch = 0

	blob := goethkzg.Blob{}
	commitment, err := kzg.Ctx().BlobToKZGCommitment(&blob, 0)
	require.NoError(t, err)
	_, proofs, err := peerdasutils.ComputeCellsAndKZGProofs(blob[:])
	require.NoError(t, err)
	require.Len(t, proofs, int(handler.beaconChainCfg.NumberOfColumns))
	bundleProofs := make([]common.Bytes48, len(proofs))
	for i := range proofs {
		bundleProofs[i] = common.Bytes48(proofs[i])
	}

	executionRequests := cltypes.NewExecutionRequestsWithVersion(handler.beaconChainCfg, clparams.GloasVersion)
	executionRequestsRoot, err := executionRequests.HashSSZ()
	require.NoError(t, err)
	payload := cltypes.NewEth1Block(clparams.GloasVersion, handler.beaconChainCfg)
	payload.BlockHash = common.HexToHash("0x1234")
	payload.SlotNumber = currentSlot

	block := cltypes.NewSignedBeaconBlock(handler.beaconChainCfg, clparams.GloasVersion)
	block.Block.Slot = currentSlot
	bid := block.Block.Body.GetSignedExecutionPayloadBid().Message
	bid.BuilderIndex = 3
	bid.Slot = currentSlot
	bid.BlockHash = payload.BlockHash
	bid.ExecutionRequestsRoot = common.Hash(executionRequestsRoot)
	bid.BlobKzgCommitments.Append((*cltypes.KZGCommitment)(&commitment))
	_, ok := handler.pendingBuilderPayloads.Add(currentSlot, bid, &selfBuildPayload{
		Payload: payload, ExecutionRequests: executionRequests, BlobBundles: []BlobBundle{{
			Commitment: common.Bytes48(commitment), Blob: (*cltypes.Blob)(&blob), KzgProofs: bundleProofs,
		}},
	})
	require.True(t, ok)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	fcu.Blocks[common.Hash(blockRoot)] = block

	var columnsWritten atomic.Int32
	columnStorage := blob_storage_mock.NewMockDataColumnStorage(ctrl)
	columnStorage.EXPECT().WriteColumnSidecars(gomock.Any(), common.Hash(blockRoot), gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, common.Hash, int64, *cltypes.DataColumnSidecar) error {
			columnsWritten.Add(1)
			return nil
		}).Times(int(handler.beaconChainCfg.NumberOfColumns))
	handler.columnStorage = columnStorage
	fcu.OnExecutionPayloadFn = func(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error {
		if columnsWritten.Load() != int32(handler.beaconChainCfg.NumberOfColumns) {
			return forkchoice.ErrEIP7594ColumnDataNotAvailable
		}
		return nil
	}

	signedEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: &cltypes.ExecutionPayloadEnvelope{
		Payload: payload, ExecutionRequests: executionRequests, BuilderIndex: bid.BuilderIndex,
		BeaconBlockRoot: common.Hash(blockRoot), ParentBeaconBlockRoot: block.Block.ParentRoot,
	}}
	body, err := json.Marshal(signedEnvelope)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelopes", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Blob-Data-Included", "false")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, int32(handler.beaconChainCfg.NumberOfColumns), columnsWritten.Load())
}

func TestPostExecutionPayloadEnvelopesSSZDecodesReferencedScheduleCapacity(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	currentSlot := handler.ethClock.GetCurrentSlot()
	referencedSlot := currentSlot - 1
	handler.beaconChainCfg.SlotsPerEpoch = 1
	handler.beaconChainCfg.NumberOfColumns = 1
	handler.beaconChainCfg.MaxBlobsPerBlock = 1
	handler.beaconChainCfg.MaxBlobsPerBlockElectra = 1
	handler.beaconChainCfg.BlobSchedule = []clparams.BlobParameters{
		{Epoch: referencedSlot, MaxBlobsPerBlock: 2},
		{Epoch: currentSlot, MaxBlobsPerBlock: 1},
	}

	contents := cltypes.NewSignedExecutionPayloadEnvelopeContents(handler.beaconChainCfg, referencedSlot)
	root := common.Hash{0x42}
	contents.SignedExecutionPayloadEnvelope.Message.BeaconBlockRoot = root
	for i := byte(1); i <= 2; i++ {
		contents.Blobs.Append(&cltypes.Blob{i})
		contents.KZGProofs.Append(&cltypes.KZGProof{i})
	}
	body, err := contents.EncodeSSZ(nil)
	require.NoError(t, err)

	validationErr := errors.New("decoded contents reached validation")
	fcu.ValidateExecutionPayloadEnvelopeErr = validationErr
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelopes", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.Header.Set("Eth-Blob-Data-Included", "true")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), validationErr.Error())
}

func TestPostExecutionPayloadEnvelopesUsesReferencedBlockForExactBlobCount(t *testing.T) {
	for _, contentType := range []string{"application/json", "application/octet-stream"} {
		t.Run(contentType, func(t *testing.T) {
			_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
			currentSlot := handler.ethClock.GetCurrentSlot()
			referencedSlot := currentSlot - 1
			handler.beaconChainCfg.SlotsPerEpoch = 1
			handler.beaconChainCfg.NumberOfColumns = 1
			handler.beaconChainCfg.MaxBlobsPerBlock = 1
			handler.beaconChainCfg.MaxBlobsPerBlockElectra = 1
			handler.beaconChainCfg.BlobSchedule = []clparams.BlobParameters{
				{Epoch: referencedSlot, MaxBlobsPerBlock: 1},
				{Epoch: currentSlot, MaxBlobsPerBlock: 2},
			}

			root := common.Hash{0x43}
			block := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{
				Slot: referencedSlot,
				Body: cltypes.NewBeaconBody(handler.beaconChainCfg, clparams.GloasVersion),
			}}
			block.Block.Body.GetSignedExecutionPayloadBid().Message.BlobKzgCommitments.Append(&cltypes.KZGCommitment{1})
			fcu.Blocks[root] = block

			contents := cltypes.NewSignedExecutionPayloadEnvelopeContents(handler.beaconChainCfg, currentSlot)
			contents.SignedExecutionPayloadEnvelope.Message.BeaconBlockRoot = root
			for i := byte(1); i <= 2; i++ {
				contents.Blobs.Append(&cltypes.Blob{i})
				contents.KZGProofs.Append(&cltypes.KZGProof{i})
			}
			var body []byte
			var err error
			if contentType == "application/json" {
				body, err = json.Marshal(contents)
			} else {
				body, err = contents.EncodeSSZ(nil)
			}
			require.NoError(t, err)

			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelopes", bytes.NewReader(body))
			request.Header.Set("Content-Type", contentType)
			request.Header.Set("Eth-Blob-Data-Included", "true")
			request.Header.Set("Eth-Consensus-Version", "gloas")
			recorder := httptest.NewRecorder()
			handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			require.Contains(t, recorder.Body.String(), "counts do not match")
		})
	}
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
	fcu.Headers = map[common.Hash]*cltypes.BeaconBlockHeader{
		root: {Slot: 12, ProposerIndex: 3},
		{2}:  {Slot: 12, ProposerIndex: 3},
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
	handler.emitters = beaconevents.NewEventEmitter()
	events := make(chan *beaconevents.EventStream, 4)
	subscription := handler.emitters.Operation().Subscribe(events)
	defer subscription.Unsubscribe()
	ctrl := gomock.NewController(t)
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	gomock.InOrder(
		gossipManager.EXPECT().Publish(gomock.Any(), gossip.TopicNameExecutionPayload, gomock.Any()).Return(errors.New("gossip unavailable")),
		gossipManager.EXPECT().Publish(gomock.Any(), gossip.TopicNameExecutionPayload, gomock.Any()).Return(nil),
	)
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
	for range 3 {
		<-events
	}
	fcu.Envelopes[root] = envelope
	fcu.OnExecutionPayloadErr = forkchoice.ErrIgnore
	request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelopes", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Blob-Data-Included", "false")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	recorder = httptest.NewRecorder()
	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	select {
	case event := <-events:
		t.Fatalf("retry emitted duplicate event %s", event.Event)
	default:
	}
}

func TestPostExecutionPayloadEnvelopeDuplicateDoesNotRepublishOrEmit(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.gossipManager = gossip_mock.NewMockGossip(gomock.NewController(t))
	handler.emitters = beaconevents.NewEventEmitter()
	events := make(chan *beaconevents.EventStream, 1)
	subscription := handler.emitters.Operation().Subscribe(events)
	defer subscription.Unsubscribe()
	fcu.OnExecutionPayloadErr = forkchoice.ErrIgnore
	persisted := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	fcu.Envelopes[common.Hash{}] = persisted
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	envelope.Signature[0] = 1
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelopes", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Eth-Consensus-Version", "gloas")
	request.Header.Set("Eth-Blob-Data-Included", "false")
	recorder := httptest.NewRecorder()

	handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	select {
	case event := <-events:
		t.Fatalf("unexpected event %s", event.Event)
	default:
	}
}

func TestPostExecutionPayloadEnvelopeConcurrentIdenticalReconcilesPersistedImport(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.emitters = beaconevents.NewEventEmitter()
	events := make(chan *beaconevents.EventStream, 4)
	subscription := handler.emitters.Operation().Subscribe(events)
	defer subscription.Unsubscribe()
	gossipManager := gossip_mock.NewMockGossip(gomock.NewController(t))
	gossipManager.EXPECT().Publish(gomock.Any(), gossip.TopicNameExecutionPayload, gomock.Any()).Return(nil).Times(2)
	handler.gossipManager = gossipManager
	root := common.Hash{7}
	fcu.Blocks[root] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 12, Body: cltypes.NewBeaconBody(handler.beaconChainCfg, clparams.GloasVersion)}}
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	envelope.Message.BeaconBlockRoot = root
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	firstPersisted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var calls atomic.Int32
	fcu.OnExecutionPayloadFn = func(_ context.Context, imported *cltypes.SignedExecutionPayloadEnvelope, _, _ bool) error {
		if calls.Add(1) == 1 {
			fcu.Envelopes[root] = imported
			close(firstPersisted)
			<-releaseFirst
			return nil
		}
		return forkchoice.ErrIgnore
	}
	post := func(payload []byte) *httptest.ResponseRecorder {
		request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelopes", bytes.NewReader(payload))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Eth-Consensus-Version", "gloas")
		request.Header.Set("Eth-Blob-Data-Included", "false")
		recorder := httptest.NewRecorder()
		handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)
		return recorder
	}
	firstResult := make(chan *httptest.ResponseRecorder, 1)
	go func() { firstResult <- post(body) }()
	<-firstPersisted
	second := post(body)
	close(releaseFirst)
	first := <-firstResult
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, http.StatusOK, second.Code)
	eventCounts := map[beaconevents.EventTopic]int{}
	for range 3 {
		eventCounts[(<-events).Event]++
	}
	require.Equal(t, 1, eventCounts[beaconevents.OpExecutionPayloadGossip])
	require.Equal(t, 1, eventCounts[beaconevents.OpExecutionPayload])
	require.Equal(t, 1, eventCounts[beaconevents.OpExecutionPayloadAvailable])
	select {
	case event := <-events:
		t.Fatalf("duplicate import emitted event %s", event.Event)
	default:
	}

	forgedEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	forgedEnvelope.Message.BeaconBlockRoot = root
	forgedEnvelope.Signature[0] = 1
	forged, err := json.Marshal(forgedEnvelope)
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, post(forged).Code)
}

func TestPostExecutionPayloadEnvelopeMissingBlockRequiresRetry(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	ctrl := gomock.NewController(t)
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	handler.gossipManager = gossipManager
	envelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	body, err := json.Marshal(envelope)
	require.NoError(t, err)
	post := func() *httptest.ResponseRecorder {
		request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/execution_payload_envelopes", bytes.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Eth-Consensus-Version", "gloas")
		request.Header.Set("Eth-Blob-Data-Included", "false")
		recorder := httptest.NewRecorder()
		handler.PostEthV1BeaconExecutionPayloadEnvelope(recorder, request)
		return recorder
	}
	fcu.OnExecutionPayloadErr = forkchoice.ErrIgnore
	require.Equal(t, http.StatusServiceUnavailable, post().Code)
	fcu.OnExecutionPayloadErr = nil
	gossipManager.EXPECT().Publish(gomock.Any(), gossip.TopicNameExecutionPayload, gomock.Any()).Return(nil)
	require.Equal(t, http.StatusOK, post().Code)
}

func TestEmitFullHeadV2DropsChangedPayloadSnapshot(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.emitters = beaconevents.NewEventEmitter()
	events := make(chan *beaconevents.EventStream, 1)
	subscription := handler.emitters.State().Subscribe(events)
	defer subscription.Unsubscribe()
	root := common.HexToHash("0x1234")
	block := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 1, Body: cltypes.NewBeaconBody(handler.beaconChainCfg, clparams.GloasVersion)}}
	fcu.HeadVal = root
	fcu.HeadSlotVal = 1
	fcu.HeadPayloadStatusVal = cltypes.PayloadStatusFull
	fcu.GetStateAtBlockRootFn = func(common.Hash, bool) (*state.CachingBeaconState, error) {
		fcu.HeadPayloadStatusVal = cltypes.PayloadStatusPending
		headState := state.New(handler.beaconChainCfg)
		headState.SetVersion(clparams.GloasVersion)
		require.NoError(t, headState.SetSlot(1))
		require.NoError(t, headState.SetBlockRootAt(0, common.Hash{1}))
		return headState, nil
	}

	handler.emitFullHeadV2(block, root)

	select {
	case event := <-events:
		t.Fatalf("unexpected event %s", event.Event)
	default:
	}
}

func TestEmitFullHeadV2DropsChangedOptimismSnapshot(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.emitters = beaconevents.NewEventEmitter()
	events := make(chan *beaconevents.EventStream, 1)
	subscription := handler.emitters.State().Subscribe(events)
	defer subscription.Unsubscribe()
	root := common.HexToHash("0x1234")
	block := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 1, Body: cltypes.NewBeaconBody(handler.beaconChainCfg, clparams.GloasVersion)}}
	fcu.HeadVal = root
	fcu.HeadSlotVal = 1
	fcu.HeadPayloadStatusVal = cltypes.PayloadStatusFull
	fcu.GetStateAtBlockRootFn = func(common.Hash, bool) (*state.CachingBeaconState, error) {
		fcu.IsRootOptimisticVal = true
		headState := state.New(handler.beaconChainCfg)
		headState.SetVersion(clparams.GloasVersion)
		require.NoError(t, headState.SetSlot(1))
		require.NoError(t, headState.SetBlockRootAt(0, common.Hash{1}))
		return headState, nil
	}

	handler.emitFullHeadV2(block, root)

	select {
	case event := <-events:
		t.Fatalf("unexpected event %s", event.Event)
	default:
	}
}

func TestGetExecutionPayloadEnvelopeDoesNotFinalizeSameSlotSideBranch(t *testing.T) {
	db, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	root := common.HexToHash("0x1234")
	slot := uint64(12)
	fcu.Envelopes[root] = &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	fcu.Blocks[root] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: slot, Body: cltypes.NewBeaconBody(handler.beaconChainCfg, clparams.GloasVersion)}}
	fcu.FinalizedSlotVal = slot
	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 2, Root: common.HexToHash("0xbeef")}
	fcu.Ancestors[slot] = forkchoice.ForkChoiceNode{Root: common.HexToHash("0xbeef")}
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(t.Context(), tx, slot, common.HexToHash("0xbeef"))
	}))
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/beacon/execution_payload_envelope/"+root.Hex(), http.NoBody)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), `"finalized":false`)
}

func TestGetExecutionPayloadEnvelopeFinalizesCheckpointRootOnly(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	root := common.HexToHash("0x1234")
	slot := uint64(64)
	fcu.Envelopes[root] = &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	fcu.Blocks[root] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: slot, Body: cltypes.NewBeaconBody(handler.beaconChainCfg, clparams.GloasVersion)}}
	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 2, Root: root}
	fcu.Ancestors[slot] = forkchoice.ForkChoiceNode{Root: root}
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/beacon/execution_payload_envelope/"+root.Hex(), http.NoBody)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), `"finalized":true`)
}

func TestGetExecutionPayloadEnvelopeFinalityBoundaryMatrix(t *testing.T) {
	for _, test := range []struct {
		name      string
		slot      uint64
		ancestor  bool
		finalized bool
	}{
		{name: "earlier canonical ancestor", slot: 63, ancestor: true, finalized: true},
		{name: "descendant", slot: 65, ancestor: true, finalized: false},
		{name: "earlier side branch", slot: 63, ancestor: false, finalized: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
			root := common.HexToHash("0x1234")
			finalizedRoot := common.HexToHash("0xbeef")
			fcu.Envelopes[root] = &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
			fcu.Blocks[root] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: test.slot, Body: cltypes.NewBeaconBody(handler.beaconChainCfg, clparams.GloasVersion)}}
			fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 2, Root: finalizedRoot}
			if test.ancestor {
				fcu.Ancestors[test.slot] = forkchoice.ForkChoiceNode{Root: root}
			} else {
				fcu.Ancestors[test.slot] = forkchoice.ForkChoiceNode{Root: common.HexToHash("0xdead")}
			}
			request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/beacon/execution_payload_envelope/"+root.Hex(), http.NoBody)
			recorder := httptest.NewRecorder()

			handler.ServeHTTP(recorder, request)

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			require.Contains(t, recorder.Body.String(), fmt.Sprintf(`"finalized":%t`, test.finalized))
		})
	}
}

func TestGetExecutionPayloadEnvelopeRejectsZeroSlotsPerEpoch(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	root := common.HexToHash("0x1234")
	fcu.Envelopes[root] = &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(handler.beaconChainCfg)}
	fcu.Blocks[root] = &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: 64, Body: cltypes.NewBeaconBody(handler.beaconChainCfg, clparams.GloasVersion)}}
	handler.beaconChainCfg.SlotsPerEpoch = 0
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/eth/v1/beacon/execution_payload_envelope/"+root.Hex(), http.NoBody)
	recorder := httptest.NewRecorder()

	require.NotPanics(t, func() { handler.ServeHTTP(recorder, request) })
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
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

func TestGetValidatorExecutionPayloadBidBuildsUnsignedBidWithoutGossip(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, forkchoiceStore, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	handler.beaconChainCfg.FuluForkEpoch = 0
	handler.beaconChainCfg.GloasForkEpoch = 0
	handler.beaconChainCfg.InitializeForkSchedule()
	require.NoError(t, postState.UpgradeToFulu())
	require.NoError(t, postState.UpgradeToGloas())
	postState.SetFinalizedCheckpoint(solid.Checkpoint{Epoch: 1})
	for range 4 {
		postState.GetBuilders().Append(&cltypes.Builder{
			Version:           handler.beaconChainCfg.PayloadBuilderVersion,
			Balance:           handler.beaconChainCfg.MinDepositAmount + 10,
			WithdrawableEpoch: handler.beaconChainCfg.FarFutureEpoch,
		})
	}
	handler.epbsPool = pool.NewEpbsPool()
	slot := postState.Slot() + 1
	proposerIndex, err := postState.GetBeaconProposerIndexForSlot(slot)
	require.NoError(t, err)
	dependentRoot, err := state.GetProposerDependentRoot(postState, state.GetEpochAtSlot(handler.beaconChainCfg, slot))
	require.NoError(t, err)
	feeRecipient := common.Address{0x42}
	handler.epbsPool.ProposerPreferences.Add(
		pool.ProposerPreferencesKey{Slot: slot, DependentRoot: dependentRoot},
		&cltypes.SignedProposerPreferences{Message: &cltypes.ProposerPreferences{
			ProposalSlot: slot, ValidatorIndex: proposerIndex, FeeRecipient: feeRecipient,
			TargetGasLimit: 30_000_000, DependentRoot: dependentRoot,
		}},
	)
	parentBid := postState.GetLatestExecutionPayloadBid()
	require.NotNil(t, parentBid)
	parentBid.ParentBlockHash = common.HexToHash("0xaaaa")
	parentBid.BlockHash = common.HexToHash("0xbbbb")
	require.NoError(t, handler.syncedData.OnHeadState(postState))
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(slot).AnyTimes()
	clock.EXPECT().GetSlotTime(slot).Return(time.Now()).AnyTimes()
	handler.ethClock = clock

	baseRoot, err := postState.BlockRoot()
	require.NoError(t, err)
	forkchoiceStore.HeadVal = baseRoot
	forkchoiceStore.HeadPayloadStatusVal = cltypes.PayloadStatusEmpty
	payload := cltypes.NewEth1Block(clparams.GloasVersion, handler.beaconChainCfg)
	payload.ParentHash = parentBid.ParentBlockHash
	payload.BlockHash = common.HexToHash("0x1234")
	payload.SlotNumber = slot
	payload.FeeRecipient = feeRecipient
	payload.GasLimit = 30_000_000
	payload.Extra = solid.NewExtraData()
	payload.Transactions = solid.NewTransactionsSSZFromTransactions(nil)
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(handler.beaconChainCfg.MaxWithdrawalsPerPayload), 44)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	var gotFeeRecipient common.Address
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), clparams.GloasVersion).
		DoAndReturn(func(_ context.Context, _, _, _ common.Hash, attrs *engine_types.PayloadAttributes, _ clparams.StateVersion) ([]byte, error) {
			gotFeeRecipient = attrs.SuggestedFeeRecipient
			return []byte{1}, nil
		}).Times(6)
	var assembled atomic.Uint32
	engine.EXPECT().GetAssembledBlock(gomock.Any(), []byte{1}, clparams.GloasVersion).
		DoAndReturn(func(context.Context, []byte, clparams.StateVersion) (*cltypes.Eth1Block, *engine_types.BlobsBundle, *typesproto.RequestsBundle, *big.Int, error) {
			built := *payload
			if call := assembled.Add(1); call > 1 {
				built.BlockHash = common.Hash{31: byte(call)}
			}
			return &built, &engine_types.BlobsBundle{}, nil, big.NewInt(2_000_000_000), nil
		}).Times(6)
	handler.engine = engine

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, fmt.Sprintf("/eth/v1/validator/execution_payload_bids/%d/3", slot), http.NoBody)
	recorder := httptest.NewRecorder()

	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, feeRecipient, gotFeeRecipient)
	require.Contains(t, recorder.Body.String(), `"builder_index":"3"`)
	require.Contains(t, recorder.Body.String(), `"block_hash":"0x0000000000000000000000000000000000000000000000000000000000001234"`)
	require.Contains(t, recorder.Body.String(), `"value":"0"`)
	require.Contains(t, recorder.Body.String(), `"execution_payment":"2"`)
	require.Contains(t, recorder.Body.String(), `"fee_recipient":"0x4200000000000000000000000000000000000000"`)
	require.NotContains(t, recorder.Body.String(), `"signature"`)
	require.NotContains(t, recorder.Body.String(), `"message"`)
	for range 4 {
		recorder = httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
		require.Contains(t, recorder.Body.String(), `"block_hash":"0x0000000000000000000000000000000000000000000000000000000000001234"`)
	}

	forkchoiceStore.Envelopes[baseRoot] = &cltypes.SignedExecutionPayloadEnvelope{}
	var headSnapshots atomic.Int32
	forkchoiceStore.GetHeadNodeFn = func() (forkchoice.ForkChoiceNode, error) {
		status := cltypes.PayloadStatusEmpty
		if headSnapshots.Add(1) > 1 {
			status = cltypes.PayloadStatusFull
		}
		return forkchoice.ForkChoiceNode{Root: baseRoot, PayloadStatus: status}, nil
	}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusNotFound, recorder.Code, recorder.Body.String())

	for _, path := range []string{
		"/eth/v1/validator/execution_payload_bid/0/3",
		"/eth/v1/validator/execution_payload_bid/18446744073709551615/3",
		fmt.Sprintf("/eth/v1/validator/execution_payload_bid/%d/4", slot),
	} {
		recorder = httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, http.NoBody))
		require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}
}

func TestGetValidatorExecutionPayloadBidRejectsInactiveBuilder(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, postState, handler, _, _, forkchoiceStore, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	handler.beaconChainCfg.FuluForkEpoch = 0
	handler.beaconChainCfg.GloasForkEpoch = 0
	handler.beaconChainCfg.InitializeForkSchedule()
	require.NoError(t, postState.UpgradeToFulu())
	require.NoError(t, postState.UpgradeToGloas())
	postState.GetBuilders().Append(&cltypes.Builder{})
	require.NoError(t, handler.syncedData.OnHeadState(postState))
	slot := postState.Slot() + 1
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(slot).AnyTimes()
	clock.EXPECT().GetSlotTime(slot).Return(time.Now()).AnyTimes()
	handler.ethClock = clock
	baseRoot, err := postState.BlockRoot()
	require.NoError(t, err)
	forkchoiceStore.HeadVal = baseRoot
	forkchoiceStore.HeadPayloadStatusVal = cltypes.PayloadStatusEmpty
	payload := cltypes.NewEth1Block(clparams.GloasVersion, handler.beaconChainCfg)
	payload.Extra = solid.NewExtraData()
	payload.Transactions = solid.NewTransactionsSSZFromTransactions(nil)
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(handler.beaconChainCfg.MaxWithdrawalsPerPayload), 44)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), clparams.GloasVersion).
		Return([]byte{1}, nil).AnyTimes()
	engine.EXPECT().GetAssembledBlock(gomock.Any(), []byte{1}, clparams.GloasVersion).
		Return(payload, &engine_types.BlobsBundle{}, nil, big.NewInt(1), nil).AnyTimes()
	handler.engine = engine

	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet,
		fmt.Sprintf("/eth/v1/validator/execution_payload_bids/%d/0", slot), http.NoBody)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
}

func TestGetValidatorExecutionPayloadEnvelopeBuildsFromSelectedLocalBid(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, _, _, _, _, handler, _, _, forkchoiceStore, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	handler.beaconChainCfg.GloasForkEpoch = 0
	slot := uint64(3)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(slot).AnyTimes()
	handler.ethClock = clock

	payloadHash := common.HexToHash("0x1234")
	payload := cltypes.NewEth1Block(clparams.GloasVersion, handler.beaconChainCfg)
	payload.BlockHash = payloadHash
	payload.SlotNumber = slot
	executionRequests := cltypes.NewExecutionRequestsWithVersion(handler.beaconChainCfg, clparams.GloasVersion)
	pending := &selfBuildPayload{
		Payload:           payload,
		ExecutionRequests: executionRequests,
	}
	for i := byte(1); i <= 4; i++ {
		handler.selfBuildPayloads.Add(common.Hash{i}, &selfBuildPayload{Payload: cltypes.NewEth1Block(clparams.GloasVersion, handler.beaconChainCfg)})
	}
	block := cltypes.NewSignedBeaconBlock(handler.beaconChainCfg, clparams.GloasVersion)
	block.Block.Slot = slot
	block.Block.ParentRoot = common.HexToHash("0xabcd")
	block.Block.Body.SignedExecutionPayloadBid.Message.BuilderIndex = 3
	block.Block.Body.SignedExecutionPayloadBid.Message.Slot = slot
	block.Block.Body.SignedExecutionPayloadBid.Message.BlockHash = payloadHash
	executionRequestsRoot, err := executionRequests.HashSSZ()
	require.NoError(t, err)
	block.Block.Body.SignedExecutionPayloadBid.Message.ExecutionRequestsRoot = common.Hash(executionRequestsRoot)
	_, ok := handler.pendingBuilderPayloads.Add(slot, block.Block.Body.SignedExecutionPayloadBid.Message, pending)
	require.True(t, ok)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	forkchoiceStore.Blocks[common.Hash(blockRoot)] = block

	forkchoiceStore.HeadVal = common.HexToHash("0xffff")
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet,
		fmt.Sprintf("/eth/v1/validator/execution_payload_envelopes/%d/%s", slot, common.Hash(blockRoot)), http.NoBody)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusNotFound, recorder.Code, recorder.Body.String())

	forkchoiceStore.HeadVal = common.Hash(blockRoot)
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), `"builder_index":"3"`)
	require.Contains(t, recorder.Body.String(), `"block_hash":"0x0000000000000000000000000000000000000000000000000000000000001234"`)

	forkchoiceStore.HeadVal = common.HexToHash("0xeeee")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusNotFound, recorder.Code, recorder.Body.String())
}

func TestPendingBuilderPayloadStoreRefusesEvictionUntilSlotExpires(t *testing.T) {
	store := newPendingBuilderPayloadStore(1)
	firstHash := common.HexToHash("0x01")
	secondHash := common.HexToHash("0x02")
	first := &selfBuildPayload{}
	second := &selfBuildPayload{}

	firstBid := newTestExecutionPayloadBid(3, 1, 0)
	firstBid.BlockHash = firstHash
	secondBid := newTestExecutionPayloadBid(3, 1, 0)
	secondBid.BlockHash = secondHash
	_, ok := store.Add(3, firstBid, first)
	require.True(t, ok)
	coalesced, ok := store.Add(3, secondBid, second)
	require.True(t, ok)
	require.Equal(t, firstHash, coalesced.BlockHash)
	got, ok := store.Get(3, firstBid)
	require.True(t, ok)
	require.Same(t, first, got)
	_, ok = store.Get(3, secondBid)
	require.False(t, ok)
	thirdBid := newTestExecutionPayloadBid(3, 2, 0)
	thirdBid.BlockHash = secondHash
	coalescedForOtherBuilder, ok := store.Add(3, thirdBid, second)
	require.True(t, ok)
	require.Equal(t, uint64(2), coalescedForOtherBuilder.BuilderIndex)
	require.Equal(t, firstHash, coalescedForOtherBuilder.BlockHash)
	got, ok = store.Get(3, coalescedForOtherBuilder)
	require.True(t, ok)
	require.Same(t, first, got)
	require.Len(t, store.entries, 1)
	fourthBid := newTestExecutionPayloadBid(4, 2, 0)
	fourthBid.BlockHash = secondHash
	_, ok = store.Add(4, fourthBid, second)
	require.True(t, ok)
	_, ok = store.Get(4, firstBid)
	require.False(t, ok)

	headAwareStore := newPendingBuilderPayloadStore(2)
	firstBid.ParentBlockRoot = common.Hash{1}
	secondBid.ParentBlockRoot = common.Hash{2}
	_, ok = headAwareStore.Add(3, firstBid, first)
	require.True(t, ok)
	differentHead, ok := headAwareStore.Add(3, secondBid, second)
	require.True(t, ok)
	require.Equal(t, common.Hash{2}, differentHead.ParentBlockRoot)
	_, firstStillAvailable := headAwareStore.Get(3, firstBid)
	require.True(t, firstStillAvailable)
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
	_, _, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	handler.beaconChainCfg.GloasForkEpoch = 0
	slot := uint64(64)
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentSlot().Return(slot).AnyTimes()
	handler.ethClock = clock
	root := common.HexToHash("0x1234")
	fcu.HeadVal = root
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
		{name: "same slot alternate root", slot: slot, root: otherRoot, want: http.StatusNotFound},
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
