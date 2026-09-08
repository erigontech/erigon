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
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	sync_mock_services "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/raw"
	gossip_mock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	"github.com/erigontech/erigon/cl/phase1/network/services/mock_services"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

func TestPoolAttesterSlashings(t *testing.T) {
	attesterSlashing := cltypes.NewAttesterSlashing(clparams.DenebVersion)
	attesterSlashing.Attestation_1.AttestingIndices = solid.NewRawUint64List(2048, []uint64{2, 3, 4, 5, 6})
	attesterSlashing.Attestation_2.AttestingIndices = solid.NewRawUint64List(2048, []uint64{2, 3, 4, 1, 6})
	// find server
	_, _, _, _, _, handler, _, syncedDataMgr, _, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)
	mockBeaconState := &state.CachingBeaconState{BeaconState: raw.New(&clparams.BeaconChainConfig{})}
	mockBeaconState.SetVersion(clparams.DenebVersion)
	syncedDataMgr.(*sync_mock_services.MockSyncedData).EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(vhsf synced_data.ViewHeadStateFn) error {
		return vhsf(mockBeaconState)
	}).AnyTimes()

	server := httptest.NewServer(handler.mux)
	defer server.Close()
	// json
	req, err := json.Marshal(attesterSlashing)
	require.NoError(t, err)
	// post attester slashing
	postReq, err := http.NewRequestWithContext(t.Context(), "POST", server.URL+"/eth/v1/beacon/pool/attester_slashings", bytes.NewBuffer(req))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(postReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, 200, resp.StatusCode)
	// get attester slashings
	getReq, err := http.NewRequestWithContext(t.Context(), "GET", server.URL+"/eth/v1/beacon/pool/attester_slashings", nil)
	require.NoError(t, err)
	resp, err = server.Client().Do(getReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, 200, resp.StatusCode)
	out := struct {
		Data []*cltypes.AttesterSlashing `json:"data"`
	}{
		Data: []*cltypes.AttesterSlashing{
			cltypes.NewAttesterSlashing(clparams.DenebVersion),
		},
	}

	err = json.NewDecoder(resp.Body).Decode(&out)
	require.NoError(t, err)

	require.Len(t, out.Data, 1)
	require.Equal(t, attesterSlashing, out.Data[0])
}

func TestPoolProposerSlashings(t *testing.T) {
	proposerSlashing := &cltypes.ProposerSlashing{
		Header1: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot:          1,
				ProposerIndex: 3,
			},
		},
		Header2: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{
				Slot:          2,
				ProposerIndex: 4,
			},
		},
	}
	// find server
	_, _, _, _, _, handler, _, syncedDataMgr, _, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)
	mockBeaconState := &state.CachingBeaconState{BeaconState: raw.New(&clparams.BeaconChainConfig{})}
	mockBeaconState.SetVersion(clparams.DenebVersion)
	syncedDataMgr.(*sync_mock_services.MockSyncedData).EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(vhsf synced_data.ViewHeadStateFn) error {
		return vhsf(mockBeaconState)
	}).AnyTimes()
	server := httptest.NewServer(handler.mux)
	defer server.Close()
	// json
	req, err := json.Marshal(proposerSlashing)
	require.NoError(t, err)

	// post attester slashing
	postReq, err := http.NewRequestWithContext(t.Context(), "POST", server.URL+"/eth/v1/beacon/pool/proposer_slashings", bytes.NewBuffer(req))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(postReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, 200, resp.StatusCode)
	// get proposer slashings
	getReq, err := http.NewRequestWithContext(t.Context(), "GET", server.URL+"/eth/v1/beacon/pool/proposer_slashings", nil)
	require.NoError(t, err)
	resp, err = server.Client().Do(getReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, 200, resp.StatusCode)
	out := struct {
		Data []*cltypes.ProposerSlashing `json:"data"`
	}{
		Data: []*cltypes.ProposerSlashing{},
	}

	err = json.NewDecoder(resp.Body).Decode(&out)
	require.NoError(t, err)

	require.Len(t, out.Data, 1)
	require.Equal(t, proposerSlashing, out.Data[0])
}

func TestPoolVoluntaryExits(t *testing.T) {
	voluntaryExit := &cltypes.SignedVoluntaryExit{
		VoluntaryExit: &cltypes.VoluntaryExit{
			Epoch:          1,
			ValidatorIndex: 3,
		},
	}
	// find server
	_, _, _, _, _, handler, _, syncedDataMgr, _, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)
	mockBeaconState := &state.CachingBeaconState{BeaconState: raw.New(&clparams.BeaconChainConfig{})}
	mockBeaconState.SetVersion(clparams.DenebVersion)
	syncedDataMgr.(*sync_mock_services.MockSyncedData).EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(vhsf synced_data.ViewHeadStateFn) error {
		return vhsf(mockBeaconState)
	}).AnyTimes()
	server := httptest.NewServer(handler.mux)
	defer server.Close()
	// json
	req, err := json.Marshal(voluntaryExit)
	require.NoError(t, err)
	// post attester slashing
	postReq, err := http.NewRequestWithContext(t.Context(), "POST", server.URL+"/eth/v1/beacon/pool/voluntary_exits", bytes.NewBuffer(req))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(postReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, 200, resp.StatusCode)
	// get voluntary exits
	getReq, err := http.NewRequestWithContext(t.Context(), "GET", server.URL+"/eth/v1/beacon/pool/voluntary_exits", nil)
	require.NoError(t, err)
	resp, err = server.Client().Do(getReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, 200, resp.StatusCode)
	out := struct {
		Data []*cltypes.SignedVoluntaryExit `json:"data"`
	}{
		Data: []*cltypes.SignedVoluntaryExit{},
	}

	err = json.NewDecoder(resp.Body).Decode(&out)
	require.NoError(t, err)

	require.Len(t, out.Data, 1)
	require.Equal(t, voluntaryExit, out.Data[0])
}

func TestPoolVoluntaryExitsRejectIgnoredValidation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		epoch     uint64
		exitEpoch uint64
	}{
		{name: "future epoch", epoch: 101, exitEpoch: math.MaxUint64},
		{name: "maximum epoch", epoch: math.MaxUint64, exitEpoch: math.MaxUint64},
		{name: "initiated exit", epoch: 100, exitEpoch: 101},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, _, head, handler, opPool, syncedData, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
			cfg := handler.beaconChainCfg
			require.NoError(t, head.SetSlot(100*cfg.SlotsPerEpoch))
			head.ValidatorSet().Set(0, solid.NewValidatorFromParameters(common.Bytes48{}, common.Hash{}, 0, false, 0, 0, tc.exitEpoch, cfg.FarFutureEpoch))
			require.NoError(t, syncedData.OnHeadState(head))
			ctrl := gomock.NewController(t)
			clock := eth_clock.NewMockEthereumClock(ctrl)
			clock.EXPECT().GetSlotTime(uint64(0)).Return(time.Unix(0, 0))
			clock.EXPECT().GetSlotByTime(gomock.Any()).Return(100 * cfg.SlotsPerEpoch)
			clock.EXPECT().GetEpochAtSlot(100 * cfg.SlotsPerEpoch).Return(100)
			handler.voluntaryExitService = services.NewVoluntaryExitService(opPool, beaconevents.NewEventEmitter(), syncedData, cfg, clock, services.NewBatchSignatureVerifier(t.Context(), nil))
			gossipManager := gossip_mock.NewMockGossip(ctrl)
			gossipManager.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
			handler.gossipManager = gossipManager
			body, err := json.Marshal(&cltypes.SignedVoluntaryExit{VoluntaryExit: &cltypes.VoluntaryExit{Epoch: tc.epoch, ValidatorIndex: 0}})
			require.NoError(t, err)
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/voluntary_exits", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			handler.mux.ServeHTTP(response, req)
			require.Equal(t, http.StatusBadRequest, response.Code)
			var endpointError beaconhttp.EndpointError
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &endpointError))
			require.Equal(t, http.StatusBadRequest, endpointError.Code)
			require.Equal(t, services.ErrIgnore.Error(), endpointError.Message)
			require.False(t, opPool.VoluntaryExitsPool.Has(0))
		})
	}
}

func TestPoolVoluntaryExitsValidationResult(t *testing.T) {
	voluntaryExit := &cltypes.SignedVoluntaryExit{VoluntaryExit: &cltypes.VoluntaryExit{Epoch: 1, ValidatorIndex: 3}}
	for _, tc := range []struct {
		name          string
		validationErr error
		stored        *cltypes.SignedVoluntaryExit
		storeNil      bool
		status        int
		publishes     int
	}{
		{name: "valid", status: http.StatusOK, publishes: 1},
		{name: "invalid", validationErr: errors.New("invalid signature"), status: http.StatusBadRequest},
		{name: "ignored", validationErr: services.ErrIgnore, status: http.StatusBadRequest},
		{name: "wrapped ignore", validationErr: fmt.Errorf("validation: %w", services.ErrIgnore), status: http.StatusBadRequest},
		{name: "retained duplicate", validationErr: services.ErrIgnore, stored: voluntaryExit, status: http.StatusOK, publishes: 1},
		{name: "wrapped retained duplicate", validationErr: fmt.Errorf("validation: %w", services.ErrIgnore), stored: voluntaryExit, status: http.StatusOK, publishes: 1},
		{name: "hard error with retained duplicate", validationErr: errors.New("invalid signature"), stored: voluntaryExit, status: http.StatusBadRequest},
		{name: "nil retained exit", validationErr: services.ErrIgnore, storeNil: true, status: http.StatusBadRequest},
		{name: "nil retained message", validationErr: services.ErrIgnore, stored: &cltypes.SignedVoluntaryExit{}, status: http.StatusBadRequest},
		{name: "different signature", validationErr: services.ErrIgnore, stored: &cltypes.SignedVoluntaryExit{VoluntaryExit: voluntaryExit.VoluntaryExit, Signature: common.Bytes96{1}}, status: http.StatusBadRequest},
		{name: "different epoch", validationErr: services.ErrIgnore, stored: &cltypes.SignedVoluntaryExit{VoluntaryExit: &cltypes.VoluntaryExit{Epoch: 2, ValidatorIndex: 3}}, status: http.StatusBadRequest},
		{name: "different validator index", validationErr: services.ErrIgnore, stored: &cltypes.SignedVoluntaryExit{VoluntaryExit: &cltypes.VoluntaryExit{Epoch: 1, ValidatorIndex: 4}}, status: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, _, _, handler, _, _, _, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)
			ctrl := gomock.NewController(t)
			if tc.stored != nil || tc.storeNil {
				handler.operationsPool.VoluntaryExitsPool.Insert(voluntaryExit.VoluntaryExit.ValidatorIndex, tc.stored)
			}
			service := mock_services.NewMockVoluntaryExitService(ctrl)
			service.EXPECT().ProcessMessage(gomock.Any(), nil, &services.SignedVoluntaryExitForGossip{
				SignedVoluntaryExit: voluntaryExit, ImmediateVerification: true,
			}).Return(tc.validationErr)
			handler.voluntaryExitService = service
			encodedSSZ, err := voluntaryExit.EncodeSSZ(nil)
			require.NoError(t, err)
			gossipManager := gossip_mock.NewMockGossip(ctrl)
			gossipManager.EXPECT().Publish(gomock.Any(), gossip.TopicNameVoluntaryExit, encodedSSZ).Return(nil).Times(tc.publishes)
			handler.gossipManager = gossipManager
			body, err := json.Marshal(voluntaryExit)
			require.NoError(t, err)
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/voluntary_exits", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			handler.mux.ServeHTTP(response, req)
			require.Equal(t, tc.status, response.Code)
			if tc.status == http.StatusBadRequest {
				var endpointError beaconhttp.EndpointError
				require.NoError(t, json.Unmarshal(response.Body.Bytes(), &endpointError))
				require.Equal(t, http.StatusBadRequest, endpointError.Code)
				require.Equal(t, tc.validationErr.Error(), endpointError.Message)
			}
		})
	}
}

func TestPoolVoluntaryExitsRetryFailedPublish(t *testing.T) {
	_, _, _, _, head, handler, opPool, syncedData, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	cfg := handler.beaconChainCfg
	require.NoError(t, head.SetSlot(1000*cfg.SlotsPerEpoch))
	key, err := bls.GenerateKey()
	require.NoError(t, err)
	var publicKey common.Bytes48
	copy(publicKey[:], bls.CompressPublicKey(key.PublicKey()))
	head.ValidatorSet().Set(0, solid.NewValidatorFromParameters(publicKey, common.Hash{}, 0, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch))
	require.NoError(t, syncedData.OnHeadState(head))
	exit := &cltypes.SignedVoluntaryExit{VoluntaryExit: &cltypes.VoluntaryExit{Epoch: 1000, ValidatorIndex: 0}}
	domain, err := head.GetDomain(cfg.DomainVoluntaryExit, exit.VoluntaryExit.Epoch)
	require.NoError(t, err)
	root, err := fork.ComputeSigningRoot(exit.VoluntaryExit, domain)
	require.NoError(t, err)
	copy(exit.Signature[:], key.Sign(root[:]).Bytes())
	ctrl := gomock.NewController(t)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetSlotTime(uint64(0)).Return(time.Unix(0, 0))
	clock.EXPECT().GetSlotByTime(gomock.Any()).Return(1000 * cfg.SlotsPerEpoch)
	clock.EXPECT().GetEpochAtSlot(1000 * cfg.SlotsPerEpoch).Return(1000)
	handler.voluntaryExitService = services.NewVoluntaryExitService(opPool, beaconevents.NewEventEmitter(), syncedData, cfg, clock, services.NewBatchSignatureVerifier(t.Context(), nil))
	encodedSSZ, err := exit.EncodeSSZ(nil)
	require.NoError(t, err)
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	gomock.InOrder(
		gossipManager.EXPECT().Publish(gomock.Any(), gossip.TopicNameVoluntaryExit, encodedSSZ).Return(errors.New("temporary publish failure")),
		gossipManager.EXPECT().Publish(gomock.Any(), gossip.TopicNameVoluntaryExit, encodedSSZ).Return(nil),
	)
	handler.gossipManager = gossipManager
	body, err := json.Marshal(exit)
	require.NoError(t, err)
	for attempt := range 2 {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/voluntary_exits", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		handler.mux.ServeHTTP(response, req)
		require.Equal(t, http.StatusOK, response.Code, "attempt %d: %s", attempt+1, response.Body.String())
		stored, ok := opPool.VoluntaryExitsPool.Get(0)
		require.True(t, ok)
		require.Equal(t, exit, stored)
	}
}

func TestPoolBlsToExecutionChainges(t *testing.T) {
	msg := []*cltypes.SignedBLSToExecutionChange{
		{
			Message: &cltypes.BLSToExecutionChange{
				ValidatorIndex: 45,
			},
			Signature: common.Bytes96{2},
		},
		{
			Message: &cltypes.BLSToExecutionChange{
				ValidatorIndex: 46,
			},
		},
	}
	// find server
	_, _, _, _, _, handler, _, syncedDataMgr, _, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)
	mockBeaconState := &state.CachingBeaconState{BeaconState: raw.New(&clparams.BeaconChainConfig{})}
	mockBeaconState.SetVersion(clparams.DenebVersion)
	syncedDataMgr.(*sync_mock_services.MockSyncedData).EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(vhsf synced_data.ViewHeadStateFn) error {
		return vhsf(mockBeaconState)
	}).AnyTimes()

	server := httptest.NewServer(handler.mux)
	defer server.Close()
	// json
	req, err := json.Marshal(msg)
	require.NoError(t, err)
	// post attester slashing
	postReq, err := http.NewRequestWithContext(t.Context(), "POST", server.URL+"/eth/v1/beacon/pool/bls_to_execution_changes", bytes.NewBuffer(req))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(postReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, 200, resp.StatusCode)
	// get bls to execution changes
	getReq, err := http.NewRequestWithContext(t.Context(), "GET", server.URL+"/eth/v1/beacon/pool/bls_to_execution_changes", nil)
	require.NoError(t, err)
	resp, err = server.Client().Do(getReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, 200, resp.StatusCode)
	out := struct {
		Data []*cltypes.SignedBLSToExecutionChange `json:"data"`
	}{
		Data: []*cltypes.SignedBLSToExecutionChange{},
	}

	err = json.NewDecoder(resp.Body).Decode(&out)
	require.NoError(t, err)

	require.Len(t, out.Data, 2)
	require.Equal(t, msg[0], out.Data[0])
	require.Equal(t, msg[1], out.Data[1])
}

func TestPoolAggregatesAndProofs(t *testing.T) {
	msg := []*cltypes.SignedAggregateAndProof{
		{
			Message: &cltypes.AggregateAndProof{
				Aggregate: &solid.Attestation{
					AggregationBits: solid.BitlistFromBytes([]byte{1, 2}, 2048),
					Data:            &solid.AttestationData{},
					Signature:       common.Bytes96{3, 45, 6},
				},
			},
			Signature: common.Bytes96{2},
		},
		{
			Message: &cltypes.AggregateAndProof{
				// Aggregate: solid.NewAttestionFromParameters([]byte{1, 2, 5, 6}, solid.NewAttestationData(), common.Bytes96{3, 0, 6}),
				Aggregate: &solid.Attestation{
					AggregationBits: solid.BitlistFromBytes([]byte{1, 2, 5, 6}, 2048),
					Data:            &solid.AttestationData{},
					Signature:       common.Bytes96{3, 0, 6},
				},
			},
			Signature: common.Bytes96{2, 3, 5},
		},
	}
	// find server
	_, _, _, _, _, handler, _, syncedDataMgr, _, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)
	mockBeaconState := &state.CachingBeaconState{BeaconState: raw.New(&clparams.BeaconChainConfig{})}
	mockBeaconState.SetVersion(clparams.DenebVersion)
	syncedDataMgr.(*sync_mock_services.MockSyncedData).EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(vhsf synced_data.ViewHeadStateFn) error {
		return vhsf(mockBeaconState)
	}).AnyTimes()

	server := httptest.NewServer(handler.mux)
	defer server.Close()
	// json
	req, err := json.Marshal(msg)
	require.NoError(t, err)
	// post attester slashing
	postReq, err := http.NewRequestWithContext(t.Context(), "POST", server.URL+"/eth/v1/validator/aggregate_and_proofs", bytes.NewBuffer(req))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(postReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, 200, resp.StatusCode)
	// get attestations
	getReq, err := http.NewRequestWithContext(t.Context(), "GET", server.URL+"/eth/v1/beacon/pool/attestations", nil)
	require.NoError(t, err)
	resp, err = server.Client().Do(getReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, 200, resp.StatusCode)
	out := struct {
		Data []*solid.Attestation `json:"data"`
	}{
		Data: []*solid.Attestation{},
	}

	err = json.NewDecoder(resp.Body).Decode(&out)
	require.NoError(t, err)

	require.Len(t, out.Data, 2)
	require.Equal(t, msg[0].Message.Aggregate, out.Data[0])
	require.Equal(t, msg[1].Message.Aggregate, out.Data[1])
}

func TestPoolAggregatesAndProofsReportsRequestIndex(t *testing.T) {
	msg := []*cltypes.SignedAggregateAndProof{
		{
			Message: &cltypes.AggregateAndProof{
				Aggregate: &solid.Attestation{
					AggregationBits: solid.BitlistFromBytes([]byte{1, 2}, 2048),
					Data:            &solid.AttestationData{},
					Signature:       common.Bytes96{3, 45, 6},
				},
			},
			Signature: common.Bytes96{2},
		},
		nil,
	}
	_, _, _, _, _, handler, _, syncedDataMgr, _, _ := setupTestingHandler(t, clparams.Phase0Version, log.Root(), false)
	mockBeaconState := &state.CachingBeaconState{BeaconState: raw.New(&clparams.BeaconChainConfig{})}
	mockBeaconState.SetVersion(clparams.DenebVersion)
	syncedDataMgr.(*sync_mock_services.MockSyncedData).EXPECT().ViewHeadState(gomock.Any()).DoAndReturn(func(vhsf synced_data.ViewHeadStateFn) error {
		return vhsf(mockBeaconState)
	}).AnyTimes()
	server := httptest.NewServer(handler.mux)
	defer server.Close()
	requestBody, err := json.Marshal(msg)
	require.NoError(t, err)

	postReq, err := http.NewRequestWithContext(t.Context(), "POST", server.URL+"/eth/v1/validator/aggregate_and_proofs", bytes.NewBuffer(requestBody))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(postReq)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 400, resp.StatusCode)
	var response poolingError
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&response))
	require.Equal(t, []poolingFailure{{Index: 1, Message: "invalid aggregate and proof"}}, response.Failures)
}

func TestPoolSyncCommittees(t *testing.T) {
	msgs := []*cltypes.SyncCommitteeMessage{
		{
			Slot:            1,
			BeaconBlockRoot: common.Hash{1, 2, 3, 4, 5, 6, 7, 8},
			ValidatorIndex:  3,
		},
	}
	_, _, _, s, _, handler, _, sd, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	require.NoError(t, sd.OnHeadState(s))
	server := httptest.NewServer(handler.mux)
	defer server.Close()
	// json
	req, err := json.Marshal(msgs)
	require.NoError(t, err)
	// post attester slashing
	postReq, err := http.NewRequestWithContext(t.Context(), "POST", server.URL+"/eth/v1/beacon/pool/sync_committees", bytes.NewBuffer(req))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(postReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, 200, resp.StatusCode)

	getReq, err := http.NewRequestWithContext(t.Context(), "GET", server.URL+"/eth/v1/validator/sync_committee_contribution?slot=1&subcommittee_index=0&beacon_block_root=0x0102030405060708000000000000000000000000000000000000000000000000", nil)
	require.NoError(t, err)
	resp, err = server.Client().Do(getReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, 200, resp.StatusCode)
	out := struct {
		Data *cltypes.Contribution `json:"data"`
	}{}

	err = json.NewDecoder(resp.Body).Decode(&out)
	require.NoError(t, err)

	require.Equal(t, &cltypes.Contribution{
		Slot:              1,
		BeaconBlockRoot:   common.Hash{1, 2, 3, 4, 5, 6, 7, 8},
		SubcommitteeIndex: 0,
		AggregationBits:   make([]byte, cltypes.DefaultSyncCommitteeAggregationBitsSize),
	}, out.Data)
}

func TestPoolSyncCommitteesIsUnavailableWhileSyncing(t *testing.T) {
	msgs := []*cltypes.SyncCommitteeMessage{
		{
			Slot:            1,
			BeaconBlockRoot: common.Hash{1, 2, 3, 4, 5, 6, 7, 8},
			ValidatorIndex:  3,
		},
	}
	_, _, _, _, _, handler, _, syncedDataMgr, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), false)
	syncedDataMgr.(*sync_mock_services.MockSyncedData).EXPECT().ViewHeadState(gomock.Any()).Return(synced_data.ErrNotSynced)

	body, err := json.Marshal(msgs)
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/sync_committees", bytes.NewReader(body))
	handler.mux.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
}

func TestPoolSyncContributionAndProofs(t *testing.T) {
	aggrBits := make([]byte, cltypes.DefaultSyncCommitteeAggregationBitsSize)
	aggrBits[0] = 1
	msgs := []*cltypes.SignedContributionAndProof{
		{
			Message: &cltypes.ContributionAndProof{
				Contribution: &cltypes.Contribution{
					Slot:            1,
					BeaconBlockRoot: common.Hash{1, 2, 3, 4, 5, 6, 7, 8},
					AggregationBits: aggrBits,
				},
			},
		},
	}
	_, _, _, s, _, handler, _, sd, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)

	require.NoError(t, sd.OnHeadState(s))
	server := httptest.NewServer(handler.mux)
	defer server.Close()
	// json
	req, err := json.Marshal(msgs)
	require.NoError(t, err)
	// post attester slashing
	postReq, err := http.NewRequestWithContext(t.Context(), "POST", server.URL+"/eth/v1/validator/contribution_and_proofs", bytes.NewBuffer(req))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(postReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, 200, resp.StatusCode)

	getReq, err := http.NewRequestWithContext(t.Context(), "GET", server.URL+"/eth/v1/validator/sync_committee_contribution?slot=1&subcommittee_index=0&beacon_block_root=0x0102030405060708000000000000000000000000000000000000000000000000", nil)
	require.NoError(t, err)
	resp, err = server.Client().Do(getReq)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, 200, resp.StatusCode)
	out := struct {
		Data *cltypes.Contribution `json:"data"`
	}{}

	err = json.NewDecoder(resp.Body).Decode(&out)
	require.NoError(t, err)

	require.Equal(t, &cltypes.Contribution{
		Slot:              1,
		BeaconBlockRoot:   common.Hash{1, 2, 3, 4, 5, 6, 7, 8},
		SubcommitteeIndex: 0,
		AggregationBits:   aggrBits,
	}, out.Data)
}
