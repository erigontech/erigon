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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	sync_mock_services "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	clgossip "github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/raw"
	"github.com/erigontech/erigon/cl/phase1/network/gossip"
	gossip_mock "github.com/erigontech/erigon/cl/phase1/network/gossip/mock_services"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	services_mock "github.com/erigontech/erigon/cl/phase1/network/services/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
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

func TestPoolV1AttestationsPublishesOnMessageEpochForkDigest(t *testing.T) {
	ctrl := gomock.NewController(t)
	service := services_mock.NewMockAttestationService(ctrl)
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	syncedData := sync_mock_services.NewMockSyncedData(ctrl)
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 2

	messageEpoch := cfg.ElectraForkEpoch - 1
	messageSlot := cfg.ElectraForkEpoch*cfg.SlotsPerEpoch - 1
	messageClock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	messageDigest, err := messageClock.ComputeForkDigest(messageEpoch)
	require.NoError(t, err)
	clock.EXPECT().ComputeForkDigest(messageEpoch).Return(messageDigest, nil).Times(1)
	syncedData.EXPECT().Syncing().Return(false).Times(1)
	syncedData.EXPECT().CommitteeCount(messageEpoch).Return(uint64(1)).Times(1)

	requestBody, err := json.Marshal([]*solid.Attestation{{
		AggregationBits: solid.BitlistFromBytes([]byte{1}, int(cfg.MaxValidatorsPerCommittee)),
		Data: &solid.AttestationData{
			Slot:   messageSlot,
			Target: solid.Checkpoint{Epoch: messageEpoch},
		},
	}})
	require.NoError(t, err)

	service.EXPECT().ProcessMessage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, subnetID *uint64, msg *services.AttestationForGossip) error {
			require.NotNil(t, msg.TopicForkDigest)
			require.Equal(t, messageDigest, *msg.TopicForkDigest)
			return nil
		},
	).Times(1)
	gossipManager.EXPECT().PublishToForkDigest(
		gomock.Any(),
		messageDigest,
		gomock.Any(),
		gomock.Any(),
	).Return(nil).Times(1)
	handler := &ApiHandler{
		logger:             log.Root(),
		netConfig:          &clparams.NetworkConfig{AttestationSubnetCount: 64},
		ethClock:           clock,
		beaconChainCfg:     &cfg,
		syncedData:         syncedData,
		attestationService: service,
		gossipManager:      gossipManager,
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/attestations", bytes.NewReader(requestBody))

	handler.PostEthV1BeaconPoolAttestations(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code)
}

func TestPoolV2AttestationsPublishesOnMessageEpochForkDigest(t *testing.T) {
	ctrl := gomock.NewController(t)
	service := services_mock.NewMockAttestationService(ctrl)
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	syncedData := sync_mock_services.NewMockSyncedData(ctrl)
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 2

	messageEpoch := cfg.GloasForkEpoch - 1
	messageSlot := cfg.GloasForkEpoch*cfg.SlotsPerEpoch - 1
	messageClock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	messageDigest, err := messageClock.ComputeForkDigest(messageEpoch)
	require.NoError(t, err)
	clock.EXPECT().ComputeForkDigest(messageEpoch).Return(messageDigest, nil).Times(1)
	syncedData.EXPECT().Syncing().Return(false).Times(1)
	syncedData.EXPECT().CommitteeCount(messageEpoch).Return(uint64(1)).Times(1)

	requestBody, err := json.Marshal([]*solid.SingleAttestation{{
		Data: &solid.AttestationData{
			Slot:   messageSlot,
			Target: solid.Checkpoint{Epoch: messageEpoch},
		},
	}})
	require.NoError(t, err)

	service.EXPECT().ProcessMessage(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, subnetID *uint64, msg *services.AttestationForGossip) error {
			require.NotNil(t, msg.TopicForkDigest)
			require.Equal(t, messageDigest, *msg.TopicForkDigest)
			return nil
		},
	).Times(1)
	publishErr := errors.New("publish failed")
	gossipManager.EXPECT().PublishToForkDigest(
		gomock.Any(),
		messageDigest,
		gomock.Any(),
		gomock.Any(),
	).Return(publishErr).Times(1)
	handler := &ApiHandler{
		logger:             log.Root(),
		netConfig:          &clparams.NetworkConfig{AttestationSubnetCount: 64},
		ethClock:           clock,
		beaconChainCfg:     &cfg,
		syncedData:         syncedData,
		attestationService: service,
		gossipManager:      gossipManager,
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/pool/attestations", bytes.NewReader(requestBody))
	request.Header.Set("Eth-Consensus-Version", clparams.FuluVersion.String())

	handler.PostEthV2BeaconPoolAttestations(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	var response poolingError
	require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
	require.Equal(t, []poolingFailure{{Index: 0, Message: publishErr.Error()}}, response.Failures)
}

func TestPoolAggregatesAndProofsDoesNotPublishIgnoredAggregate(t *testing.T) {
	ctrl := gomock.NewController(t)
	service := services_mock.NewMockAggregateAndProofService(ctrl)
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	topicDigest, err := clock.ComputeForkDigest(0)
	require.NoError(t, err)

	committeeBits := solid.NewBitVector(int(cfg.MaxCommitteesPerSlot))
	require.NoError(t, committeeBits.SetBitAt(0, true))
	requestBody, err := json.Marshal([]*cltypes.SignedAggregateAndProof{{
		Message: &cltypes.AggregateAndProof{
			Aggregate: &solid.Attestation{
				AggregationBits: solid.BitlistFromBytes([]byte{1}, int(cfg.MaxValidatorsPerCommittee*cfg.MaxCommitteesPerSlot)),
				Data:            &solid.AttestationData{},
				CommitteeBits:   committeeBits,
			},
		},
	}})
	require.NoError(t, err)

	service.EXPECT().ProcessMessage(gomock.Any(), nil, gomock.Any()).DoAndReturn(
		func(ctx context.Context, subnetID *uint64, msg *services.SignedAggregateAndProofForGossip) error {
			require.NotNil(t, msg.TopicForkDigest)
			require.Equal(t, topicDigest, *msg.TopicForkDigest)
			return services.ErrIgnore
		},
	).Times(1)
	handler := &ApiHandler{
		logger:                    log.Root(),
		ethClock:                  clock,
		beaconChainCfg:            &cfg,
		aggregateAndProofsService: service,
		gossipManager:             gossipManager,
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/aggregate_and_proofs", bytes.NewReader(requestBody))

	handler.PostEthV1ValidatorAggregatesAndProof(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code)
}

func TestPoolAggregatesAndProofsPublishesOnMessageEpochForkDigest(t *testing.T) {
	ctrl := gomock.NewController(t)
	service := services_mock.NewMockAggregateAndProofService(ctrl)
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	clock := eth_clock.NewMockEthereumClock(ctrl)
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 2

	messageEpoch := cfg.GloasForkEpoch - 1
	messageSlot := cfg.GloasForkEpoch*cfg.SlotsPerEpoch - 1
	messageClock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	messageDigest, err := messageClock.ComputeForkDigest(messageEpoch)
	require.NoError(t, err)
	clock.EXPECT().ComputeForkDigest(messageEpoch).Return(messageDigest, nil).Times(1)
	clock.EXPECT().GetCurrentEpoch().Return(cfg.GloasForkEpoch).AnyTimes()

	committeeBits := solid.NewBitVector(int(cfg.MaxCommitteesPerSlot))
	require.NoError(t, committeeBits.SetBitAt(0, true))
	requestBody, err := json.Marshal([]*cltypes.SignedAggregateAndProof{{
		Message: &cltypes.AggregateAndProof{
			Aggregate: &solid.Attestation{
				AggregationBits: solid.BitlistFromBytes([]byte{1}, int(cfg.MaxValidatorsPerCommittee*cfg.MaxCommitteesPerSlot)),
				Data: &solid.AttestationData{
					Slot:   messageSlot,
					Target: solid.Checkpoint{Epoch: messageEpoch},
				},
				CommitteeBits: committeeBits,
			},
		},
	}})
	require.NoError(t, err)

	service.EXPECT().ProcessMessage(gomock.Any(), nil, gomock.Any()).DoAndReturn(
		func(ctx context.Context, subnetID *uint64, msg *services.SignedAggregateAndProofForGossip) error {
			require.NotNil(t, msg.TopicForkDigest)
			require.Equal(t, messageDigest, *msg.TopicForkDigest)
			return nil
		},
	).Times(1)
	gossipManager.EXPECT().PublishToForkDigest(
		gomock.Any(),
		messageDigest,
		clgossip.TopicNameBeaconAggregateAndProof,
		gomock.Any(),
	).Return(nil).Times(1)
	handler := &ApiHandler{
		logger:                    log.Root(),
		ethClock:                  clock,
		beaconChainCfg:            &cfg,
		aggregateAndProofsService: service,
		gossipManager:             gossipManager,
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/aggregate_and_proofs", bytes.NewReader(requestBody))

	handler.PostEthV1ValidatorAggregatesAndProof(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code)
}

func TestPoolAggregatesAndProofsRetriesAfterPublishFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	service := services_mock.NewMockAggregateAndProofService(ctrl)
	gossipManager := gossip_mock.NewMockGossip(ctrl)
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	clock := eth_clock.NewEthereumClock(0, common.Hash{}, &cfg)
	topicDigest, err := clock.ComputeForkDigest(0)
	require.NoError(t, err)

	committeeBits := solid.NewBitVector(int(cfg.MaxCommitteesPerSlot))
	require.NoError(t, committeeBits.SetBitAt(0, true))
	requestBody, err := json.Marshal([]*cltypes.SignedAggregateAndProof{{
		Message: &cltypes.AggregateAndProof{
			Aggregate: &solid.Attestation{
				AggregationBits: solid.BitlistFromBytes([]byte{1}, int(cfg.MaxValidatorsPerCommittee*cfg.MaxCommitteesPerSlot)),
				Data:            &solid.AttestationData{},
				CommitteeBits:   committeeBits,
			},
		},
	}})
	require.NoError(t, err)

	publishErr := errors.New("publish failed")
	gomock.InOrder(
		service.EXPECT().ProcessMessage(gomock.Any(), nil, gomock.Any()).Return(nil),
		gossipManager.EXPECT().PublishToForkDigest(
			gomock.Any(),
			topicDigest,
			clgossip.TopicNameBeaconAggregateAndProof,
			gomock.Any(),
		).Return(publishErr),
		service.EXPECT().ProcessMessage(gomock.Any(), nil, gomock.Any()).Return(
			fmt.Errorf("%w: %w", services.ErrIgnore, services.ErrAggregatorAlreadySeen),
		),
		gossipManager.EXPECT().PublishToForkDigest(
			gomock.Any(),
			topicDigest,
			clgossip.TopicNameBeaconAggregateAndProof,
			gomock.Any(),
		).Return(nil),
	)
	handler := &ApiHandler{
		logger:                    log.Root(),
		ethClock:                  clock,
		beaconChainCfg:            &cfg,
		aggregateAndProofsService: service,
		gossipManager:             gossipManager,
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/aggregate_and_proofs", bytes.NewReader(requestBody))

	handler.PostEthV1ValidatorAggregatesAndProof(recorder, request)

	require.Equal(t, http.StatusBadRequest, recorder.Code)
	var response poolingError
	require.NoError(t, json.NewDecoder(recorder.Body).Decode(&response))
	require.Equal(t, []poolingFailure{{Index: 0, Message: publishErr.Error()}}, response.Failures)

	recorder = httptest.NewRecorder()
	request = httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/validator/aggregate_and_proofs", bytes.NewReader(requestBody))

	handler.PostEthV1ValidatorAggregatesAndProof(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code)
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

// TestPoolSyncCommitteesPublishesInBackground proves the handler queues the
// gossip publish rather than awaiting it inline: it must call
// PublishBackground, never the blocking Publish, and must not depend on the
// publish completing before writing the HTTP response.
func TestPoolSyncCommitteesPublishesInBackground(t *testing.T) {
	msgs := []*cltypes.SyncCommitteeMessage{
		{
			Slot:            1,
			BeaconBlockRoot: common.Hash{1, 2, 3, 4, 5, 6, 7, 8},
			ValidatorIndex:  3,
		},
	}
	_, _, _, s, _, handler, _, sd, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	require.NoError(t, sd.OnHeadState(s))

	ctrl := gomock.NewController(t)
	mockGossip := gossip_mock.NewMockGossip(ctrl)
	published := make(chan struct{}, 1)
	mockGossip.EXPECT().PublishBackground(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(name string, data []byte, expiry time.Time, logCtx ...any) error {
			select {
			case published <- struct{}{}:
			default:
			}
			return nil
		},
	).MinTimes(1)
	handler.gossipManager = mockGossip

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	body, err := json.Marshal(msgs)
	require.NoError(t, err)
	postReq, err := http.NewRequestWithContext(t.Context(), "POST", server.URL+"/eth/v1/beacon/pool/sync_committees", bytes.NewBuffer(body))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")

	resp, err := server.Client().Do(postReq)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	select {
	case <-published:
	case <-time.After(2 * time.Second):
		t.Fatal("expected PostEthV1BeaconPoolSyncCommittees to call PublishBackground")
	}
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

// TestPoolSyncCommitteesReturns500OnAdmissionFailure proves a known
// admission failure (queue full, in this case) is surfaced as a 500 for an
// otherwise-valid batch, rather than hidden behind a 200 the way a
// fire-and-forget PublishBackground would.
func TestPoolSyncCommitteesReturns500OnAdmissionFailure(t *testing.T) {
	msgs := []*cltypes.SyncCommitteeMessage{
		{
			Slot:            1,
			BeaconBlockRoot: common.Hash{1, 2, 3, 4, 5, 6, 7, 8},
			ValidatorIndex:  3,
		},
	}
	_, _, _, s, _, handler, _, sd, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	require.NoError(t, sd.OnHeadState(s))

	ctrl := gomock.NewController(t)
	mockGossip := gossip_mock.NewMockGossip(ctrl)
	mockGossip.EXPECT().PublishBackground(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(gossip.ErrPublishQueueFull).AnyTimes()
	handler.gossipManager = mockGossip

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	body, err := json.Marshal(msgs)
	require.NoError(t, err)
	postReq, err := http.NewRequestWithContext(t.Context(), "POST", server.URL+"/eth/v1/beacon/pool/sync_committees", bytes.NewBuffer(body))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")

	resp, err := server.Client().Do(postReq)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode,
		"a known admission failure must not be hidden behind a 200")
}

// TestPoolSyncCommitteesLogsAdmissionFailuresOncePerRequest proves a batch
// with several admission failures produces one aggregate Warn log with a
// count, not one Warn per failed message - which under queue congestion
// (up to a full sync-committee burst) could otherwise be hundreds of lines
// for a single request.
func TestPoolSyncCommitteesLogsAdmissionFailuresOncePerRequest(t *testing.T) {
	msgs := []*cltypes.SyncCommitteeMessage{
		{Slot: 1, BeaconBlockRoot: common.Hash{1, 2, 3, 4, 5, 6, 7, 8}, ValidatorIndex: 3},
		{Slot: 1, BeaconBlockRoot: common.Hash{1, 2, 3, 4, 5, 6, 7, 8}, ValidatorIndex: 3},
	}
	_, _, _, s, _, handler, _, sd, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	require.NoError(t, sd.OnHeadState(s))

	ctrl := gomock.NewController(t)
	mockGossip := gossip_mock.NewMockGossip(ctrl)
	var publishAttempts atomic.Int32
	mockGossip.EXPECT().PublishBackground(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(name string, data []byte, expiry time.Time, logCtx ...any) error {
			publishAttempts.Add(1)
			return gossip.ErrPublishQueueFull
		},
	).AnyTimes()
	handler.gossipManager = mockGossip

	records := make(chan *log.Record, 64)
	prevHandler := log.Root().GetHandler()
	log.Root().SetHandler(log.ChannelHandler(records))
	defer log.Root().SetHandler(prevHandler)

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	body, err := json.Marshal(msgs)
	require.NoError(t, err)
	postReq, err := http.NewRequestWithContext(t.Context(), "POST", server.URL+"/eth/v1/beacon/pool/sync_committees", bytes.NewBuffer(body))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")

	resp, err := server.Client().Do(postReq)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	var summaries []*log.Record
	for {
		select {
		case r := <-records:
			if r.Msg == "[Beacon REST] sync-committee publish admission failed" {
				summaries = append(summaries, r)
			}
			continue
		default:
		}
		break
	}
	require.Len(t, summaries, 1, "must log the admission-failure summary exactly once per request, not once per message")
	require.Contains(t, summaries[0].Ctx, "count")
	require.Contains(t, summaries[0].Ctx, int(publishAttempts.Load()))
}

// TestPoolSyncCommitteesDoesNotSurface500ForExpiredAdmission proves an
// already-expired message is not treated as a server-side admission
// failure the way queue-full/shutdown/fork-digest failures are: a message
// whose useful window has already closed is expected, ordinary behavior,
// not a fault worth a 500 - covering the residual case where a message
// passed ProcessMessage (so it wasn't ErrIgnore'd) but its own deadline
// passed by the time PublishBackground was reached.
func TestPoolSyncCommitteesDoesNotSurface500ForExpiredAdmission(t *testing.T) {
	msgs := []*cltypes.SyncCommitteeMessage{
		{
			Slot:            1,
			BeaconBlockRoot: common.Hash{1, 2, 3, 4, 5, 6, 7, 8},
			ValidatorIndex:  3,
		},
	}
	_, _, _, s, _, handler, _, sd, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	require.NoError(t, sd.OnHeadState(s))

	ctrl := gomock.NewController(t)
	mockGossip := gossip_mock.NewMockGossip(ctrl)
	mockGossip.EXPECT().PublishBackground(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(gossip.ErrPublishJobExpired).AnyTimes()
	handler.gossipManager = mockGossip

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	body, err := json.Marshal(msgs)
	require.NoError(t, err)
	postReq, err := http.NewRequestWithContext(t.Context(), "POST", server.URL+"/eth/v1/beacon/pool/sync_committees", bytes.NewBuffer(body))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")

	resp, err := server.Client().Do(postReq)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode,
		"an already-expired message must not be surfaced as a 500 - it's ordinary, not a fault")
}

// TestPoolSyncCommitteesSkipsPublishForIgnoredMessage proves an
// ErrIgnore'd message never reaches PublishBackground at all - not merely
// that its eventual admission outcome is excluded from the 500 path. This
// matters because PublishBackground can fail for reasons unrelated to the
// message's own staleness (queue full, shutdown, fork-digest resolution);
// those must not surface as a 500 either when ProcessMessage already said
// there is nothing to do for this message.
func TestPoolSyncCommitteesSkipsPublishForIgnoredMessage(t *testing.T) {
	msgs := []*cltypes.SyncCommitteeMessage{
		{
			Slot:            1,
			BeaconBlockRoot: common.Hash{1, 2, 3, 4, 5, 6, 7, 8},
			ValidatorIndex:  3,
		},
	}
	_, _, _, s, _, handler, _, sd, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	require.NoError(t, sd.OnHeadState(s))

	ctrl := gomock.NewController(t)
	mockSyncCommittee := services_mock.NewMockSyncCommitteeMessagesService(ctrl)
	mockSyncCommittee.EXPECT().ProcessMessage(gomock.Any(), gomock.Any(), gomock.Any()).Return(services.ErrIgnore).AnyTimes()
	handler.syncCommitteeMessagesService = mockSyncCommittee

	mockGossip := gossip_mock.NewMockGossip(ctrl)
	mockGossip.EXPECT().PublishBackground(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	handler.gossipManager = mockGossip

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	body, err := json.Marshal(msgs)
	require.NoError(t, err)
	postReq, err := http.NewRequestWithContext(t.Context(), "POST", server.URL+"/eth/v1/beacon/pool/sync_committees", bytes.NewBuffer(body))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")

	resp, err := server.Client().Do(postReq)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
}

// TestPoolSyncCommitteesSkipsPublishForRealServiceIgnoredSlot proves the
// ErrIgnore-skip against the real syncCommitteeMessagesService, not just a
// mock told to return the sentinel: a message for slot 1 is catastrophically
// stale relative to the real ethClock's wall-clock-derived current slot, so
// IsSlotCurrentSlotWithMaximumClockDisparity genuinely rejects it and
// ProcessMessage genuinely returns ErrIgnore, independent of the handler's
// own control flow.
func TestPoolSyncCommitteesSkipsPublishForRealServiceIgnoredSlot(t *testing.T) {
	msgs := []*cltypes.SyncCommitteeMessage{
		{
			Slot:            1,
			BeaconBlockRoot: common.Hash{1, 2, 3, 4, 5, 6, 7, 8},
			ValidatorIndex:  3,
		},
	}
	_, _, _, s, _, handler, _, sd, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	require.NoError(t, sd.OnHeadState(s))

	realSyncedData, ok := sd.(*synced_data.SyncedDataManager)
	require.True(t, ok, "test requires the real SyncedDataManager, not a mock, to exercise the real service")
	handler.syncCommitteeMessagesService = services.NewSyncCommitteeMessagesService(
		handler.beaconChainCfg, handler.ethClock, realSyncedData, nil, nil, false)

	ctrl := gomock.NewController(t)
	mockGossip := gossip_mock.NewMockGossip(ctrl)
	mockGossip.EXPECT().PublishBackground(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	handler.gossipManager = mockGossip

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	body, err := json.Marshal(msgs)
	require.NoError(t, err)
	postReq, err := http.NewRequestWithContext(t.Context(), "POST", server.URL+"/eth/v1/beacon/pool/sync_committees", bytes.NewBuffer(body))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")

	resp, err := server.Client().Do(postReq)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
}

// TestPoolSyncCommitteesValidationFailurePrecedesAdmissionFailure proves the
// response precedence when a batch has both a validation failure (an
// out-of-range validator index, which pool.go already reports as an
// indexed 400 via the existing ComputeSubnetsForSyncCommittee error path)
// and, independently, an admission failure for the other, valid message:
// the 400 takes precedence, since it carries more actionable detail, but
// does not silently discard the admission failure - PublishBackground's own
// logging/counting for it still happens regardless of which response is
// written.
func TestPoolSyncCommitteesValidationFailurePrecedesAdmissionFailure(t *testing.T) {
	msgs := []*cltypes.SyncCommitteeMessage{
		{
			Slot:            1,
			BeaconBlockRoot: common.Hash{1, 2, 3, 4, 5, 6, 7, 8},
			ValidatorIndex:  3,
		},
		{
			Slot:            1,
			BeaconBlockRoot: common.Hash{1, 2, 3, 4, 5, 6, 7, 8},
			ValidatorIndex:  999_999_999, // out of range: fails ComputeSubnetsForSyncCommittee
		},
	}
	_, _, _, s, _, handler, _, sd, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	require.NoError(t, sd.OnHeadState(s))

	ctrl := gomock.NewController(t)
	mockGossip := gossip_mock.NewMockGossip(ctrl)
	mockGossip.EXPECT().PublishBackground(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(gossip.ErrPublishQueueFull).AnyTimes()
	handler.gossipManager = mockGossip

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	body, err := json.Marshal(msgs)
	require.NoError(t, err)
	postReq, err := http.NewRequestWithContext(t.Context(), "POST", server.URL+"/eth/v1/beacon/pool/sync_committees", bytes.NewBuffer(body))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")

	resp, err := server.Client().Do(postReq)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode,
		"a validation failure must take precedence over an admission failure in the response")
}

// TestSyncCommitteeMessageExpiry pins the formula directly: slot end (the
// start of the next slot) plus the network config's maximum gossip clock
// disparity. Every existing handler test that exercises PublishBackground
// matches the expiry argument with gomock.Any(), so an off-by-one on
// slot+1 or a wrong disparity conversion would not be caught anywhere else.
func TestSyncCommitteeMessageExpiry(t *testing.T) {
	bcfg := clparams.MainnetBeaconConfig
	bcfg.InitializeForkSchedule()
	genesis, err := initial_state.GetGenesisState(t.Context(), chainspec.MainnetChainID)
	require.NoError(t, err)
	ethClock := eth_clock.NewEthereumClock(genesis.GenesisTime(), genesis.GenesisValidatorsRoot(), &bcfg)
	netCfg := &clparams.NetworkConfig{MaximumGossipClockDisparity: clparams.ConfigDurationMSec(500 * time.Millisecond)}

	const slot = 12345
	got := syncCommitteeMessageExpiry(ethClock, netCfg, slot)
	want := ethClock.GetSlotTime(slot + 1).Add(500 * time.Millisecond)
	require.Equal(t, want, got)
	require.NotEqual(t, ethClock.GetSlotTime(slot).Add(500*time.Millisecond), got,
		"sanity: must be keyed off slot+1 (slot end), not slot (slot start)")
}

// TestSyncCommitteeMessageExpiryDoesNotWrapAtMaxSlot proves an
// out-of-range slot is treated as already expired outright, rather than
// handed to GetSlotTime where slot+1 (or GetSlotTime's own internal
// multiplication) can silently overflow uint64 and alias to an arbitrary
// timestamp - including one that is not obviously in the past, which a
// weaker "not equal to genesis" assertion would not rule out.
func TestSyncCommitteeMessageExpiryDoesNotWrapAtMaxSlot(t *testing.T) {
	bcfg := clparams.MainnetBeaconConfig
	bcfg.InitializeForkSchedule()
	genesis, err := initial_state.GetGenesisState(t.Context(), chainspec.MainnetChainID)
	require.NoError(t, err)
	ethClock := eth_clock.NewEthereumClock(genesis.GenesisTime(), genesis.GenesisValidatorsRoot(), &bcfg)
	netCfg := &clparams.NetworkConfig{MaximumGossipClockDisparity: clparams.ConfigDurationMSec(500 * time.Millisecond)}

	got := syncCommitteeMessageExpiry(ethClock, netCfg, math.MaxUint64)
	require.True(t, got.Before(time.Now()), "an out-of-range slot must produce an expiry that is actually in the past")
}

// TestSyncCommitteeMessageExpiryDoesNotAliasToFutureForHugeSlot proves the
// out-of-range guard catches slots the naive slot+1-then-GetSlotTime
// arithmetic would otherwise alias to a plausible, not-obviously-bogus
// future timestamp (2030-01-01 here) rather than something clearly
// invalid - the failure mode a bound targeting only slot == MaxUint64
// would miss.
func TestSyncCommitteeMessageExpiryDoesNotAliasToFutureForHugeSlot(t *testing.T) {
	bcfg := clparams.MainnetBeaconConfig
	bcfg.InitializeForkSchedule()
	genesis, err := initial_state.GetGenesisState(t.Context(), chainspec.MainnetChainID)
	require.NoError(t, err)
	ethClock := eth_clock.NewEthereumClock(genesis.GenesisTime(), genesis.GenesisValidatorsRoot(), &bcfg)
	netCfg := &clparams.NetworkConfig{MaximumGossipClockDisparity: clparams.ConfigDurationMSec(500 * time.Millisecond)}

	const aliasingSlot = 3074457345642144600
	naiveAliased := ethClock.GetSlotTime(aliasingSlot + 1)
	require.True(t, naiveAliased.After(time.Now()),
		"sanity: this slot must actually demonstrate the aliasing failure mode, not merely be large")

	got := syncCommitteeMessageExpiry(ethClock, netCfg, aliasingSlot)
	require.True(t, got.Before(time.Now()),
		"the bound must catch this slot even though the naive formula aliases to a plausible future date")
}

// TestPoolSyncCommitteesUsesCalculatedExpiry proves the handler actually
// wires syncCommitteeMessageExpiry's result into PublishBackground for the
// message's own slot, not just that it calls PublishBackground at all.
func TestPoolSyncCommitteesUsesCalculatedExpiry(t *testing.T) {
	const msgSlot = 1
	msgs := []*cltypes.SyncCommitteeMessage{
		{
			Slot:            msgSlot,
			BeaconBlockRoot: common.Hash{1, 2, 3, 4, 5, 6, 7, 8},
			ValidatorIndex:  3,
		},
	}
	_, _, _, s, _, handler, _, sd, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	require.NoError(t, sd.OnHeadState(s))

	wantExpiry := syncCommitteeMessageExpiry(handler.ethClock, handler.netConfig, msgSlot)

	ctrl := gomock.NewController(t)
	mockGossip := gossip_mock.NewMockGossip(ctrl)
	gotExpiry := make(chan time.Time, 1)
	mockGossip.EXPECT().PublishBackground(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(name string, data []byte, expiry time.Time, logCtx ...any) error {
			select {
			case gotExpiry <- expiry:
			default:
			}
			return nil
		},
	).MinTimes(1)
	handler.gossipManager = mockGossip

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	body, err := json.Marshal(msgs)
	require.NoError(t, err)
	postReq, err := http.NewRequestWithContext(t.Context(), "POST", server.URL+"/eth/v1/beacon/pool/sync_committees", bytes.NewBuffer(body))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(postReq)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	select {
	case got := <-gotExpiry:
		require.Equal(t, wantExpiry, got)
	case <-time.After(2 * time.Second):
		t.Fatal("PublishBackground was never called")
	}
}

// TestPoolSyncCommitteesMarksMessagePublishedOnSuccessfulAdmission proves
// the handler reports a successful PublishBackground admission back to the
// sync-committee service, with the message's own content, so a later
// duplicate submission can be recognized as already published.
func TestPoolSyncCommitteesMarksMessagePublishedOnSuccessfulAdmission(t *testing.T) {
	msg := &cltypes.SyncCommitteeMessage{
		Slot:            1,
		BeaconBlockRoot: common.Hash{1, 2, 3, 4, 5, 6, 7, 8},
		ValidatorIndex:  3,
		Signature:       common.Bytes96{9},
	}
	msgs := []*cltypes.SyncCommitteeMessage{msg}
	_, _, _, s, _, handler, _, sd, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	require.NoError(t, sd.OnHeadState(s))

	ctrl := gomock.NewController(t)
	mockSyncCommittee := services_mock.NewMockSyncCommitteeMessagesService(ctrl)
	mockSyncCommittee.EXPECT().ProcessMessage(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	markPublishedCalled := make(chan struct{}, 1)
	mockSyncCommittee.EXPECT().MarkPublished(gomock.Any(), msg.Slot, msg.ValidatorIndex, msg.BeaconBlockRoot, msg.Signature).DoAndReturn(
		func(subnet, slot, validatorIndex uint64, root common.Hash, signature common.Bytes96) {
			select {
			case markPublishedCalled <- struct{}{}:
			default:
			}
		},
	).AnyTimes()
	handler.syncCommitteeMessagesService = mockSyncCommittee

	mockGossip := gossip_mock.NewMockGossip(ctrl)
	mockGossip.EXPECT().PublishBackground(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	handler.gossipManager = mockGossip

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	body, err := json.Marshal(msgs)
	require.NoError(t, err)
	postReq, err := http.NewRequestWithContext(t.Context(), "POST", server.URL+"/eth/v1/beacon/pool/sync_committees", bytes.NewBuffer(body))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(postReq)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	select {
	case <-markPublishedCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("MarkPublished was never called after a successful admission")
	}
}

// TestPoolSyncCommitteesDoesNotMarkPublishedOnAdmissionFailure proves the
// handler does not report a message as published when PublishBackground
// itself failed, so a retry of the same content still gets a real chance to
// publish instead of being ignored as already-done.
func TestPoolSyncCommitteesDoesNotMarkPublishedOnAdmissionFailure(t *testing.T) {
	msgs := []*cltypes.SyncCommitteeMessage{
		{
			Slot:            1,
			BeaconBlockRoot: common.Hash{1, 2, 3, 4, 5, 6, 7, 8},
			ValidatorIndex:  3,
		},
	}
	_, _, _, s, _, handler, _, sd, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	require.NoError(t, sd.OnHeadState(s))

	ctrl := gomock.NewController(t)
	mockSyncCommittee := services_mock.NewMockSyncCommitteeMessagesService(ctrl)
	mockSyncCommittee.EXPECT().ProcessMessage(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockSyncCommittee.EXPECT().MarkPublished(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	handler.syncCommitteeMessagesService = mockSyncCommittee

	mockGossip := gossip_mock.NewMockGossip(ctrl)
	mockGossip.EXPECT().PublishBackground(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(gossip.ErrPublishQueueFull).AnyTimes()
	handler.gossipManager = mockGossip

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	body, err := json.Marshal(msgs)
	require.NoError(t, err)
	postReq, err := http.NewRequestWithContext(t.Context(), "POST", server.URL+"/eth/v1/beacon/pool/sync_committees", bytes.NewBuffer(body))
	require.NoError(t, err)
	postReq.Header.Set("Content-Type", "application/json")
	resp, err := server.Client().Do(postReq)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
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

// An ignored submission is not published because gossip accepts self-published messages without validating them
// again. An already-seen attestation succeeds without publishing because the seen check runs before signature
// validation, so the submitted attestation itself was not validated.
func TestPoolAttestationsDoNotPublishIgnored(t *testing.T) {
	data, err := json.Marshal(&solid.AttestationData{})
	require.NoError(t, err)
	single, err := json.Marshal([]*solid.SingleAttestation{{Data: &solid.AttestationData{}}})
	require.NoError(t, err)
	requests := []struct {
		name    string
		path    string
		version string
		body    string
	}{
		{
			name: "v1",
			path: "/eth/v1/beacon/pool/attestations",
			body: fmt.Sprintf(`[{"aggregation_bits":"0x01","data":%s,"signature":"0x%s"}]`, data, strings.Repeat("00", 96)),
		},
		{
			name:    "v2",
			path:    "/eth/v2/beacon/pool/attestations",
			version: "electra",
			body:    string(single),
		},
	}
	outcomes := []struct {
		name   string
		err    error
		status int
	}{
		{name: "already seen", err: fmt.Errorf("%w: %w", services.ErrIgnore, services.ErrAttestationAlreadySeen), status: http.StatusOK},
		{name: "stale head", err: fmt.Errorf("head epoch 0 too far from attestation epoch 2: %w", services.ErrIgnore), status: http.StatusBadRequest},
		{name: "invalid signature", err: errors.New("invalid signature"), status: http.StatusBadRequest},
	}
	for _, tt := range requests {
		for _, outcome := range outcomes {
			t.Run(tt.name+" "+outcome.name, func(t *testing.T) {
				_, _, _, s, _, handler, _, sd, _, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
				require.NoError(t, sd.OnHeadState(s))
				netCfg := clparams.NetworkConfigs[chainspec.MainnetChainID]
				handler.netConfig = &netCfg

				ctrl := gomock.NewController(t)
				attestationService := services_mock.NewMockAttestationService(ctrl)
				attestationService.EXPECT().ProcessMessage(gomock.Any(), gomock.Any(), gomock.Any()).Return(outcome.err).Times(1)
				handler.attestationService = attestationService
				mockGossip := gossip_mock.NewMockGossip(ctrl)
				mockGossip.EXPECT().Publish(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
				handler.gossipManager = mockGossip

				req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, tt.path, strings.NewReader(tt.body))
				req.Header.Set("Content-Type", "application/json")
				if tt.version != "" {
					req.Header.Set("Eth-Consensus-Version", tt.version)
				}
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, req)
				require.Equal(t, outcome.status, recorder.Code, recorder.Body.String())
				if outcome.status == http.StatusBadRequest {
					var response poolingError
					require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response))
					require.Equal(t, []poolingFailure{{Index: 0, Message: outcome.err.Error()}}, response.Failures)
				}
			})
		}
	}
}
