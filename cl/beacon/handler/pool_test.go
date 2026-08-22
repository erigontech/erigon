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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	sync_mock_services "github.com/erigontech/erigon/cl/beacon/synced_data/mock_services"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/raw"
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
		vhsf(mockBeaconState)
		return nil
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
		vhsf(mockBeaconState)
		return nil
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
		vhsf(mockBeaconState)
		return nil
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
		vhsf(mockBeaconState)
		return nil
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
		vhsf(mockBeaconState)
		return nil
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
		vhsf(mockBeaconState)
		return nil
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

	sd.OnHeadState(s)
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

	sd.OnHeadState(s)
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
