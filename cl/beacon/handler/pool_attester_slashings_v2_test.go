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

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	mock_services "github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
)

func TestPostPoolAttesterSlashingsV2Version(t *testing.T) {
	for _, tc := range []struct {
		name        string
		header      string
		electraNode bool
		count       int
		status      int
		limit       int
	}{
		{name: "missing", count: 1, status: http.StatusBadRequest},
		{name: "unsupported", header: "unknown", count: 1, status: http.StatusBadRequest},
		{name: "phase0", header: "phase0", count: 2, status: http.StatusOK, limit: 2},
		{name: "altair", header: "altair", count: 2, status: http.StatusOK, limit: 2},
		{name: "bellatrix", header: "bellatrix", count: 2, status: http.StatusOK, limit: 2},
		{name: "capella", header: "capella", count: 2, status: http.StatusOK, limit: 2},
		{name: "deneb on Electra node", header: "deneb", electraNode: true, count: 2, status: http.StatusOK, limit: 2},
		{name: "deneb over limit on Electra node", header: "deneb", electraNode: true, count: 3, status: http.StatusBadRequest},
		{name: "electra on pre-Electra node", header: "electra", count: 4, status: http.StatusOK, limit: 4},
		{name: "electra over limit", header: "electra", count: 5, status: http.StatusBadRequest},
		{name: "fulu", header: "fulu", count: 4, status: http.StatusOK, limit: 4},
		{name: "gloas", header: "gloas", count: 4, status: http.StatusOK, limit: 4},
		{name: "heze", header: "heze", count: 4, status: http.StatusOK, limit: 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler, store := newV2AttesterSlashingHandler(t, tc.electraNode)
			slashing := cltypes.NewAttesterSlashing(clparams.ElectraVersion)
			for i := range tc.count {
				slashing.Attestation_1.AttestingIndices.Append(uint64(i))
				slashing.Attestation_2.AttestingIndices.Append(uint64(i))
			}
			body, err := json.Marshal(slashing)
			require.NoError(t, err)
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/pool/attester_slashings", bytes.NewReader(body))
			if tc.header != "" {
				request.Header.Set("Eth-Consensus-Version", tc.header)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			require.Equal(t, tc.status, response.Code, response.Body.String())
			stored := store.Pool.AttesterSlashingsPool.Raw()
			if tc.status == http.StatusOK {
				require.Len(t, stored, 1)
				require.Equal(t, tc.limit, stored[0].Attestation_1.AttestingIndices.Cap())
				require.Equal(t, tc.limit, stored[0].Attestation_2.AttestingIndices.Cap())
				version, err := clparams.StringToClVersion(tc.header)
				require.NoError(t, err)
				slashing.SetVersion(version)
				slashing.Attestation_1.AttestingIndices.SetCap(tc.limit)
				slashing.Attestation_2.AttestingIndices.SetCap(tc.limit)
				expectedRoot, err := slashing.HashSSZ()
				require.NoError(t, err)
				actualRoot, err := stored[0].HashSSZ()
				require.NoError(t, err)
				require.Equal(t, expectedRoot, actualRoot)
			} else {
				require.Empty(t, stored)
				if tc.header == "" || tc.header == "unknown" {
					require.Contains(t, response.Body.String(), "Eth-Consensus-Version")
				}
			}
		})
	}
}

func newV2AttesterSlashingHandler(t *testing.T, electraNode bool) (*ApiHandler, *mock_services.ForkChoiceStorageMock) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxValidatorsPerCommittee = 2
	cfg.MaxCommitteesPerSlot = 2
	epoch := uint64(0)
	if electraNode {
		epoch = cfg.ElectraForkEpoch
	}
	clock := eth_clock.NewMockEthereumClock(gomock.NewController(t))
	clock.EXPECT().GetCurrentEpoch().Return(epoch).AnyTimes()
	store := mock_services.NewForkChoiceStorageMock(t)
	store.Pool = pool.NewOperationsPool(&cfg)
	handler := &ApiHandler{
		beaconChainCfg:  &cfg,
		ethClock:        clock,
		forkchoiceStore: store,
		routerCfg:       &beacon_router_configuration.RouterConfiguration{Beacon: true},
	}
	handler.Init()
	return handler, store
}

func TestPostPoolAttesterSlashingsV2BodyLimit(t *testing.T) {
	for _, tc := range []struct {
		name        string
		header      string
		electraNode bool
		size        int
		status      int
	}{
		{name: "Deneb at limit on Electra node", header: "deneb", electraNode: true, size: 4096 + 64*2, status: http.StatusOK},
		{name: "Deneb over limit on Electra node", header: "deneb", electraNode: true, size: 4096 + 64*2 + 1, status: http.StatusBadRequest},
		{name: "Electra at limit on pre-Electra node", header: "electra", size: 4096 + 64*4, status: http.StatusOK},
		{name: "Electra over limit on pre-Electra node", header: "electra", size: 4096 + 64*4 + 1, status: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler, store := newV2AttesterSlashingHandler(t, tc.electraNode)
			slashing := cltypes.NewAttesterSlashing(clparams.ElectraVersion)
			slashing.Attestation_1.AttestingIndices.Append(1)
			slashing.Attestation_2.AttestingIndices.Append(1)
			body, err := json.Marshal(slashing)
			require.NoError(t, err)
			body = append(body, bytes.Repeat([]byte(" "), tc.size-len(body))...)
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/pool/attester_slashings", bytes.NewReader(body))
			request.Header.Set("Eth-Consensus-Version", tc.header)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			require.Equal(t, tc.status, response.Code, response.Body.String())
			if tc.status == http.StatusOK {
				require.Len(t, store.Pool.AttesterSlashingsPool.Raw(), 1)
			} else {
				require.Empty(t, store.Pool.AttesterSlashingsPool.Raw())
			}
		})
	}
}

func TestPostPoolAttesterSlashingsV1WithoutVersion(t *testing.T) {
	handler, store := newV2AttesterSlashingHandler(t, false)
	body, err := json.Marshal(cltypes.NewAttesterSlashing(clparams.Phase0Version))
	require.NoError(t, err)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v1/beacon/pool/attester_slashings", bytes.NewReader(body))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Len(t, store.Pool.AttesterSlashingsPool.Raw(), 1)
}

func TestPostPoolAttesterSlashingsV2InvalidAttestations(t *testing.T) {
	for _, body := range []string{
		`{`,
		`{}`,
		`{"attestation_1":{},"attestation_2":null}`,
		`{"attestation_1":null,"attestation_2":{}}`,
		`{"attestation_1":{"attesting_indices":["1","2","3"]},"attestation_2":{}}`,
		`{"attestation_1":{},"attestation_2":{"attesting_indices":["1","2","3"]}}`,
		`{"attestation_1":null,"attestation_1":{"attesting_indices":["1","2","3"]},"attestation_2":{}}`,
		`{"attestation_1":{},"attestation_2":null,"attestation_2":{"attesting_indices":["1","2","3"]}}`,
	} {
		t.Run(body, func(t *testing.T) {
			handler, store := newV2AttesterSlashingHandler(t, true)
			request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/eth/v2/beacon/pool/attester_slashings", bytes.NewBufferString(body))
			request.Header.Set("Eth-Consensus-Version", "deneb")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
			require.Empty(t, store.Pool.AttesterSlashingsPool.Raw())
		})
	}
}
