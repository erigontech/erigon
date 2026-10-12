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
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

type stateBuildersHTTPResponse struct {
	ExecutionOptimistic bool `json:"execution_optimistic"`
	Finalized           bool `json:"finalized"`
	Data                []struct {
		Index   string `json:"index"`
		Status  string `json:"status"`
		Builder struct {
			Pubkey  common.Bytes48 `json:"pubkey"`
			Version string         `json:"version"`
		} `json:"builder"`
	} `json:"data"`
}

func setupStateBuildersHandler(t *testing.T) (*ApiHandler, *state.CachingBeaconState, [3]*cltypes.Builder) {
	t.Helper()
	_, blocks, _, _, postState, handler, _, syncedData, fcu, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)

	postState.SetVersion(clparams.GloasVersion)
	postState.SetFinalizedCheckpoint(solid.Checkpoint{Epoch: 5})
	builders := [3]*cltypes.Builder{
		{Pubkey: common.Bytes48{1}, Balance: 11, DepositEpoch: 5, WithdrawableEpoch: math.MaxUint64},
		{Pubkey: common.Bytes48{2}, Balance: 22, DepositEpoch: 4, WithdrawableEpoch: math.MaxUint64},
		{Pubkey: common.Bytes48{3}, Balance: 33, DepositEpoch: 1, WithdrawableEpoch: 7},
	}
	registry := solid.NewStaticListSSZ[*cltypes.Builder](int(postState.BeaconConfig().BuilderRegistryLimit), new(cltypes.Builder).EncodingSizeSSZ())
	for _, builder := range builders {
		registry.Append(builder)
	}
	postState.SetBuilders(registry)
	require.NoError(t, syncedData.OnHeadState(postState))
	fcu.HeadVal, _ = blocks[len(blocks)-1].Block.HashSSZ()
	fcu.HeadSlotVal = postState.Slot()
	fcu.IsRootOptimisticVal = true
	return handler, postState, builders
}

func postStateBuilders(t *testing.T, handler *ApiHandler, stateID, body string) (*http.Response, stateBuildersHTTPResponse) {
	t.Helper()
	server := httptest.NewServer(handler.mux)
	t.Cleanup(server.Close)
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/eth/v1/beacon/states/"+stateID+"/builders", bytes.NewBufferString(body))
	require.NoError(t, err)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := server.Client().Do(req)
	require.NoError(t, err)
	var decoded stateBuildersHTTPResponse
	if resp.StatusCode == http.StatusOK {
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&decoded))
	}
	return resp, decoded
}

func TestPostStateBuildersReturnsAndFiltersBuilders(t *testing.T) {
	handler, _, builders := setupStateBuildersHandler(t)

	t.Run("all without body", func(t *testing.T) {
		resp, decoded := postStateBuilders(t, handler, "head", "")
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.True(t, decoded.ExecutionOptimistic)
		require.False(t, decoded.Finalized)
		require.Equal(t, []string{"0", "1", "2"}, []string{decoded.Data[0].Index, decoded.Data[1].Index, decoded.Data[2].Index})
		require.Equal(t, []string{"pending", "active", "exited"}, []string{decoded.Data[0].Status, decoded.Data[1].Status, decoded.Data[2].Status})
		require.Equal(t, "0", decoded.Data[0].Builder.Version)
		require.Equal(t, builders[0].Pubkey, decoded.Data[0].Builder.Pubkey)
	})

	t.Run("index pubkey and status", func(t *testing.T) {
		body, err := json.Marshal(map[string]any{
			"ids":      []string{"0", builders[1].Pubkey.String(), "999"},
			"statuses": []string{"active", "pending"},
		})
		require.NoError(t, err)
		resp, decoded := postStateBuilders(t, handler, "head", string(body))
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Len(t, decoded.Data, 2)
		require.Equal(t, []string{"0", "1"}, []string{decoded.Data[0].Index, decoded.Data[1].Index})
	})
}

func TestPostStateBuildersRejectsInvalidFilters(t *testing.T) {
	handler, _, _ := setupStateBuildersHandler(t)
	tests := []string{
		`{"ids":["nope"]}`,
		`{"ids":["0","0"]}`,
		`{"statuses":["unknown"]}`,
		`{"statuses":["active","active"]}`,
		`{"ids":`,
		`{"unknown":true}`,
		`null`,
		`{} {}`,
		`{"ids":["01","1"]}`,
	}
	for _, body := range tests {
		t.Run(body, func(t *testing.T) {
			resp, _ := postStateBuilders(t, handler, "head", body)
			defer resp.Body.Close()
			require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		})
	}
}

func TestPostStateBuildersRejectsPreGloasAndMissingState(t *testing.T) {
	_, _, _, _, _, preGloasHandler, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), true)
	resp, _ := postStateBuilders(t, preGloasHandler, "head", `{}`)
	resp.Body.Close()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	handler, _, _ := setupStateBuildersHandler(t)
	resp, _ = postStateBuilders(t, handler, "finalized", `{}`)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}
