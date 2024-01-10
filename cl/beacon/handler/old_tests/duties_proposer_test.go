package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

func TestProposerDutiesProposerFcu(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, postState, _, handler, _, syncedDataManager, fcu := setupTestingHandler(t, clparams.Phase0Version)
	epoch := blocks[len(blocks)-1].Block.Slot / 32

	require.NoError(t, syncedDataManager.OnHeadState(postState))

	fcu.FinalizedCheckpointVal = solid.NewCheckpointFromParameters(common.Hash{}, epoch)

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/eth/v1/validator/duties/proposer/" + strconv.FormatUint(epoch, 10))
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	out := make(map[string]interface{})
	err = json.NewDecoder(resp.Body).Decode(&out)
	require.NoError(t, err)

	data := out["data"].([]interface{})
	require.Equal(t, len(data), 32)
	for _, v := range data {
		d := v.(map[string]interface{})
		require.NotNil(t, d["pubkey"])
		require.NotNil(t, d["validator_index"])
		require.NotNil(t, d["slot"])
	}
}

func TestProposerDutiesProposerBadEpoch(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, _, _, postState, _, handler, _, syncedDataManager, fcu := setupTestingHandler(t, clparams.Phase0Version)

	require.NoError(t, syncedDataManager.OnHeadState(postState))

	fcu.FinalizedCheckpointVal = solid.NewCheckpointFromParameters(common.Hash{}, 1)

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/eth/v1/validator/duties/proposer/abc")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestProposerDutiesNotSynced(t *testing.T) {
	_, _, _, _, _, handler, _, _, fcu := setupTestingHandler(t, clparams.Phase0Version)

	fcu.FinalizedCheckpointVal = solid.NewCheckpointFromParameters(common.Hash{}, 1)

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/eth/v1/validator/duties/proposer/1")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func TestProposerDutiesProposerFcuHistorical(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, postState, _, handler, _, syncedDataManager, fcu := setupTestingHandler(t, clparams.Phase0Version)
	epoch := blocks[len(blocks)-1].Block.Slot / 32

	require.NoError(t, syncedDataManager.OnHeadState(postState))

	fcu.FinalizedCheckpointVal = solid.NewCheckpointFromParameters(common.Hash{}, epoch)

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/eth/v1/validator/duties/proposer/" + strconv.FormatUint(epoch-1, 10))
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	out := make(map[string]interface{})
	err = json.NewDecoder(resp.Body).Decode(&out)
	require.NoError(t, err)

	data := out["data"].([]interface{})
	require.Equal(t, len(data), 32)
	for _, v := range data {
		d := v.(map[string]interface{})
		require.NotNil(t, d["pubkey"])
		require.NotNil(t, d["validator_index"])
		require.NotNil(t, d["slot"])
	}
}
