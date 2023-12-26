package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/stretchr/testify/require"
)

func TestGetSpec(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, _, _, _, _, handler, _, _, _ := setupTestingHandler(t, clparams.Phase0Version)

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/eth/v1/config/spec")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	out := make(map[string]interface{})
	err = json.NewDecoder(resp.Body).Decode(&out)
	require.NoError(t, err)

	data := out["data"].(map[string]interface{})
	require.Equal(t, data["SlotsPerEpoch"], float64(32))
	require.Equal(t, data["SlotsPerHistoricalRoot"], float64(8192))
}

func TestGetForkSchedule(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, _, _, _, _, handler, _, _, _ := setupTestingHandler(t, clparams.Phase0Version)

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/eth/v1/config/fork_schedule")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	out := make(map[string]interface{})
	err = json.NewDecoder(resp.Body).Decode(&out)
	require.NoError(t, err)

	require.Greater(t, len(out["data"].([]interface{})), 2)
	for _, v := range out["data"].([]interface{}) {
		data := v.(map[string]interface{})
		require.NotNil(t, data["current_version"])
		require.NotNil(t, data["epoch"])
		require.NotNil(t, data["previous_version"])
	}
}

func TestGetDepositContract(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, _, _, _, _, handler, _, _, _ := setupTestingHandler(t, clparams.Phase0Version)

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/eth/v1/config/deposit_contract")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	out := make(map[string]interface{})
	err = json.NewDecoder(resp.Body).Decode(&out)
	require.NoError(t, err)

	data := out["data"].(map[string]interface{})
	require.Equal(t, data["address"], "0x00000000219ab540356cBB839Cbe05303d7705Fa")
	require.Equal(t, data["chain_id"], "1")
}
