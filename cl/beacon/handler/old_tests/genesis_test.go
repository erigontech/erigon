package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/stretchr/testify/require"
)

func TestGetGenesis(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, _, _, _, _, handler, _, _, _ := setupTestingHandler(t, clparams.Phase0Version)

	server := httptest.NewServer(handler.mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/eth/v1/beacon/genesis")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	out := make(map[string]interface{})
	err = json.NewDecoder(resp.Body).Decode(&out)
	require.NoError(t, err)

	data := out["data"].(map[string]interface{})
	genesisTime := data["genesis_time"].(string)
	require.Equal(t, genesisTime, "1606824023")
	require.Equal(t, data["genesis_fork_version"], "0xbba4da96")
	require.Equal(t, data["genesis_validators_root"], "0x4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95")
}
