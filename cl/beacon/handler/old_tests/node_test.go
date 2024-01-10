package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/stretchr/testify/require"
)

func TestNodeHealthSyncing(t *testing.T) {
	//  i just want the correct schema to be generated
	_, _, _, _, _, handler, _, _, _ := setupTestingHandler(t, clparams.Phase0Version)

	// Call GET /eth/v1/node/health
	server := httptest.NewServer(handler.mux)
	defer server.Close()

	req, err := http.NewRequest("GET", server.URL+"/eth/v1/node/health?syncing_status=666", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 666, resp.StatusCode)
}

func TestNodeHealthSyncingTip(t *testing.T) {
	//  i just want the correct schema to be generated
	_, _, _, _, post, handler, _, sm, _ := setupTestingHandler(t, clparams.Phase0Version)

	// Call GET /eth/v1/node/health
	server := httptest.NewServer(handler.mux)
	defer server.Close()

	req, err := http.NewRequest("GET", server.URL+"/eth/v1/node/health?syncing_status=666", nil)
	require.NoError(t, err)

	require.NoError(t, sm.OnHeadState(post))
	s, cancel := sm.HeadState()
	s.SetSlot(999999999999999)
	cancel()

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
}

func TestNodeVersion(t *testing.T) {
	//  i just want the correct schema to be generated
	_, _, _, _, _, handler, _, _, _ := setupTestingHandler(t, clparams.Phase0Version)

	// Call GET /eth/v1/node/health
	server := httptest.NewServer(handler.mux)
	defer server.Close()

	req, err := http.NewRequest("GET", server.URL+"/eth/v1/node/version", nil)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	out := map[string]interface{}{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	v := out["data"].(map[string]interface{})["version"].(string)
	require.True(t, strings.Contains(v, "Caplin"))
}
