package handler

import (
	"io"
	"net/http/httptest"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/stretchr/testify/require"
)

func TestGetHeads(t *testing.T) {
	// find server
	_, _, _, _, _, handler, _, _, fcu := setupTestingHandler(t, clparams.Phase0Version)
	fcu.HeadSlotVal = 128
	fcu.HeadVal = libcommon.Hash{1, 2, 3}
	server := httptest.NewServer(handler.mux)
	defer server.Close()

	// get heads
	resp, err := server.Client().Get(server.URL + "/eth/v2/debug/beacon/heads")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, 200, resp.StatusCode)
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, `{"data":[{"execution_optimistic":false,"root":"0x0102030000000000000000000000000000000000000000000000000000000000","slot":128}]}`+"\n", string(out))
}
