package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"
)

func TestPostEthV1ValidatorPreparation(t *testing.T) {
	_, _, _, _, _, handler, _, _, _, vp := setupTestingHandler(t, clparams.BellatrixVersion, log.Root())
	server := httptest.NewServer(handler.mux)
	defer server.Close()
	req := []ValidatorPreparationPayload{
		{
			ValidatorIndex: 1,
			FeeRecipient:   libcommon.Address{1},
		},
		{
			ValidatorIndex: 2,
			FeeRecipient:   libcommon.Address{2},
		},
	}

	reqByte, err := json.Marshal(req)
	require.NoError(t, err)

	resp, err := http.Post(server.URL+"/eth/v1/validator/prepare_beacon_proposer", "application/json", bytes.NewBuffer(reqByte))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	a1, _ := vp.GetFeeRecipient(1)
	a2, _ := vp.GetFeeRecipient(2)

	require.Equal(t, libcommon.Address{1}, a1)
	require.Equal(t, libcommon.Address{2}, a2)
}
