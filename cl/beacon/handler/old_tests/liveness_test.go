package handler

import (
	"bytes"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

func TestLiveness(t *testing.T) {
	//  i just want the correct schema to be generated
	_, blocks, _, _, _, handler, _, _, fcu := setupTestingHandler(t, clparams.Phase0Version)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	fcu.FinalizedCheckpointVal = solid.NewCheckpointFromParameters(fcu.HeadVal, fcu.HeadSlotVal/32)
	fcu.FinalizedSlotVal = math.MaxUint64
	reqBody := `["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]`
	server := httptest.NewServer(handler.mux)
	defer server.Close()
	//
	body := bytes.Buffer{}
	body.WriteString(reqBody)
	// Query the block in the handler with /eth/v2/beacon/states/{block_id} with content-type octet-stream
	req, err := http.NewRequest("POST", server.URL+"/eth/v1/validator/liveness/"+strconv.FormatUint(fcu.HeadSlotVal/32, 10), &body)
	require.NoError(t, err)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	out := map[string]interface{}{}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	data := out["data"].([]interface{})
	require.Equal(t, 11, len(data))
	// check that is has is_live (bool) and index (stringifed int)
	for _, d := range data {
		d := d.(map[string]interface{})
		require.Equal(t, 2, len(d))
		isLive, ok := d["is_live"]
		require.True(t, ok)
		_, ok = isLive.(bool)
		require.True(t, ok)
		i1, ok := d["index"]
		require.True(t, ok)
		strIndex, ok := i1.(string)
		require.True(t, ok)
		_, err := strconv.ParseUint(strIndex, 10, 64)
		require.NoError(t, err)

	}

}
