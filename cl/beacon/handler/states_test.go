package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestGetStateFork(t *testing.T) {

	// setupTestingHandler(t)
	_, blocks, _, _, postState, handler, _, _, fcu := setupTestingHandler(t)

	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot
	fmt.Println(fcu.HeadSlotVal)

	cases := []struct {
		blockID string
		code    int
	}{
		{
			blockID: "0x" + common.Bytes2Hex(postRoot[:]),
			code:    http.StatusOK,
		},
		{
			blockID: "head",
			code:    http.StatusOK,
		},
		{
			blockID: "0x" + common.Bytes2Hex(make([]byte, 32)),
			code:    http.StatusNotFound,
		},
		{
			blockID: strconv.FormatInt(int64(postState.Slot()), 10),
			code:    http.StatusOK,
		},
	}

	for _, c := range cases {
		t.Run(c.blockID, func(t *testing.T) {
			server := httptest.NewServer(handler.mux)
			defer server.Close()
			// Query the block in the handler with /eth/v2/beacon/blocks/{block_id}
			resp, err := http.Get(server.URL + "/eth/v1/beacon/states/" + c.blockID + "/fork")
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, c.code, resp.StatusCode)
			if resp.StatusCode != http.StatusOK {
				return
			}
			jsonVal := make(map[string]interface{})
			// unmarshal the json
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&jsonVal))
			data := jsonVal["data"].(map[string]interface{})
			require.Equal(t, data["current_version"], "0x00000000")
			require.Equal(t, data["previous_version"], "0x00000000")
			require.Equal(t, data["epoch"], float64(0))
		})
	}
}

func TestGetStateRoot(t *testing.T) {

	// setupTestingHandler(t)
	_, blocks, _, _, postState, handler, _, _, fcu := setupTestingHandler(t)

	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot
	fmt.Println(fcu.HeadSlotVal)

	fcu.FinalizedCheckpointVal = solid.NewCheckpointFromParameters(fcu.HeadVal, fcu.HeadSlotVal/32)

	cases := []struct {
		blockID string
		code    int
	}{
		{
			blockID: "0x" + common.Bytes2Hex(postRoot[:]),
			code:    http.StatusOK,
		},
		{
			blockID: "finalized",
			code:    http.StatusOK,
		},
		{
			blockID: "0x" + common.Bytes2Hex(make([]byte, 32)),
			code:    http.StatusNotFound,
		},
		{
			blockID: strconv.FormatInt(int64(postState.Slot()), 10),
			code:    http.StatusOK,
		},
	}

	for _, c := range cases {
		t.Run(c.blockID, func(t *testing.T) {
			server := httptest.NewServer(handler.mux)
			defer server.Close()
			// Query the block in the handler with /eth/v2/beacon/blocks/{block_id}
			resp, err := http.Get(server.URL + "/eth/v1/beacon/states/" + c.blockID + "/root")
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, c.code, resp.StatusCode)
			if resp.StatusCode != http.StatusOK {
				return
			}
			jsonVal := make(map[string]interface{})
			// unmarshal the json
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&jsonVal))
			data := jsonVal["data"].(map[string]interface{})
			require.Equal(t, data["root"], "0x"+common.Bytes2Hex(postRoot[:]))
		})
	}
}
