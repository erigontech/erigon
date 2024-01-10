package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestGetHeader(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, _, handler, _, _, fcu := setupTestingHandler(t, clparams.Phase0Version)

	// Start by testing
	rootBlock1, err := blocks[0].Block.HashSSZ()
	if err != nil {
		t.Fatal(err)
	}

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	bodyRoot1, err := blocks[0].Block.Body.HashSSZ()
	require.NoError(t, err)

	bodyRoot2, err := blocks[len(blocks)-1].Block.Body.HashSSZ()
	require.NoError(t, err)

	cases := []struct {
		blockID  string
		code     int
		slot     uint64
		bodyRoot string
	}{
		{
			blockID:  "0x" + common.Bytes2Hex(rootBlock1[:]),
			code:     http.StatusOK,
			slot:     blocks[0].Block.Slot,
			bodyRoot: "0x" + common.Bytes2Hex(bodyRoot1[:]),
		},
		{
			blockID:  "head",
			code:     http.StatusOK,
			slot:     blocks[len(blocks)-1].Block.Slot,
			bodyRoot: "0x" + common.Bytes2Hex(bodyRoot2[:]),
		},
		{
			blockID: "0x" + common.Bytes2Hex(make([]byte, 32)),
			code:    http.StatusNotFound,
		},
	}

	for _, c := range cases {
		t.Run(c.blockID, func(t *testing.T) {
			server := httptest.NewServer(handler.mux)
			defer server.Close()
			// Query the block in the handler with /eth/v2/beacon/blocks/{block_id}
			resp, err := http.Get(server.URL + "/eth/v1/beacon/headers/" + c.blockID)
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
			header := data["header"].(map[string]interface{})
			message := header["message"].(map[string]interface{})

			// compare the block
			require.Equal(t, message["slot"], strconv.FormatInt(int64(c.slot), 10))
			require.Equal(t, message["body_root"], c.bodyRoot)
			require.Equal(t, data["canonical"], true)
		})
	}
}

func TestGetHeaders(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, _, handler, _, _, fcu := setupTestingHandler(t, clparams.Phase0Version)

	var err error

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	bodyRoot1, err := blocks[0].Block.Body.HashSSZ()
	require.NoError(t, err)

	bodyRoot2, err := blocks[len(blocks)-1].Block.Body.HashSSZ()
	require.NoError(t, err)

	cases := []struct {
		name       string
		code       int
		slotReq    *uint64
		parentRoot *libcommon.Hash
		slot       uint64
		bodyRoot   string
		count      int
	}{
		{
			count:    1,
			name:     "slot",
			code:     http.StatusOK,
			slotReq:  &blocks[0].Block.Slot,
			slot:     blocks[0].Block.Slot,
			bodyRoot: "0x" + common.Bytes2Hex(bodyRoot1[:]),
		},
		{
			count:    0,
			name:     "none",
			code:     http.StatusOK,
			slot:     blocks[len(blocks)-1].Block.Slot,
			bodyRoot: "0x" + common.Bytes2Hex(bodyRoot2[:]),
		},
		{
			count:      0,
			name:       "parent",
			code:       http.StatusOK,
			slotReq:    &blocks[0].Block.Slot,
			slot:       blocks[0].Block.Slot,
			parentRoot: &blocks[0].Block.ParentRoot,
			bodyRoot:   "0x" + common.Bytes2Hex(bodyRoot1[:]),
		},
		{
			count:      0,
			name:       "wtf",
			code:       http.StatusOK,
			slotReq:    new(uint64),
			slot:       blocks[0].Block.Slot,
			parentRoot: &blocks[0].Block.ParentRoot,
			bodyRoot:   "0x" + common.Bytes2Hex(bodyRoot1[:]),
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			server := httptest.NewServer(handler.mux)
			defer server.Close()
			url := server.URL + "/eth/v1/beacon/headers?lol=0" // lol is a random query param

			if c.slotReq != nil {
				url += "&slot=" + strconv.FormatInt(int64(*c.slotReq), 10)
			}
			if c.parentRoot != nil {
				url += "&parent_root=" + "0x" + common.Bytes2Hex(c.parentRoot[:])
			}
			// Query the block in the handler with /eth/v2/beacon/blocks/{block_id}
			resp, err := http.Get(url)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, c.code, resp.StatusCode)
			if resp.StatusCode != http.StatusOK {
				return
			}
			jsonVal := make(map[string]interface{})
			// unmarshal the json
			require.NoError(t, json.NewDecoder(resp.Body).Decode(&jsonVal))
			data := jsonVal["data"].([]interface{})
			require.Equal(t, len(data), c.count)
		})
	}
}
