package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestGetBlindedBlock(t *testing.T) {

	// setupTestingHandler(t)
	_, blocks, _, _, _, handler, _, _, fcu := setupTestingHandler(t)

	// Start by testing
	rootBlock1, err := blocks[0].Block.HashSSZ()
	if err != nil {
		t.Fatal(err)
	}

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	cases := []struct {
		blockID string
		code    int
		slot    uint64
	}{
		{
			blockID: "0x" + common.Bytes2Hex(rootBlock1[:]),
			code:    http.StatusOK,
			slot:    blocks[0].Block.Slot,
		},
		{
			blockID: "head",
			code:    http.StatusOK,
			slot:    blocks[len(blocks)-1].Block.Slot,
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
			resp, err := http.Get(server.URL + "/eth/v1/beacon/blinded_blocks/" + c.blockID)
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
			message := data["message"].(map[string]interface{})

			// compare the block
			require.Equal(t, message["slot"], float64(c.slot))
		})
	}
}

func TestGetBlockBlinded(t *testing.T) {

	// setupTestingHandler(t)
	_, blocks, _, _, _, handler, _, _, fcu := setupTestingHandler(t)

	// Start by testing
	rootBlock1, err := blocks[0].Block.HashSSZ()
	if err != nil {
		t.Fatal(err)
	}

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	cases := []struct {
		blockID string
		code    int
		slot    uint64
	}{
		{
			blockID: "0x" + common.Bytes2Hex(rootBlock1[:]),
			code:    http.StatusOK,
			slot:    blocks[0].Block.Slot,
		},
		{
			blockID: "head",
			code:    http.StatusOK,
			slot:    blocks[len(blocks)-1].Block.Slot,
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
			resp, err := http.Get(server.URL + "/eth/v2/beacon/blocks/" + c.blockID)
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
			message := data["message"].(map[string]interface{})

			// compare the block
			require.Equal(t, message["slot"], float64(c.slot))
		})
	}
}

func TestGetBlockAttestations(t *testing.T) {

	// setupTestingHandler(t)
	_, blocks, _, _, _, handler, _, _, fcu := setupTestingHandler(t)

	// Start by testing
	rootBlock1, err := blocks[0].Block.HashSSZ()
	if err != nil {
		t.Fatal(err)
	}

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	cases := []struct {
		blockID string
		code    int
		attLen  int
	}{
		{
			blockID: "0x" + common.Bytes2Hex(rootBlock1[:]),
			code:    http.StatusOK,
			attLen:  blocks[0].Block.Body.Attestations.Len(),
		},
		{
			blockID: "head",
			code:    http.StatusOK,
			attLen:  blocks[len(blocks)-1].Block.Body.Attestations.Len(),
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
			resp, err := http.Get(server.URL + "/eth/v1/beacon/blocks/" + c.blockID + "/attestations")
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
			require.Equal(t, len(data), c.attLen)
		})
	}
}

func TestGetBlockRoot(t *testing.T) {

	// setupTestingHandler(t)
	_, blocks, _, _, _, handler, _, _, fcu := setupTestingHandler(t)

	var err error

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot
	// compute block 0 and block len -1 root
	blk0Root, err := blocks[0].Block.HashSSZ()
	require.NoError(t, err)

	blkLastRoot, err := blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	cases := []struct {
		blockID string
		code    int
		root    string
	}{
		{
			blockID: strconv.FormatInt(int64(blocks[0].Block.Slot), 10),
			code:    http.StatusOK,
			root:    "0x" + common.Bytes2Hex(blk0Root[:]),
		},
		{
			blockID: "head",
			code:    http.StatusOK,
			root:    "0x" + common.Bytes2Hex(blkLastRoot[:]),
		},
		{
			blockID: "19912929",
			code:    http.StatusNotFound,
		},
	}

	for _, c := range cases {
		t.Run(c.blockID, func(t *testing.T) {
			server := httptest.NewServer(handler.mux)
			defer server.Close()
			// Query the block in the handler with /eth/v2/beacon/blocks/{block_id}
			resp, err := http.Get(server.URL + "/eth/v1/beacon/blocks/" + c.blockID + "/root")
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
			root := data["root"].(string)
			require.Equal(t, root, c.root)
		})
	}
}
