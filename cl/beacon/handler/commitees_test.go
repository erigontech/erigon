package handler

import (
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestGetCommitteesAntiquated(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, postState, handler, _, _, fcu := setupTestingHandler(t, clparams.Phase0Version)

	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	fcu.FinalizedCheckpointVal = solid.NewCheckpointFromParameters(fcu.HeadVal, fcu.HeadSlotVal/32)
	fcu.FinalizedSlotVal = math.MaxUint64

	fcu.StateAtBlockRootVal[fcu.HeadVal] = postState

	cases := []struct {
		name     string
		blockID  string
		code     int
		query    string
		expected string
	}{
		{
			name:     "slot",
			blockID:  "0x" + common.Bytes2Hex(postRoot[:]),
			code:     http.StatusOK,
			query:    "?slot=" + strconv.FormatUint(fcu.HeadSlotVal, 10),
			expected: `{"data":[{"index":"0","slot":"8322","validators":["0","104","491","501","379","318","275","504","75","280","105","399","35","401"]}],"finalized":true,"execution_optimistic":false}` + "\n",
		},
		{
			name:     "empty-index",
			blockID:  "0x" + common.Bytes2Hex(postRoot[:]),
			code:     http.StatusOK,
			query:    "?index=1",
			expected: `{"data":[],"finalized":true,"execution_optimistic":false}` + "\n",
		},
		{
			name:     "all-queries",
			blockID:  "0x" + common.Bytes2Hex(postRoot[:]),
			code:     http.StatusOK,
			query:    "?index=0&slot=" + strconv.FormatUint(fcu.HeadSlotVal-32, 10) + "&epoch=" + strconv.FormatUint((fcu.HeadSlotVal/32)-1, 10),
			expected: `{"data":[{"index":"0","slot":"8290","validators":["127","377","274","85","309","420","423","398","153","480","273","429","374","260"]}],"finalized":true,"execution_optimistic":false}` + "\n",
		},
		{
			blockID: "0x" + common.Bytes2Hex(make([]byte, 32)),
			code:    http.StatusNotFound,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			server := httptest.NewServer(handler.mux)
			defer server.Close()
			// Query the block in the handler with /eth/v2/beacon/states/{block_id} with content-type octet-stream
			req, err := http.NewRequest("GET", server.URL+"/eth/v1/beacon/states/"+c.blockID+"/committees"+c.query, nil)
			require.NoError(t, err)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()
			require.Equal(t, c.code, resp.StatusCode)
			if resp.StatusCode != http.StatusOK {
				return
			}
			// read the all of the octect
			out, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, c.expected, string(out))
		})
	}
}

func TestGetCommitteesNonAntiquated(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, postState, handler, _, sm, fcu := setupTestingHandler(t, clparams.Phase0Version)

	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	fcu.FinalizedCheckpointVal = solid.NewCheckpointFromParameters(fcu.HeadVal, fcu.HeadSlotVal/32)
	fcu.FinalizedSlotVal = 0

	fcu.StateAtBlockRootVal[fcu.HeadVal] = postState
	require.NoError(t, sm.OnHeadState(postState))
	cases := []struct {
		name     string
		blockID  string
		code     int
		query    string
		expected string
	}{
		{
			name:     "slot",
			blockID:  "0x" + common.Bytes2Hex(postRoot[:]),
			code:     http.StatusOK,
			query:    "?slot=" + strconv.FormatUint(fcu.HeadSlotVal, 10),
			expected: `{"data":[{"index":"0","slot":"8322","validators":["0","104","491","501","379","318","275","504","75","280","105","399","35","401"]}],"finalized":false,"execution_optimistic":false}` + "\n",
		},
		{
			name:     "empty-index",
			blockID:  "0x" + common.Bytes2Hex(postRoot[:]),
			code:     http.StatusOK,
			query:    "?index=1",
			expected: `{"data":[],"finalized":false,"execution_optimistic":false}` + "\n",
		},
		{
			name:     "all-queries",
			blockID:  "0x" + common.Bytes2Hex(postRoot[:]),
			code:     http.StatusOK,
			query:    "?index=0&slot=" + strconv.FormatUint(fcu.HeadSlotVal-32, 10) + "&epoch=" + strconv.FormatUint((fcu.HeadSlotVal/32)-1, 10),
			expected: `{"data":[{"index":"0","slot":"8290","validators":["127","377","274","85","309","420","423","398","153","480","273","429","374","260"]}],"finalized":false,"execution_optimistic":false}` + "\n",
		},
		{
			blockID: "0x" + common.Bytes2Hex(make([]byte, 32)),
			code:    http.StatusNotFound,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			server := httptest.NewServer(handler.mux)
			defer server.Close()
			// Query the block in the handler with /eth/v2/beacon/states/{block_id} with content-type octet-stream
			req, err := http.NewRequest("GET", server.URL+"/eth/v1/beacon/states/"+c.blockID+"/committees"+c.query, nil)
			require.NoError(t, err)

			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()
			require.Equal(t, c.code, resp.StatusCode)
			if resp.StatusCode != http.StatusOK {
				return
			}
			// read the all of the octect
			out, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, c.expected, string(out))
		})
	}
}
