// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package handler

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

func TestGetBlockRewards(t *testing.T) {
	_, blocks, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), false)
	var err error
	fcu.HeadVal, err = blocks[len(blocks)-5].Block.HashSSZ()
	require.NoError(t, err)
	genesisVal, err := blocks[0].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot
	//fcu.FinalizedCheckpointVal = solid.NewCheckpointFromParameters(fcu.HeadVal, math.MaxUint64)
	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: math.MaxUint64, Root: fcu.HeadVal}
	fcu.FinalizedSlotVal = math.MaxUint64

	cases := []struct {
		blockID      string
		code         int
		expectedResp string
	}{
		{
			blockID:      "0x" + common.Bytes2Hex(fcu.HeadVal[:]),
			code:         http.StatusOK,
			expectedResp: `{"data":{"proposer_index":"203","attestations":"332205","proposer_slashings":"0","attester_slashings":"0","sync_aggregate":"0","total":"332205"},"execution_optimistic":false,"finalized":true}` + "\n",
		},
		{
			blockID:      "0x" + common.Bytes2Hex(genesisVal[:]),
			code:         http.StatusOK,
			expectedResp: `{"data":{"proposer_index":"98","attestations":"332205","proposer_slashings":"0","attester_slashings":"0","sync_aggregate":"0","total":"332205"},"execution_optimistic":false,"finalized":true}` + "\n",
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
			resp, err := http.Get(server.URL + "/eth/v1/beacon/rewards/blocks/" + c.blockID)
			require.NoError(t, err)
			defer resp.Body.Close()
			require.Equal(t, c.code, resp.StatusCode)
			if resp.StatusCode != http.StatusOK {
				return
			}

			// unmarshal the json
			out, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.Equal(t, c.expectedResp, string(out))
		})
	}
}

func TestPostSyncCommitteeRewards(t *testing.T) {
	_, blocks, _, _, _, handler, _, _, fcu, _ := setupTestingHandler(t, clparams.BellatrixVersion, log.Root(), true)
	var err error
	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot
	fcu.FinalizedSlotVal = math.MaxInt64

	fcu.FinalizedCheckpointVal = solid.Checkpoint{Epoch: 99999999, Root: fcu.HeadVal}
	fcu.JustifiedCheckpointVal = solid.Checkpoint{Epoch: fcu.HeadSlotVal / 32, Root: fcu.HeadVal}

	cases := []struct {
		name     string
		blockId  string
		code     int
		request  string
		expected string
	}{
		{
			name:     "all validators",
			blockId:  "0x" + common.Bytes2Hex(fcu.HeadVal[:]),
			code:     http.StatusOK,
			expected: `{"data":[{"validator_index":"0","reward":"-698"},{"validator_index":"1","reward":"-698"},{"validator_index":"2","reward":"-698"},{"validator_index":"3","reward":"-698"},{"validator_index":"4","reward":"-698"},{"validator_index":"5","reward":"-698"},{"validator_index":"6","reward":"-698"},{"validator_index":"7","reward":"-698"},{"validator_index":"8","reward":"-698"},{"validator_index":"9","reward":"-698"},{"validator_index":"10","reward":"-698"},{"validator_index":"11","reward":"-698"},{"validator_index":"12","reward":"-698"},{"validator_index":"13","reward":"-698"},{"validator_index":"14","reward":"-698"},{"validator_index":"15","reward":"-698"},{"validator_index":"16","reward":"-698"},{"validator_index":"17","reward":"-698"},{"validator_index":"18","reward":"-698"},{"validator_index":"19","reward":"-698"},{"validator_index":"20","reward":"-698"},{"validator_index":"21","reward":"-698"},{"validator_index":"22","reward":"-698"},{"validator_index":"23","reward":"-698"},{"validator_index":"24","reward":"-698"},{"validator_index":"25","reward":"-698"},{"validator_index":"26","reward":"-698"},{"validator_index":"27","reward":"-698"},{"validator_index":"28","reward":"-698"},{"validator_index":"29","reward":"-698"},{"validator_index":"30","reward":"-698"},{"validator_index":"31","reward":"-698"},{"validator_index":"32","reward":"-698"},{"validator_index":"33","reward":"-698"},{"validator_index":"34","reward":"-698"},{"validator_index":"35","reward":"-698"},{"validator_index":"36","reward":"-698"},{"validator_index":"37","reward":"-698"},{"validator_index":"38","reward":"-698"},{"validator_index":"39","reward":"-698"},{"validator_index":"40","reward":"-698"},{"validator_index":"41","reward":"-698"},{"validator_index":"42","reward":"-698"},{"validator_index":"43","reward":"-698"},{"validator_index":"44","reward":"-698"},{"validator_index":"45","reward":"-698"},{"validator_index":"46","reward":"-698"},{"validator_index":"47","reward":"-698"},{"validator_index":"48","reward":"-698"},{"validator_index":"49","reward":"-698"},{"validator_index":"50","reward":"-698"},{"validator_index":"51","reward":"-698"},{"validator_index":"52","reward":"-698"},{"validator_index":"53","reward":"-698"},{"validator_index":"54","reward":"-698"},{"validator_index":"55","reward":"-698"},{"validator_index":"56","reward":"-698"},{"validator_index":"57","reward":"-698"},{"validator_index":"58","reward":"-698"},{"validator_index":"59","reward":"-698"},{"validator_index":"60","reward":"-698"},{"validator_index":"61","reward":"-698"},{"validator_index":"62","reward":"-698"},{"validator_index":"63","reward":"-698"},{"validator_index":"64","reward":"-698"},{"validator_index":"65","reward":"-698"},{"validator_index":"66","reward":"-698"},{"validator_index":"67","reward":"-698"},{"validator_index":"68","reward":"-698"},{"validator_index":"69","reward":"-698"},{"validator_index":"70","reward":"-698"},{"validator_index":"71","reward":"-698"},{"validator_index":"72","reward":"-698"},{"validator_index":"73","reward":"-698"},{"validator_index":"74","reward":"-698"},{"validator_index":"75","reward":"-698"},{"validator_index":"76","reward":"-698"},{"validator_index":"77","reward":"-698"},{"validator_index":"78","reward":"-698"},{"validator_index":"79","reward":"-698"},{"validator_index":"80","reward":"-698"},{"validator_index":"81","reward":"-698"},{"validator_index":"82","reward":"-698"},{"validator_index":"83","reward":"-698"},{"validator_index":"84","reward":"-698"},{"validator_index":"85","reward":"-698"},{"validator_index":"86","reward":"-698"},{"validator_index":"87","reward":"-698"},{"validator_index":"88","reward":"-698"},{"validator_index":"89","reward":"-698"},{"validator_index":"90","reward":"-698"},{"validator_index":"91","reward":"-698"},{"validator_index":"92","reward":"-698"},{"validator_index":"93","reward":"-698"},{"validator_index":"94","reward":"-698"},{"validator_index":"95","reward":"-698"},{"validator_index":"96","reward":"-698"},{"validator_index":"97","reward":"-698"},{"validator_index":"98","reward":"-698"},{"validator_index":"99","reward":"-698"},{"validator_index":"100","reward":"-698"},{"validator_index":"101","reward":"-698"},{"validator_index":"102","reward":"-698"},{"validator_index":"103","reward":"-698"},{"validator_index":"104","reward":"-698"},{"validator_index":"105","reward":"-698"},{"validator_index":"106","reward":"-698"},{"validator_index":"107","reward":"-698"},{"validator_index":"108","reward":"-698"},{"validator_index":"109","reward":"-698"},{"validator_index":"110","reward":"-698"},{"validator_index":"111","reward":"-698"},{"validator_index":"112","reward":"-698"},{"validator_index":"113","reward":"-698"},{"validator_index":"114","reward":"-698"},{"validator_index":"115","reward":"-698"},{"validator_index":"116","reward":"-698"},{"validator_index":"117","reward":"-698"},{"validator_index":"118","reward":"-698"},{"validator_index":"119","reward":"-698"},{"validator_index":"120","reward":"-698"},{"validator_index":"121","reward":"-698"},{"validator_index":"122","reward":"-698"},{"validator_index":"123","reward":"-698"},{"validator_index":"124","reward":"-698"},{"validator_index":"125","reward":"-698"},{"validator_index":"126","reward":"-698"},{"validator_index":"127","reward":"-698"},{"validator_index":"128","reward":"-698"},{"validator_index":"129","reward":"-698"},{"validator_index":"130","reward":"-698"},{"validator_index":"131","reward":"-698"},{"validator_index":"132","reward":"-698"},{"validator_index":"133","reward":"-698"},{"validator_index":"134","reward":"-698"},{"validator_index":"135","reward":"-698"},{"validator_index":"136","reward":"-698"},{"validator_index":"137","reward":"-698"},{"validator_index":"138","reward":"-698"},{"validator_index":"139","reward":"-698"},{"validator_index":"140","reward":"-698"},{"validator_index":"141","reward":"-698"},{"validator_index":"142","reward":"-698"},{"validator_index":"143","reward":"-698"},{"validator_index":"144","reward":"-698"},{"validator_index":"145","reward":"-698"},{"validator_index":"146","reward":"-698"},{"validator_index":"147","reward":"-698"},{"validator_index":"148","reward":"-698"},{"validator_index":"149","reward":"-698"},{"validator_index":"150","reward":"-698"},{"validator_index":"151","reward":"-698"},{"validator_index":"152","reward":"-698"},{"validator_index":"153","reward":"-698"},{"validator_index":"154","reward":"-698"},{"validator_index":"155","reward":"-698"},{"validator_index":"156","reward":"-698"},{"validator_index":"157","reward":"-698"},{"validator_index":"158","reward":"-698"},{"validator_index":"159","reward":"-698"},{"validator_index":"160","reward":"-698"},{"validator_index":"161","reward":"-698"},{"validator_index":"162","reward":"-698"},{"validator_index":"163","reward":"-698"},{"validator_index":"164","reward":"-698"},{"validator_index":"165","reward":"-698"},{"validator_index":"166","reward":"-698"},{"validator_index":"167","reward":"-698"},{"validator_index":"168","reward":"-698"},{"validator_index":"169","reward":"-698"},{"validator_index":"170","reward":"-698"},{"validator_index":"171","reward":"-698"},{"validator_index":"172","reward":"-698"},{"validator_index":"173","reward":"-698"},{"validator_index":"174","reward":"-698"},{"validator_index":"175","reward":"-698"},{"validator_index":"176","reward":"-698"},{"validator_index":"177","reward":"-698"},{"validator_index":"178","reward":"-698"},{"validator_index":"179","reward":"-698"},{"validator_index":"180","reward":"-698"},{"validator_index":"181","reward":"-698"},{"validator_index":"182","reward":"-698"},{"validator_index":"183","reward":"-698"},{"validator_index":"184","reward":"-698"},{"validator_index":"185","reward":"-698"},{"validator_index":"186","reward":"-698"},{"validator_index":"187","reward":"-698"},{"validator_index":"188","reward":"-698"},{"validator_index":"189","reward":"-698"},{"validator_index":"190","reward":"-698"},{"validator_index":"191","reward":"-698"},{"validator_index":"192","reward":"-698"},{"validator_index":"193","reward":"-698"},{"validator_index":"194","reward":"-698"},{"validator_index":"195","reward":"-698"},{"validator_index":"196","reward":"-698"},{"validator_index":"197","reward":"-698"},{"validator_index":"198","reward":"-698"},{"validator_index":"199","reward":"-698"},{"validator_index":"200","reward":"-698"},{"validator_index":"201","reward":"-698"},{"validator_index":"202","reward":"-698"},{"validator_index":"203","reward":"-698"},{"validator_index":"204","reward":"-698"},{"validator_index":"205","reward":"-698"},{"validator_index":"206","reward":"-698"},{"validator_index":"207","reward":"-698"},{"validator_index":"208","reward":"-698"},{"validator_index":"209","reward":"-698"},{"validator_index":"210","reward":"-698"},{"validator_index":"211","reward":"-698"},{"validator_index":"212","reward":"-698"},{"validator_index":"213","reward":"-698"},{"validator_index":"214","reward":"-698"},{"validator_index":"215","reward":"-698"},{"validator_index":"216","reward":"-698"},{"validator_index":"217","reward":"-698"},{"validator_index":"218","reward":"-698"},{"validator_index":"219","reward":"-698"},{"validator_index":"220","reward":"-698"},{"validator_index":"221","reward":"-698"},{"validator_index":"222","reward":"-698"},{"validator_index":"223","reward":"-698"},{"validator_index":"224","reward":"-698"},{"validator_index":"225","reward":"-698"},{"validator_index":"226","reward":"-698"},{"validator_index":"227","reward":"-698"},{"validator_index":"228","reward":"-698"},{"validator_index":"229","reward":"-698"},{"validator_index":"230","reward":"-698"},{"validator_index":"231","reward":"-698"},{"validator_index":"232","reward":"-698"},{"validator_index":"233","reward":"-698"},{"validator_index":"234","reward":"-698"},{"validator_index":"235","reward":"-698"},{"validator_index":"236","reward":"-698"},{"validator_index":"237","reward":"-698"},{"validator_index":"238","reward":"-698"},{"validator_index":"239","reward":"-698"},{"validator_index":"240","reward":"-698"},{"validator_index":"241","reward":"-698"},{"validator_index":"242","reward":"-698"},{"validator_index":"243","reward":"-698"},{"validator_index":"244","reward":"-698"},{"validator_index":"245","reward":"-698"},{"validator_index":"246","reward":"-698"},{"validator_index":"247","reward":"-698"},{"validator_index":"248","reward":"-698"},{"validator_index":"249","reward":"-698"},{"validator_index":"250","reward":"-698"},{"validator_index":"251","reward":"-698"},{"validator_index":"252","reward":"-698"},{"validator_index":"253","reward":"-698"},{"validator_index":"254","reward":"-698"},{"validator_index":"255","reward":"-698"}],"execution_optimistic":false,"finalized":true}` + "\n",
		},
		{
			blockId: "0x" + common.Bytes2Hex(make([]byte, 32)),
			code:    http.StatusNotFound,
		},
		{
			name:     "2 validators",
			blockId:  "0x" + common.Bytes2Hex(fcu.HeadVal[:]),
			request:  `["1","4"]`,
			code:     http.StatusOK,
			expected: `{"data":[{"validator_index":"1","reward":"-698"},{"validator_index":"4","reward":"-698"}],"execution_optimistic":false,"finalized":true}` + "\n", // Add your expected response
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			server := httptest.NewServer(handler.mux)
			defer server.Close()
			url := fmt.Sprintf("%s/eth/v1/beacon/rewards/sync_committee/%s", server.URL, c.blockId)

			// Create a request
			req, err := http.NewRequest("POST", url, strings.NewReader(c.request))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")

			// Perform the request
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			// Check status code
			require.Equal(t, c.code, resp.StatusCode)

			if resp.StatusCode != http.StatusOK {
				return
			}

			// Read the response body
			out, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			if string(out) != c.expected {
				panic(string(out))
			}
			// Compare the response with the expected result
			require.Equal(t, c.expected, string(out))
		})
	}
}
