package handler

import (
	"bytes"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/stretchr/testify/require"
)

func TestDutiesAttesterAntiquated(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, postState, handler, _, _, fcu := setupTestingHandler(t, clparams.Phase0Version)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	fcu.FinalizedCheckpointVal = solid.NewCheckpointFromParameters(fcu.HeadVal, fcu.HeadSlotVal/32)
	fcu.FinalizedSlotVal = math.MaxUint64

	fcu.StateAtBlockRootVal[fcu.HeadVal] = postState

	cases := []struct {
		name     string
		epoch    string
		code     int
		reqBody  string
		expected string
	}{
		{
			name:     "non-empty-indicies",
			epoch:    strconv.FormatUint(fcu.HeadSlotVal/32, 10),
			code:     http.StatusOK,
			reqBody:  `["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]`,
			expected: `{"data":[{"pubkey":"0x97f1d3a73197d7942695638c4fa9ac0fc3688c4f9774b905a14e3a3f171bac586c55e83ff97a1aeffb3af00adb22c6bb","validator_index":"0","committee_index":"0","committee_length":"14","validator_committee_index":"0","committees_at_slot":"1","slot":"8322"},{"pubkey":"0xb0e7791fb972fe014159aa33a98622da3cdc98ff707965e536d8636b5fcc5ac7a91a8c46e59a00dca575af0f18fb13dc","validator_index":"4","committee_index":"0","committee_length":"13","validator_committee_index":"5","committees_at_slot":"1","slot":"8327"},{"pubkey":"0xb928f3beb93519eecf0145da903b40a4c97dca00b21f12ac0df3be9116ef2ef27b2ae6bcd4c5bc2d54ef5a70627efcb7","validator_index":"6","committee_index":"0","committee_length":"13","validator_committee_index":"10","committees_at_slot":"1","slot":"8327"},{"pubkey":"0xa6e82f6da4520f85c5d27d8f329eccfa05944fd1096b20734c894966d12a9e2a9a9744529d7212d33883113a0cadb909","validator_index":"5","committee_index":"0","committee_length":"14","validator_committee_index":"10","committees_at_slot":"1","slot":"8329"},{"pubkey":"0x89ece308f9d1f0131765212deca99697b112d61f9be9a5f1f3780a51335b3ff981747a0b2ca2179b96d2c0c9024e5224","validator_index":"2","committee_index":"0","committee_length":"14","validator_committee_index":"11","committees_at_slot":"1","slot":"8331"},{"pubkey":"0xaf81da25ecf1c84b577fefbedd61077a81dc43b00304015b2b596ab67f00e41c86bb00ebd0f90d4b125eb0539891aeed","validator_index":"9","committee_index":"0","committee_length":"14","validator_committee_index":"8","committees_at_slot":"1","slot":"8342"},{"pubkey":"0xac9b60d5afcbd5663a8a44b7c5a02f19e9a77ab0a35bd65809bb5c67ec582c897feb04decc694b13e08587f3ff9b5b60","validator_index":"3","committee_index":"0","committee_length":"13","validator_committee_index":"6","committees_at_slot":"1","slot":"8348"}],"execution_optimistic":false}` + "\n",
		},
		{
			name:     "empty-index",
			epoch:    strconv.FormatUint(fcu.HeadSlotVal/32, 10),
			code:     http.StatusOK,
			reqBody:  `[]`,
			expected: `{"data":[],"execution_optimistic":false}` + "\n",
		},
		{
			name:    "404",
			reqBody: `["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]`,
			epoch:   `999999999`,
			code:    http.StatusBadRequest,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			server := httptest.NewServer(handler.mux)
			defer server.Close()
			//
			body := bytes.Buffer{}
			body.WriteString(c.reqBody)
			// Query the block in the handler with /eth/v2/beacon/states/{block_id} with content-type octet-stream
			req, err := http.NewRequest("POST", server.URL+"/eth/v1/validator/duties/attester/"+c.epoch, &body)
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

func TestDutiesAttesterNonAntiquated(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, postState, handler, _, sm, fcu := setupTestingHandler(t, clparams.Phase0Version)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot

	fcu.FinalizedCheckpointVal = solid.NewCheckpointFromParameters(fcu.HeadVal, fcu.HeadSlotVal/32)
	fcu.FinalizedSlotVal = 0

	fcu.StateAtBlockRootVal[fcu.HeadVal] = postState
	require.NoError(t, sm.OnHeadState(postState))
	cases := []struct {
		name     string
		epoch    string
		code     int
		reqBody  string
		expected string
	}{
		{
			name:     "non-empty-indicies",
			epoch:    strconv.FormatUint(fcu.HeadSlotVal/32, 10),
			code:     http.StatusOK,
			reqBody:  `["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]`,
			expected: `{"data":[{"pubkey":"0x97f1d3a73197d7942695638c4fa9ac0fc3688c4f9774b905a14e3a3f171bac586c55e83ff97a1aeffb3af00adb22c6bb","validator_index":"0","committee_index":"0","committee_length":"14","validator_committee_index":"0","committees_at_slot":"1","slot":"8322"},{"pubkey":"0xb0e7791fb972fe014159aa33a98622da3cdc98ff707965e536d8636b5fcc5ac7a91a8c46e59a00dca575af0f18fb13dc","validator_index":"4","committee_index":"0","committee_length":"13","validator_committee_index":"5","committees_at_slot":"1","slot":"8327"},{"pubkey":"0xb928f3beb93519eecf0145da903b40a4c97dca00b21f12ac0df3be9116ef2ef27b2ae6bcd4c5bc2d54ef5a70627efcb7","validator_index":"6","committee_index":"0","committee_length":"13","validator_committee_index":"10","committees_at_slot":"1","slot":"8327"},{"pubkey":"0xa6e82f6da4520f85c5d27d8f329eccfa05944fd1096b20734c894966d12a9e2a9a9744529d7212d33883113a0cadb909","validator_index":"5","committee_index":"0","committee_length":"14","validator_committee_index":"10","committees_at_slot":"1","slot":"8329"},{"pubkey":"0x89ece308f9d1f0131765212deca99697b112d61f9be9a5f1f3780a51335b3ff981747a0b2ca2179b96d2c0c9024e5224","validator_index":"2","committee_index":"0","committee_length":"14","validator_committee_index":"11","committees_at_slot":"1","slot":"8331"},{"pubkey":"0xaf81da25ecf1c84b577fefbedd61077a81dc43b00304015b2b596ab67f00e41c86bb00ebd0f90d4b125eb0539891aeed","validator_index":"9","committee_index":"0","committee_length":"14","validator_committee_index":"8","committees_at_slot":"1","slot":"8342"},{"pubkey":"0xac9b60d5afcbd5663a8a44b7c5a02f19e9a77ab0a35bd65809bb5c67ec582c897feb04decc694b13e08587f3ff9b5b60","validator_index":"3","committee_index":"0","committee_length":"13","validator_committee_index":"6","committees_at_slot":"1","slot":"8348"}],"execution_optimistic":false}` + "\n",
		},
		{
			name:     "empty-index",
			epoch:    strconv.FormatUint(fcu.HeadSlotVal/32, 10),
			code:     http.StatusOK,
			reqBody:  `[]`,
			expected: `{"data":[],"execution_optimistic":false}` + "\n",
		},
		{
			name:    "404",
			reqBody: `["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]`,
			epoch:   `999999999`,
			code:    http.StatusBadRequest,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			server := httptest.NewServer(handler.mux)
			defer server.Close()
			//
			body := bytes.Buffer{}
			body.WriteString(c.reqBody)
			// Query the block in the handler with /eth/v2/beacon/states/{block_id} with content-type octet-stream
			req, err := http.NewRequest("POST", server.URL+"/eth/v1/validator/duties/attester/"+c.epoch, &body)
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
