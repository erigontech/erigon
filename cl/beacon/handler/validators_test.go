package handler

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestGetAllValidators(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, postState, handler, _, _, fcu := setupTestingHandler(t, clparams.Phase0Version)

	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot
	fcu.FinalizedCheckpointVal = solid.NewCheckpointFromParameters(fcu.HeadVal, fcu.HeadSlotVal/32)

	cases := []struct {
		blockID      string
		code         int
		queryParams  string
		expectedResp string
	}{
		{
			blockID:      "0x" + common.Bytes2Hex(postRoot[:]),
			code:         http.StatusOK,
			queryParams:  "?id=1,2,3",
			expectedResp: `{"data":[{"index":"1","status":"withdrawal_possible","balance":"20125000000","validator":{"pubkey":"0xa572cbea904d67468808c8eb50a9450c9721db309128012543902d0ac358a62ae28f75bb8f1c7c42c39a8c5529bf0f4e","withdrawal_credentials":"0x307830303166303965643330356330373637643536663162336264623235663330313239383032376638653938613865306364326463626363363630373233643762","effective_balance":"20000000000","slashed":false,"activation_eligibility_epoch":"0","activation_epoch":"0","exit_epoch":"253","withdrawable_epoch":"257"}},{"index":"2","status":"active_slashed","balance":"25678253779","validator":{"pubkey":"0x89ece308f9d1f0131765212deca99697b112d61f9be9a5f1f3780a51335b3ff981747a0b2ca2179b96d2c0c9024e5224","withdrawal_credentials":"0x307830303661646334613165346361626133376335346435366432343131666430646633613130326638343839613463316265353335663466643566383831306339","effective_balance":"25000000000","slashed":true,"activation_eligibility_epoch":"0","activation_epoch":"0","exit_epoch":"261","withdrawable_epoch":"8448"}},{"index":"3","status":"active_slashed","balance":"35998164834","validator":{"pubkey":"0xac9b60d5afcbd5663a8a44b7c5a02f19e9a77ab0a35bd65809bb5c67ec582c897feb04decc694b13e08587f3ff9b5b60","withdrawal_credentials":"0x307830303831633835323037386132616434333064343338643765616566633339363436663533383935323932353936626265313939653264376431383834616238","effective_balance":"32000000000","slashed":true,"activation_eligibility_epoch":"0","activation_epoch":"0","exit_epoch":"261","withdrawable_epoch":"8448"}}],"finalized":true,"execution_optimistic":false}` + "\n",
		},
		{
			blockID:      "finalized",
			code:         http.StatusOK,
			queryParams:  "?status=active&id=1,2,3",
			expectedResp: `{"data":[{"index":"2","status":"active_slashed","balance":"25678253779","validator":{"pubkey":"0x89ece308f9d1f0131765212deca99697b112d61f9be9a5f1f3780a51335b3ff981747a0b2ca2179b96d2c0c9024e5224","withdrawal_credentials":"0x307830303661646334613165346361626133376335346435366432343131666430646633613130326638343839613463316265353335663466643566383831306339","effective_balance":"25000000000","slashed":true,"activation_eligibility_epoch":"0","activation_epoch":"0","exit_epoch":"261","withdrawable_epoch":"8448"}},{"index":"3","status":"active_slashed","balance":"35998164834","validator":{"pubkey":"0xac9b60d5afcbd5663a8a44b7c5a02f19e9a77ab0a35bd65809bb5c67ec582c897feb04decc694b13e08587f3ff9b5b60","withdrawal_credentials":"0x307830303831633835323037386132616434333064343338643765616566633339363436663533383935323932353936626265313939653264376431383834616238","effective_balance":"32000000000","slashed":true,"activation_eligibility_epoch":"0","activation_epoch":"0","exit_epoch":"261","withdrawable_epoch":"8448"}}],"finalized":true,"execution_optimistic":false}` + "\n",
		},
		{
			blockID:      "finalized",
			code:         http.StatusOK,
			queryParams:  "?id=0xa572cbea904d67468808c8eb50a9450c9721db309128012543902d0ac358a62ae28f75bb8f1c7c42c39a8c5529bf0f4e",
			expectedResp: `{"data":[{"index":"1","status":"withdrawal_possible","balance":"20125000000","validator":{"pubkey":"0xa572cbea904d67468808c8eb50a9450c9721db309128012543902d0ac358a62ae28f75bb8f1c7c42c39a8c5529bf0f4e","withdrawal_credentials":"0x307830303166303965643330356330373637643536663162336264623235663330313239383032376638653938613865306364326463626363363630373233643762","effective_balance":"20000000000","slashed":false,"activation_eligibility_epoch":"0","activation_epoch":"0","exit_epoch":"253","withdrawable_epoch":"257"}}],"finalized":true,"execution_optimistic":false}` + "\n",
		},
		{
			blockID: "alabama",
			code:    http.StatusBadRequest,
		},
	}

	for _, c := range cases {
		t.Run(c.blockID, func(t *testing.T) {
			server := httptest.NewServer(handler.mux)
			defer server.Close()
			// Query the block in the handler with /eth/v2/beacon/blocks/{block_id}
			resp, err := http.Get(server.URL + "/eth/v1/beacon/states/" + c.blockID + "/validators" + c.queryParams)
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

func TestGetValidatorsBalances(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, postState, handler, _, _, fcu := setupTestingHandler(t, clparams.Phase0Version)

	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot
	fcu.FinalizedCheckpointVal = solid.NewCheckpointFromParameters(fcu.HeadVal, fcu.HeadSlotVal/32)

	cases := []struct {
		blockID      string
		code         int
		queryParams  string
		expectedResp string
	}{
		{
			blockID:      "0x" + common.Bytes2Hex(postRoot[:]),
			code:         http.StatusOK,
			queryParams:  "?id=1,2,3",
			expectedResp: `{"data":[{"index":"1","balance":"20125000000"},{"index":"2","balance":"25678253779"},{"index":"3","balance":"35998164834"}],"finalized":true,"execution_optimistic":false}` + "\n",
		},
		{
			blockID:      "finalized",
			code:         http.StatusOK,
			queryParams:  "?id=0xa572cbea904d67468808c8eb50a9450c9721db309128012543902d0ac358a62ae28f75bb8f1c7c42c39a8c5529bf0f4e",
			expectedResp: `{"data":[{"index":"1","balance":"20125000000"}],"finalized":true,"execution_optimistic":false}` + "\n",
		},
		{
			blockID: "alabama",
			code:    http.StatusBadRequest,
		},
	}

	for _, c := range cases {
		t.Run(c.blockID, func(t *testing.T) {
			server := httptest.NewServer(handler.mux)
			defer server.Close()
			// Query the block in the handler with /eth/v2/beacon/blocks/{block_id}
			resp, err := http.Get(server.URL + "/eth/v1/beacon/states/" + c.blockID + "/validator_balances" + c.queryParams)
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

func TestGetSingleValidator(t *testing.T) {

	// setupTestingHandler(t, clparams.Phase0Version)
	_, blocks, _, _, postState, handler, _, _, fcu := setupTestingHandler(t, clparams.Phase0Version)

	postRoot, err := postState.HashSSZ()
	require.NoError(t, err)

	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot
	fcu.FinalizedCheckpointVal = solid.NewCheckpointFromParameters(fcu.HeadVal, fcu.HeadSlotVal/32)

	cases := []struct {
		blockID      string
		code         int
		validatorIdx string
		expectedResp string
	}{
		{
			blockID:      "0x" + common.Bytes2Hex(postRoot[:]),
			code:         http.StatusOK,
			validatorIdx: "1",
			expectedResp: `{"data":{"index":"1","status":"withdrawal_possible","balance":"20125000000","validator":{"pubkey":"0xa572cbea904d67468808c8eb50a9450c9721db309128012543902d0ac358a62ae28f75bb8f1c7c42c39a8c5529bf0f4e","withdrawal_credentials":"0x307830303166303965643330356330373637643536663162336264623235663330313239383032376638653938613865306364326463626363363630373233643762","effective_balance":"20000000000","slashed":false,"activation_eligibility_epoch":"0","activation_epoch":"0","exit_epoch":"253","withdrawable_epoch":"257"}},"finalized":true,"execution_optimistic":false}` + "\n",
		},
		{
			blockID:      "finalized",
			code:         http.StatusOK,
			validatorIdx: "0xa572cbea904d67468808c8eb50a9450c9721db309128012543902d0ac358a62ae28f75bb8f1c7c42c39a8c5529bf0f4e",
			expectedResp: `{"data":{"index":"1","status":"withdrawal_possible","balance":"20125000000","validator":{"pubkey":"0xa572cbea904d67468808c8eb50a9450c9721db309128012543902d0ac358a62ae28f75bb8f1c7c42c39a8c5529bf0f4e","withdrawal_credentials":"0x307830303166303965643330356330373637643536663162336264623235663330313239383032376638653938613865306364326463626363363630373233643762","effective_balance":"20000000000","slashed":false,"activation_eligibility_epoch":"0","activation_epoch":"0","exit_epoch":"253","withdrawable_epoch":"257"}},"finalized":true,"execution_optimistic":false}` + "\n",
		},
		{
			blockID:      "alabama",
			code:         http.StatusBadRequest,
			validatorIdx: "3",
		},
	}

	for _, c := range cases {
		t.Run(c.blockID, func(t *testing.T) {
			server := httptest.NewServer(handler.mux)
			defer server.Close()
			// Query the block in the handler with /eth/v2/beacon/blocks/{block_id}
			resp, err := http.Get(server.URL + "/eth/v1/beacon/states/" + c.blockID + "/validators/" + c.validatorIdx)
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
