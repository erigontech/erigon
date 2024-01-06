package handler

import (
	"io"
	"net/http/httptest"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/phase1/forkchoice"
	"github.com/stretchr/testify/require"
)

func TestGetHeads(t *testing.T) {
	// find server
	_, _, _, _, p, handler, _, sm, fcu := setupTestingHandler(t, clparams.Phase0Version)
	sm.OnHeadState(p)
	s, cancel := sm.HeadState()
	s.SetSlot(789274827847783)
	cancel()

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

func TestGetForkchoice(t *testing.T) {
	// find server
	_, _, _, _, p, handler, _, sm, fcu := setupTestingHandler(t, clparams.Phase0Version)
	sm.OnHeadState(p)
	s, cancel := sm.HeadState()
	s.SetSlot(789274827847783)
	cancel()

	fcu.HeadSlotVal = 128
	fcu.HeadVal = libcommon.Hash{1, 2, 3}
	server := httptest.NewServer(handler.mux)
	defer server.Close()

	fcu.WeightsMock = []forkchoice.ForkNode{
		{
			BlockRoot:  libcommon.Hash{1, 2, 3},
			ParentRoot: libcommon.Hash{1, 2, 3},
			Slot:       128,
			Weight:     1,
		},
		{
			BlockRoot:  libcommon.Hash{1, 2, 2, 4, 5, 3},
			ParentRoot: libcommon.Hash{1, 2, 5},
			Slot:       128,
			Weight:     2,
		},
	}

	fcu.FinalizedCheckpointVal = solid.NewCheckpointFromParameters(libcommon.Hash{1, 2, 3}, 1)
	fcu.JustifiedCheckpointVal = solid.NewCheckpointFromParameters(libcommon.Hash{1, 2, 3}, 2)

	// get heads
	resp, err := server.Client().Get(server.URL + "/eth/v1/debug/fork_choice")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, 200, resp.StatusCode)
	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, `{"finalized_checkpoint":{"epoch":"1","root":"0x0102030000000000000000000000000000000000000000000000000000000000"},"fork_choice_nodes":[{"slot":"128","block_root":"0x0102030000000000000000000000000000000000000000000000000000000000","parent_root":"0x0102030000000000000000000000000000000000000000000000000000000000","justified_epoch":"0","finalized_epoch":"0","weight":"1","validity":"","execution_block_hash":"0x0000000000000000000000000000000000000000000000000000000000000000"},{"slot":"128","block_root":"0x0102020405030000000000000000000000000000000000000000000000000000","parent_root":"0x0102050000000000000000000000000000000000000000000000000000000000","justified_epoch":"0","finalized_epoch":"0","weight":"2","validity":"","execution_block_hash":"0x0000000000000000000000000000000000000000000000000000000000000000"}],"justified_checkpoint":{"epoch":"2","root":"0x0102030000000000000000000000000000000000000000000000000000000000"}}`+"\n", string(out))
}
