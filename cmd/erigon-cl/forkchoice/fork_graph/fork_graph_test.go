package fork_graph_test

import (
	_ "embed"
	"testing"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/forkchoice/fork_graph"
	"github.com/stretchr/testify/require"
)

//go:embed test_data/block_0xe2a37a22d208ebe969c50e9d44bb3f1f63c5404787b9c214a5f2f28fb9835feb.ssz_snappy
var block1 []byte

//go:embed test_data/block_0xbf1a9ba2d349f6b5a5095bff40bd103ae39177e36018fb1f589953b9eeb0ca9d.ssz_snappy
var block2 []byte

//go:embed test_data/anchor_state.ssz_snappy
var anchor []byte

func TestForkGraph(t *testing.T) {
	blockA, blockB, blockC := &cltypes.SignedBeaconBlock{}, &cltypes.SignedBeaconBlock{}, &cltypes.SignedBeaconBlock{}
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappyWithVersion(blockA, block1, int(clparams.Phase0Version)))
	require.NoError(t, utils.DecodeSSZSnappyWithVersion(blockB, block2, int(clparams.Phase0Version)))
	require.NoError(t, utils.DecodeSSZSnappyWithVersion(blockC, block2, int(clparams.Phase0Version)))
	require.NoError(t, utils.DecodeSSZSnappyWithVersion(anchorState, anchor, int(clparams.Phase0Version)))
	graph := fork_graph.New(anchorState)
	status, err := graph.AddChainSegment(blockA)
	require.NoError(t, err)
	// Save current state hash
	expectedStateHashPostFail, err := graph.LastState().HashSSZ()
	require.NoError(t, err)
	require.Equal(t, status, fork_graph.Success)
	status, err = graph.AddChainSegment(blockB)
	require.NoError(t, err)
	require.Equal(t, status, fork_graph.Success)
	// Try again with same should yield success
	status, err = graph.AddChainSegment(blockB)
	require.NoError(t, err)
	require.Equal(t, status, fork_graph.PreValidated)
	// Now make blockC a bad block
	blockC.Block.ProposerIndex = 81214459 // some invalid thing
	status, err = graph.AddChainSegment(blockC)
	require.NoError(t, err)
	require.Equal(t, status, fork_graph.InvalidBlock)
	haveStateHashPostFail, err := graph.LastState().HashSSZ()
	require.NoError(t, err)
	require.Equal(t, expectedStateHashPostFail, haveStateHashPostFail)
}
