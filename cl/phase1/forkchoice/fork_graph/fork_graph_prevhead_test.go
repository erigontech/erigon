package fork_graph

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
)

func decodedAnchor(t *testing.T) *state.CachingBeaconState {
	t.Helper()
	s := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(s, anchor, int(clparams.Phase0Version)))
	return s
}

func TestPreviousHeadFastPathUsesAuthoritativePriorRoot(t *testing.T) {
	manager := synced_data.NewSyncedDataManager(&clparams.MainnetBeaconConfig, true)
	fg, err := NewForkGraphDisk(decodedAnchor(t), manager, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	graph := fg.(*forkGraphDisk)

	first := decodedAnchor(t)
	firstHeader := first.LatestBlockHeader()
	firstHeader.Root = common.Hash{}
	first.SetLatestBlockHeader(&firstHeader)
	first.SetPreviousStateRoot(common.Hash{0xaa, 0xbb, 0xcc})
	firstRoot, err := first.BlockRoot()
	require.NoError(t, err)

	recomputable := decodedAnchor(t)
	recomputableHeader := recomputable.LatestBlockHeader()
	recomputableHeader.Root = common.Hash{}
	recomputable.SetLatestBlockHeader(&recomputableHeader)
	recomputedRoot, err := recomputable.BlockRoot()
	require.NoError(t, err)
	require.NotEqual(t, recomputedRoot, firstRoot, "fixture must make previousStateRoot authoritative, not recomputable")

	require.NoError(t, manager.OnHeadStateWithBlockRoot(first, firstRoot))

	second := decodedAnchor(t)
	require.NoError(t, second.SetSlot(first.Slot()+1))
	secondRoot, err := second.BlockRoot()
	require.NoError(t, err)
	require.NotEqual(t, firstRoot, secondRoot)
	require.NoError(t, manager.OnHeadStateWithBlockRoot(second, secondRoot))

	out, ok, err := graph.useCachedStateIfPossible(firstRoot, nil)
	require.NoError(t, err)
	require.True(t, ok, "previous-head fast path must resolve the authoritative prior root")
	require.NotNil(t, out)
}
