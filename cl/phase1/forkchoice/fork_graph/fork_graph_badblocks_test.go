// Copyright 2026 The Erigon Authors
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

package fork_graph

import (
	"sync"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
)

// A block rejected below the anchor slot is recorded in badBlocks but never
// reaches f.blocks, so the block-keyed prune loop cannot reach it. OnBlock caps
// finalizedSlot at the anchor, so this store site is out of a peer's reach; the
// reachable one is the invalid-block path below.
func TestForkGraphPrunesBelowAnchorBadBlocks(t *testing.T) {
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(anchorState, anchor, int(clparams.Phase0Version)))
	graph, err := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	disk := graph.(*forkGraphDisk)

	const rejected = 64
	for i := range uint64(rejected) {
		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
		block.Block.Slot = anchorState.Slot()
		block.Block.ProposerIndex = i
		_, status, err := graph.AddChainSegment(block, false)
		require.NoError(t, err)
		require.Equal(t, BelowAnchor, status)
	}
	require.Equal(t, rejected, countSyncMap(&disk.badBlocks))

	require.NoError(t, graph.Prune(anchorState.Slot()+clparams.MainnetBeaconConfig.SlotsPerEpoch))
	require.Zero(t, countSyncMap(&disk.badBlocks),
		"below-anchor bad blocks must not survive a prune past their slot")
}

// MarkHeaderAsInvalid takes the slot from the stored header. With no header it
// falls back to slotUnknown, which no prune slot can pass.
func TestForkGraphKeepsMarkedInvalidRootWithUnknownSlot(t *testing.T) {
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(anchorState, anchor, int(clparams.Phase0Version)))
	graph, err := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	disk := graph.(*forkGraphDisk)

	root := common.Hash{0xaa}
	graph.MarkHeaderAsInvalid(root)
	require.Equal(t, 1, countSyncMap(&disk.badBlocks))

	require.NoError(t, graph.Prune(anchorState.Slot()+clparams.MainnetBeaconConfig.SlotsPerEpoch))
	require.Equal(t, 1, countSyncMap(&disk.badBlocks),
		"a header with no known slot must not be dropped by an unrelated prune")
}

// An invalid block is deleted from f.blocks before its root reaches badBlocks,
// so the block-keyed prune loop never sees it. This is the site a peer can
// drive, with many distinct invalid blocks off one known parent.
func TestForkGraphPrunesInvalidBlockBadBlocks(t *testing.T) {
	blockA := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	blockC := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(blockA, block1, int(clparams.Phase0Version)))
	require.NoError(t, utils.DecodeSSZSnappy(blockC, block2, int(clparams.Phase0Version)))
	require.NoError(t, utils.DecodeSSZSnappy(anchorState, anchor, int(clparams.Phase0Version)))
	graph, err := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	disk := graph.(*forkGraphDisk)

	_, status, err := graph.AddChainSegment(blockA, true)
	require.NoError(t, err)
	require.Equal(t, Success, status)

	blockC.Block.ProposerIndex = 81214459 // fails the transition, so the block is invalid
	_, status, _ = graph.AddChainSegment(blockC, true)
	require.Equal(t, InvalidBlock, status)
	require.Equal(t, 1, countSyncMap(&disk.badBlocks))

	require.NoError(t, graph.Prune(blockC.Block.Slot))
	require.Equal(t, 1, countSyncMap(&disk.badBlocks),
		"a prune at the block's own slot must not drop it")

	require.NoError(t, graph.Prune(blockC.Block.Slot+1))
	require.Zero(t, countSyncMap(&disk.badBlocks),
		"an invalid block must not survive a prune past its slot")
}

// A root marked through MarkHeaderAsInvalid whose header is stored carries that
// header's slot, so it expires by slot rather than living for the process.
func TestForkGraphPrunesMarkedInvalidHeaderBySlot(t *testing.T) {
	anchorState := state.New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(anchorState, anchor, int(clparams.Phase0Version)))
	graph, err := NewForkGraphDisk(anchorState, nil, afero.NewMemMapFs(), beacon_router_configuration.RouterConfiguration{})
	require.NoError(t, err)
	disk := graph.(*forkGraphDisk)

	anchorRoot, err := anchorState.BlockRoot()
	require.NoError(t, err)
	header, ok := graph.GetHeader(anchorRoot)
	require.True(t, ok)

	graph.MarkHeaderAsInvalid(anchorRoot)
	require.Equal(t, 1, countSyncMap(&disk.badBlocks))

	require.NoError(t, graph.Prune(header.Slot))
	require.Equal(t, 1, countSyncMap(&disk.badBlocks),
		"a prune at the header's own slot must not drop it")

	require.NoError(t, graph.Prune(header.Slot+1))
	require.Zero(t, countSyncMap(&disk.badBlocks),
		"a marked root with a known slot must not survive a prune past it")
}

func countSyncMap(m *sync.Map) int {
	count := 0
	m.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}
