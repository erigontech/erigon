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
// reaches f.blocks, which is the only source Prune walks. Any peer can
// therefore grow the map without bound by replaying below-anchor blocks.
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

// MarkHeaderAsInvalid has no slot of its own, so the entry must survive until a
// prune passes the header's slot rather than being dropped on the next prune.
func TestForkGraphKeepsMarkedInvalidHeaderUntilItsSlotIsPruned(t *testing.T) {
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

func countSyncMap(m *sync.Map) int {
	count := 0
	m.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}
