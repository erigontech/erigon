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

package forkchoice

import (
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

type forkNodesTestGraph struct {
	*headSnapshotForkGraph
}

func (g *forkNodesTestGraph) GetFinalizedCheckpoint(common.Hash) (solid.Checkpoint, bool) {
	return solid.Checkpoint{}, true
}

func newForkNodesTestStore(t *testing.T) (*ForkChoiceStore, common.Hash) {
	t.Helper()
	f, graph := newHeadSnapshotTestStore()
	f.forkGraph = &forkNodesTestGraph{headSnapshotForkGraph: graph}
	var err error
	f.eth2Roots, err = lru.New[common.Hash, common.Hash](4)
	require.NoError(t, err)
	checkpoint := f.JustifiedCheckpoint()
	f.checkpointStates.Store(checkpoint, &checkpointState{
		beaconConfig: f.beaconCfg, validatorSetSize: 1,
		balances: []uint64{32}, actives: []byte{1}, slasheds: []byte{0},
	})
	f.setLatestMessage(0, LatestMessage{Slot: 12, Root: checkpoint.Root})
	return f, checkpoint.Root
}

func TestForkNodesGloasExportsPreparedWeight(t *testing.T) {
	f, root := newForkNodesTestStore(t)
	f.checkpointStates.Store(f.JustifiedCheckpoint(), &checkpointState{
		beaconConfig: f.beaconCfg, validatorSetSize: 3,
		balances: []uint64{32, 16, 8}, actives: []byte{7}, slasheds: []byte{0},
	})
	f.setLatestMessage(1, LatestMessage{Slot: 12, Root: root, PayloadPresent: true})
	f.setLatestMessage(2, LatestMessage{Slot: 11, Root: root})
	executionHash := common.Hash{0xee}
	f.eth2Roots.Add(root, executionHash)
	_, _, err := f.GetHeadNode()
	require.NoError(t, err)

	require.Equal(t, []ForkNode{{
		Slot: 11, BlockRoot: root, Weight: 56, Validity: "valid", ExecutionBlock: executionHash,
	}}, f.ForkNodes())
	require.Empty(t, f.weights)
}

func TestForkNodesGloasExcludesPreviousJustifiedSubtree(t *testing.T) {
	f, root := newForkNodesTestStore(t)
	graph := f.forkGraph.(*forkNodesTestGraph)
	childRoot := common.Hash{0xbb}
	child := cltypes.NewSignedBeaconBlock(f.beaconCfg, clparams.GloasVersion)
	child.Block.Slot = 12
	child.Block.ParentRoot = root
	graph.headers[childRoot] = child.SignedBeaconBlockHeader().Header
	graph.blocks[childRoot] = child
	f.updateChildren(11, root, childRoot)
	_, _, err := f.GetHeadNode()
	require.NoError(t, err)

	cs, err := f.getCheckpointState(f.JustifiedCheckpoint())
	require.NoError(t, err)
	checkpoint := solid.Checkpoint{Root: childRoot}
	f.justifiedCheckpoint.Store(checkpoint)
	f.checkpointStates.Store(checkpoint, cs)
	graph.currentJustified = checkpoint
	f.headHash = common.Hash{}
	_, _, err = f.GetHeadNode()
	require.NoError(t, err)

	require.Equal(t, []ForkNode{{
		Slot: 12, BlockRoot: childRoot, ParentRoot: root, Validity: "valid",
	}}, f.ForkNodes())
}
