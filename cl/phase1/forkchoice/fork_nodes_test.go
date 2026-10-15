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

func TestForkNodesGloasIncludesProposerBoost(t *testing.T) {
	f, root := newForkNodesTestStore(t)
	graph := f.forkGraph.(*forkNodesTestGraph)
	rootA, rootB := common.Hash{0xbb}, common.Hash{0xcc}
	for _, childRoot := range []common.Hash{rootA, rootB} {
		child := cltypes.NewSignedBeaconBlock(f.beaconCfg, clparams.GloasVersion)
		child.Block.Slot = 13
		child.Block.ParentRoot = root
		graph.headers[childRoot] = child.SignedBeaconBlockHeader().Header
		graph.blocks[childRoot] = child
		f.updateChildren(11, root, childRoot)
	}
	f.time.Store(13 * f.beaconCfg.SecondsPerSlot)
	f.checkpointStates.Store(f.JustifiedCheckpoint(), &checkpointState{
		beaconConfig: f.beaconCfg, validatorSetSize: 3,
		balances: []uint64{100, 90, 3010}, actives: []byte{7}, slasheds: []byte{0},
		unslashedActiveBalance: 3200,
	})
	f.setLatestMessage(0, LatestMessage{Slot: 13, Root: rootA})
	f.setLatestMessage(1, LatestMessage{Slot: 13, Root: rootB})
	f.proposerBoostRoot.Store(rootB)
	head, _, err := f.GetHeadNode()
	require.NoError(t, err)
	require.Equal(t, rootB, head.Root)

	nodes := f.ForkNodes()
	require.Len(t, nodes, 3)
	weights := make(map[common.Hash]uint64, len(nodes))
	for _, node := range nodes {
		weights[node.BlockRoot] = node.Weight
	}
	require.Equal(t, map[common.Hash]uint64{root: 230, rootA: 100, rootB: 130}, weights)
}

func TestForkNodesGloasDoesNotPopulateBoostCache(t *testing.T) {
	f, root := newForkNodesTestStore(t)
	graph := f.forkGraph.(*forkNodesTestGraph)
	parentRoot := common.Hash{0x99}
	parent := cltypes.NewSignedBeaconBlock(f.beaconCfg, clparams.GloasVersion)
	parent.Block.Slot = 9
	graph.blocks[parentRoot] = parent
	graph.headers[parentRoot] = parent.SignedBeaconBlockHeader().Header
	graph.blocks[root].Block.ParentRoot = parentRoot
	graph.headers[root].ParentRoot = parentRoot
	f.checkpointStates.Store(f.JustifiedCheckpoint(), &checkpointState{
		beaconConfig: f.beaconCfg, validatorSetSize: 2,
		balances: []uint64{32, 3168}, actives: []byte{3}, slasheds: []byte{0},
		unslashedActiveBalance: 3200,
	})
	f.proposerBoostRoot.Store(root)
	_, _, err := f.GetHeadNode()
	require.NoError(t, err)
	require.False(t, f.gloasWeightTree.boostKnown)

	require.Equal(t, []ForkNode{{
		Slot: 11, BlockRoot: root, ParentRoot: parentRoot, Weight: 72, Validity: "valid",
	}}, f.ForkNodes())
	require.False(t, f.gloasWeightTree.boostKnown)
}

func TestForkNodesGloasBoostArrivesAfterPreparation(t *testing.T) {
	f, root := newForkNodesTestStore(t)
	f.checkpointStates.Store(f.JustifiedCheckpoint(), &checkpointState{
		beaconConfig: f.beaconCfg, validatorSetSize: 2,
		balances: []uint64{32, 3168}, actives: []byte{3}, slasheds: []byte{0},
		unslashedActiveBalance: 3200,
	})
	_, _, err := f.GetHeadNode()
	require.NoError(t, err)

	graph := f.forkGraph.(*forkNodesTestGraph)
	childRoot := common.Hash{0xbb}
	child := cltypes.NewSignedBeaconBlock(f.beaconCfg, clparams.GloasVersion)
	child.Block.Slot = 13
	child.Block.ParentRoot = root
	graph.headers[childRoot] = child.SignedBeaconBlockHeader().Header
	graph.blocks[childRoot] = child
	f.updateChildren(11, root, childRoot)
	f.time.Store(13 * f.beaconCfg.SecondsPerSlot)
	f.proposerBoostRoot.Store(childRoot)

	require.Equal(t, []ForkNode{{
		Slot: 11, BlockRoot: root, Weight: 72, Validity: "valid",
	}}, f.ForkNodes())
}

func TestForkNodesGloasRequiresCurrentCheckpointState(t *testing.T) {
	for _, test := range []string{"unprepared", "missing state", "changed root", "changed epoch"} {
		t.Run(test, func(t *testing.T) {
			f, _ := newForkNodesTestStore(t)
			if test != "unprepared" {
				_, _, err := f.GetHeadNode()
				require.NoError(t, err)
			}
			checkpoint := f.JustifiedCheckpoint()
			switch test {
			case "missing state":
				f.gloasWeightTree.prepare(checkpoint, nil)
			case "changed root":
				checkpoint.Root = common.Hash{0xbb}
				f.justifiedCheckpoint.Store(checkpoint)
			case "changed epoch":
				checkpoint.Epoch++
				f.justifiedCheckpoint.Store(checkpoint)
			}

			require.Empty(t, f.ForkNodes())
		})
	}
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
