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
	missingJustified    map[common.Hash]bool
	missingFinalized    map[common.Hash]bool
	finalizedCheckpoint solid.Checkpoint
}

func (g *forkNodesTestGraph) GetCurrentJustifiedCheckpoint(root common.Hash) (solid.Checkpoint, bool) {
	if g.missingJustified[root] {
		return solid.Checkpoint{}, false
	}
	return g.headSnapshotForkGraph.GetCurrentJustifiedCheckpoint(root)
}

func (g *forkNodesTestGraph) GetFinalizedCheckpoint(root common.Hash) (solid.Checkpoint, bool) {
	if g.missingFinalized[root] {
		return solid.Checkpoint{}, false
	}
	return g.finalizedCheckpoint, true
}

func newForkNodesTestStore(t *testing.T) (*ForkChoiceStore, *forkNodesTestGraph, common.Hash) {
	t.Helper()
	f, baseGraph := newHeadSnapshotTestStore()
	graph := &forkNodesTestGraph{headSnapshotForkGraph: baseGraph}
	f.forkGraph = graph
	var err error
	f.eth2Roots, err = lru.New[common.Hash, common.Hash](4)
	require.NoError(t, err)
	checkpoint := f.JustifiedCheckpoint()
	f.checkpointStates.Store(checkpoint, &checkpointState{
		beaconConfig: f.beaconCfg, validatorSetSize: 1,
		balances: []uint64{32}, actives: []byte{1}, slasheds: []byte{0},
	})
	f.setLatestMessage(0, LatestMessage{Slot: 12, Root: checkpoint.Root})
	return f, graph, checkpoint.Root
}

func addForkNodesTestBlock(f *ForkChoiceStore, graph *forkNodesTestGraph, root, parent common.Hash, slot uint64) {
	block := cltypes.NewSignedBeaconBlock(f.beaconCfg, clparams.GloasVersion)
	block.Block.Slot = slot
	block.Block.ParentRoot = parent
	graph.headers[root] = block.SignedBeaconBlockHeader().Header
	graph.blocks[root] = block
}

func TestForkNodesSwitchesWeightsAtGloasBoundary(t *testing.T) {
	f, _, root := newForkNodesTestStore(t)
	f.beaconCfg.GloasForkEpoch = 1
	f.beaconCfg.InitializeForkSchedule()
	f.weights = map[common.Hash]uint64{root: 17}
	checkpoint := f.JustifiedCheckpoint()
	f.unrealizedJustifications.Store(root, checkpoint)

	for _, test := range []struct {
		name   string
		slot   uint64
		weight uint64
	}{
		{name: "first Gloas slot", slot: 32, weight: 32},
		{name: "last pre-Gloas slot", slot: 31, weight: 17},
	} {
		t.Run(test.name, func(t *testing.T) {
			f.timeMs.Store(test.slot * f.beaconCfg.SecondsPerSlot * 1000)

			require.Equal(t, []ForkNode{{
				Slot: 11, BlockRoot: root, Weight: test.weight, Validity: "valid",
			}}, f.ForkNodes())
		})
	}
}

func TestForkNodesUsesStoreCheckpointsForAnchor(t *testing.T) {
	f, graph, anchorRoot := newForkNodesTestStore(t)
	graph.anchorRoot = anchorRoot
	viableRoot, missingCheckpointsRoot := common.Hash{0xbb}, common.Hash{0xcc}
	for i, root := range []common.Hash{viableRoot, missingCheckpointsRoot} {
		addForkNodesTestBlock(f, graph, root, anchorRoot, uint64(97+i))
		f.updateChildren(11, anchorRoot, root)
	}
	graph.missingJustified = map[common.Hash]bool{anchorRoot: true, missingCheckpointsRoot: true}
	graph.missingFinalized = map[common.Hash]bool{anchorRoot: true}
	oldCheckpoint := f.JustifiedCheckpoint()
	cs, ok := f.checkpointStates.Load(oldCheckpoint)
	require.True(t, ok)
	justified := solid.Checkpoint{Epoch: 3, Root: anchorRoot}
	finalized := solid.Checkpoint{Epoch: 2, Root: anchorRoot}
	graph.currentJustified = justified
	graph.finalizedCheckpoint = finalized
	f.justifiedCheckpoint.Store(justified)
	f.finalizedCheckpoint.Store(finalized)
	f.checkpointStates.Store(justified, cs)
	f.timeMs.Store(98 * f.beaconCfg.SecondsPerSlot * 1000)

	want := []ForkNode{
		{Slot: 11, BlockRoot: anchorRoot, JustifiedEpoch: 3, FinalizedEpoch: 2, Weight: 32, Validity: "valid"},
		{Slot: 97, BlockRoot: viableRoot, ParentRoot: anchorRoot, JustifiedEpoch: 3, FinalizedEpoch: 2, Validity: "valid"},
	}
	require.Equal(t, want, f.ForkNodes())

	delete(graph.missingJustified, missingCheckpointsRoot)
	graph.missingFinalized[missingCheckpointsRoot] = true
	require.Equal(t, want, f.ForkNodes())
}

func TestForkNodesDoesNotUseStoreCheckpointsForAncestorAnchor(t *testing.T) {
	f, graph, anchorRoot := newForkNodesTestStore(t)
	graph.anchorRoot = anchorRoot
	f.beaconCfg.GloasForkEpoch = 100
	f.beaconCfg.InitializeForkSchedule()
	f.timeMs.Store(65 * f.beaconCfg.SecondsPerSlot * 1000)

	justifiedRoot := common.Hash{0xbb}
	addForkNodesTestBlock(f, graph, justifiedRoot, anchorRoot, 64)
	justified := solid.Checkpoint{Epoch: 2, Root: justifiedRoot}
	finalized := solid.Checkpoint{Epoch: 1, Root: anchorRoot}
	graph.currentJustified = justified
	graph.finalizedCheckpoint = finalized
	graph.missingJustified = map[common.Hash]bool{anchorRoot: true}
	graph.missingFinalized = map[common.Hash]bool{anchorRoot: true}
	f.justifiedCheckpoint.Store(justified)
	f.finalizedCheckpoint.Store(finalized)

	orphanRoot := common.Hash{0xcc}
	addForkNodesTestBlock(f, graph, orphanRoot, anchorRoot, 65)
	f.weights = make(map[common.Hash]uint64)
	f.accountWeights(map[common.Hash]uint64{orphanRoot: 32}, f.weights, justifiedRoot, orphanRoot)

	require.Equal(t, []ForkNode{{
		Slot: 65, BlockRoot: orphanRoot, ParentRoot: anchorRoot,
		JustifiedEpoch: 2, FinalizedEpoch: 1, Weight: 32, Validity: "valid",
	}}, f.ForkNodes())
}

func TestForkNodesGloasExportsPreparedWeight(t *testing.T) {
	f, _, root := newForkNodesTestStore(t)
	f.checkpointStates.Store(f.JustifiedCheckpoint(), &checkpointState{
		beaconConfig: f.beaconCfg, validatorSetSize: 4,
		balances: []uint64{32, 16, 8, 4}, actives: []byte{15}, slasheds: []byte{0},
	})
	f.setLatestMessage(1, LatestMessage{Slot: 12, Root: root, PayloadPresent: true})
	f.setLatestMessage(2, LatestMessage{Slot: 11, Root: root})
	executionHash := common.Hash{0xee}
	f.eth2Roots.Add(root, executionHash)
	_, _, err := f.GetHeadNode()
	require.NoError(t, err)

	nodes := []ForkNode{{
		Slot: 11, BlockRoot: root, Weight: 56, Validity: "valid", ExecutionBlock: executionHash,
	}}
	require.Equal(t, nodes, f.ForkNodes())
	require.Empty(t, f.weights)

	f.setLatestMessage(3, LatestMessage{Slot: 12, Root: root})
	require.Equal(t, nodes, f.ForkNodes())

	f.headHash = common.Hash{}
	_, _, err = f.GetHeadNode()
	require.NoError(t, err)
	nodes[0].Weight = 60
	require.Equal(t, nodes, f.ForkNodes())
}

func TestForkNodesGloasRefreshesWeightWhenFilteredTreeBecomesEmpty(t *testing.T) {
	f, graph, root := newForkNodesTestStore(t)
	oldCheckpoint := f.JustifiedCheckpoint()
	f.checkpointStates.Delete(oldCheckpoint)
	justified := solid.Checkpoint{Epoch: 3, Root: root}
	graph.currentJustified = justified
	f.justifiedCheckpoint.Store(justified)
	f.checkpointStates.Store(justified, &checkpointState{
		beaconConfig: f.beaconCfg, validatorSetSize: 2,
		balances: []uint64{32, 16}, actives: []byte{3}, slasheds: []byte{0},
	})
	f.unrealizedJustifications.Store(root, solid.Checkpoint{Epoch: 2, Root: root})
	f.timeMs.Store(4 * f.beaconCfg.SlotsPerEpoch * f.beaconCfg.SecondsPerSlot * 1000)

	_, _, err := f.GetHeadNode()
	require.NoError(t, err)

	f.setLatestMessage(1, LatestMessage{Slot: 12, Root: root})
	f.timeMs.Store(5 * f.beaconCfg.SlotsPerEpoch * f.beaconCfg.SecondsPerSlot * 1000)
	f.headHash = common.Hash{}

	require.Equal(t, []ForkNode{{
		Slot: 11, BlockRoot: root, JustifiedEpoch: 3, Weight: 48, Validity: "valid",
	}}, f.ForkNodes())
}

func TestForkNodesGloasIncludesProposerBoost(t *testing.T) {
	f, graph, root := newForkNodesTestStore(t)
	rootA, rootB := common.Hash{0xbb}, common.Hash{0xcc}
	for _, childRoot := range []common.Hash{rootA, rootB} {
		addForkNodesTestBlock(f, graph, childRoot, root, 13)
		f.updateChildren(11, root, childRoot)
	}
	f.timeMs.Store(13 * f.beaconCfg.SecondsPerSlot * 1000)
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
	f, graph, root := newForkNodesTestStore(t)
	parentRoot := common.Hash{0x99}
	addForkNodesTestBlock(f, graph, parentRoot, common.Hash{}, 9)
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
	f, graph, root := newForkNodesTestStore(t)
	f.checkpointStates.Store(f.JustifiedCheckpoint(), &checkpointState{
		beaconConfig: f.beaconCfg, validatorSetSize: 2,
		balances: []uint64{32, 3168}, actives: []byte{3}, slasheds: []byte{0},
		unslashedActiveBalance: 3200,
	})
	_, _, err := f.GetHeadNode()
	require.NoError(t, err)

	childRoot := common.Hash{0xbb}
	addForkNodesTestBlock(f, graph, childRoot, root, 13)
	f.updateChildren(11, root, childRoot)
	f.timeMs.Store(13 * f.beaconCfg.SecondsPerSlot * 1000)
	f.proposerBoostRoot.Store(childRoot)

	require.Equal(t, []ForkNode{{
		Slot: 11, BlockRoot: root, Weight: 72, Validity: "valid",
	}}, f.ForkNodes())
}

func TestForkNodesGloasIgnoresBoostOutsideSubtree(t *testing.T) {
	f, graph, root := newForkNodesTestStore(t)
	commonRoot, parentRoot, boostRoot := common.Hash{0x99}, common.Hash{0xbb}, common.Hash{0xcc}
	for _, node := range []struct {
		root, parent common.Hash
		slot         uint64
	}{
		{root: commonRoot, slot: 9},
		{root: parentRoot, parent: commonRoot, slot: 10},
		{root: boostRoot, parent: parentRoot, slot: 11},
	} {
		addForkNodesTestBlock(f, graph, node.root, node.parent, node.slot)
	}
	graph.blocks[root].Block.ParentRoot = commonRoot
	graph.headers[root].ParentRoot = commonRoot
	f.timeMs.Store(11 * f.beaconCfg.SecondsPerSlot * 1000)
	balances := make([]uint64, 16)
	for i := range balances {
		balances[i] = 32
		f.setLatestMessage(uint64(i), LatestMessage{Slot: 11, Root: root})
	}
	f.checkpointStates.Store(f.JustifiedCheckpoint(), &checkpointState{
		beaconConfig: f.beaconCfg, validatorSetSize: len(balances), balances: balances,
		actives: []byte{0xff, 0xff}, slasheds: []byte{0, 0}, unslashedActiveBalance: 512,
	})
	_, _, err := f.GetHeadNode()
	require.NoError(t, err)
	withoutBoost := f.ForkNodes()
	f.proposerBoostRoot.Store(boostRoot)
	blockReads := 0
	graph.onGetBlock = func() { blockReads++ }

	require.Equal(t, withoutBoost, f.ForkNodes())
	require.LessOrEqual(t, blockReads, 3)
}

func TestForkNodesGloasBoostRequiresBlockAncestry(t *testing.T) {
	f, graph, root := newForkNodesTestStore(t)
	f.checkpointStates.Store(f.JustifiedCheckpoint(), &checkpointState{
		beaconConfig: f.beaconCfg, validatorSetSize: 2,
		balances: []uint64{32, 3168}, actives: []byte{3}, slasheds: []byte{0},
		unslashedActiveBalance: 3200,
	})
	_, _, err := f.GetHeadNode()
	require.NoError(t, err)

	boostRoot := common.Hash{0xbb}
	graph.headers[boostRoot] = &cltypes.BeaconBlockHeader{Slot: 13, ParentRoot: root}
	f.timeMs.Store(13 * f.beaconCfg.SecondsPerSlot * 1000)
	f.proposerBoostRoot.Store(boostRoot)
	require.False(t, f.isAncestor(
		ForkChoiceNode{Root: boostRoot, PayloadStatus: cltypes.PayloadStatusPending},
		ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusPending},
	))

	require.Equal(t, []ForkNode{{
		Slot: 11, BlockRoot: root, Weight: 32, Validity: "valid",
	}}, f.ForkNodes())
}

func TestForkNodesGloasRequiresCurrentCheckpointState(t *testing.T) {
	for _, test := range []struct {
		name     string
		addChild bool
		want     []ForkNode
	}{
		{name: "unprepared", want: []ForkNode{{Slot: 11, BlockRoot: common.Hash{0xaa}, Weight: 32, Validity: "valid"}}},
		{name: "missing state"},
		{name: "changed root"},
		{name: "changed epoch"},
		{name: "pruned with invalidated head", addChild: true, want: []ForkNode{
			{Slot: 11, BlockRoot: common.Hash{0xaa}, Weight: 32, Validity: "valid"},
			{Slot: 12, BlockRoot: common.Hash{0xbb}, ParentRoot: common.Hash{0xaa}, Validity: "valid"},
		}},
		{name: "pruned with cached head", addChild: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, graph, root := newForkNodesTestStore(t)
			if test.addChild {
				childRoot := common.Hash{0xbb}
				addForkNodesTestBlock(f, graph, childRoot, root, 12)
				f.updateChildren(11, root, childRoot)
			}
			if test.name != "unprepared" {
				_, _, err := f.GetHeadNode()
				require.NoError(t, err)
			}
			checkpoint := f.JustifiedCheckpoint()
			switch test.name {
			case "missing state":
				f.gloasWeightTree.prepare(checkpoint, nil)
			case "changed root":
				checkpoint.Root = common.Hash{0xbb}
				f.justifiedCheckpoint.Store(checkpoint)
			case "changed epoch":
				checkpoint.Epoch++
				f.justifiedCheckpoint.Store(checkpoint)
			case "pruned with invalidated head":
				f.mu.Lock()
				f.gloasWeightTree.pruneFinalized(11)
				f.headHash = common.Hash{}
				f.mu.Unlock()
			case "pruned with cached head":
				f.mu.Lock()
				f.gloasWeightTree.pruneFinalized(11)
				f.mu.Unlock()
			}

			got := f.ForkNodes()
			if test.want == nil {
				require.Empty(t, got)
			} else {
				require.Equal(t, test.want, got)
			}
		})
	}
}

func TestForkNodesGloasExcludesPreviousJustifiedSubtree(t *testing.T) {
	f, graph, root := newForkNodesTestStore(t)
	childRoot := common.Hash{0xbb}
	addForkNodesTestBlock(f, graph, childRoot, root, 12)
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
