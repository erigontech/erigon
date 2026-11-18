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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

// ancestorChain stores a chain of blocks at slots 10, 11, ... and returns their roots. Each Gloas block's bid builds
// on its parent's payload unless its index is in skipsParentPayload.
func ancestorChain(version clparams.StateVersion, n int, skipsParentPayload ...int) (*ForkChoiceStore, *getFinalizedExecutionHashForkGraph, []common.Hash) {
	cfg := clparams.MainnetBeaconConfig
	graph := &getFinalizedExecutionHashForkGraph{
		headers: map[common.Hash]*cltypes.BeaconBlockHeader{},
		blocks:  map[common.Hash]*cltypes.SignedBeaconBlock{},
	}
	store := &ForkChoiceStore{beaconCfg: &cfg, forkGraph: graph}
	roots := make([]common.Hash, n)
	var parentRoot, parentBlockHash common.Hash
	for i := range n {
		block := cltypes.NewSignedBeaconBlock(&cfg, version)
		block.Block.Slot = 10 + uint64(i)
		block.Block.ParentRoot = parentRoot
		blockHash := common.Hash{byte(i + 1), 0xee}
		if version >= clparams.GloasVersion {
			bidParent := parentBlockHash
			if slices.Contains(skipsParentPayload, i) {
				bidParent = common.Hash{0xbe, 0xef}
			}
			block.Block.Body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
				ParentBlockHash: bidParent,
				BlockHash:       blockHash,
			}}
		}
		roots[i] = common.Hash{byte(i + 1), 0x01}
		graph.headers[roots[i]] = block.SignedBeaconBlockHeader().Header
		graph.blocks[roots[i]] = block
		parentRoot, parentBlockHash = roots[i], blockHash
	}
	return store, graph, roots
}

func TestAncestorReturnsTheParentPayloadStatusOfTheAncestor(t *testing.T) {
	store, _, roots := ancestorChain(clparams.GloasVersion, 6, 3) // block 3 skips the payload of block 2
	tip := ForkChoiceNode{Root: roots[5], PayloadStatus: cltypes.PayloadStatusFull}

	require.Equal(t, ForkChoiceNode{Root: roots[2], PayloadStatus: cltypes.PayloadStatusEmpty}, store.Ancestor(roots[5], 12))
	require.Equal(t, ForkChoiceNode{Root: roots[1], PayloadStatus: cltypes.PayloadStatusFull}, store.Ancestor(roots[5], 11))
	require.Equal(t, ForkChoiceNode{Root: roots[3], PayloadStatus: cltypes.PayloadStatusFull}, store.getAncestor(tip, 13))
}

func TestAncestorOfANodeAtOrBelowTheSlotIsTheNode(t *testing.T) {
	store, _, roots := ancestorChain(clparams.GloasVersion, 6)
	tip := ForkChoiceNode{Root: roots[5], PayloadStatus: cltypes.PayloadStatusFull}
	require.Equal(t, tip, store.getAncestor(tip, 15))
}

func TestAncestorBeforeTheFirstStoredBlockIsUnknown(t *testing.T) {
	store, _, roots := ancestorChain(clparams.GloasVersion, 6)
	require.Equal(t, ForkChoiceNode{PayloadStatus: cltypes.PayloadStatusPending}, store.Ancestor(roots[5], 9))
}

func TestAncestorOfAnUnknownRootIsUnknown(t *testing.T) {
	store, _, _ := ancestorChain(clparams.GloasVersion, 6)
	require.Equal(t, ForkChoiceNode{PayloadStatus: cltypes.PayloadStatusPending}, store.Ancestor(common.Hash{0x99}, 9))
}

func TestAncestorBeforeGloasHasAnEmptyPayloadStatus(t *testing.T) {
	store, _, roots := ancestorChain(clparams.FuluVersion, 4)
	require.Equal(t, ForkChoiceNode{Root: roots[1], PayloadStatus: cltypes.PayloadStatusEmpty}, store.Ancestor(roots[3], 11))
}

func TestAncestorReadsTwoBlocksWhateverTheDistance(t *testing.T) {
	store, graph, roots := ancestorChain(clparams.GloasVersion, 33)
	require.Equal(t, ForkChoiceNode{Root: roots[0], PayloadStatus: cltypes.PayloadStatusFull}, store.Ancestor(roots[32], 10))
	require.Equal(t, 2, graph.getBlockCalls, "only the returned ancestor and its child are read as blocks")
}

func TestAncestorReturnsTheCheckpointAnchor(t *testing.T) {
	store, graph, roots := ancestorChain(clparams.GloasVersion, 3)
	store.beaconCfg.GloasForkEpoch = 0
	anchorBid := graph.blocks[roots[0]].Block.Body.GetSignedExecutionPayloadBid().Message.Copy()
	anchorBid.Slot = 10
	store.anchorExecutionPayloadBid = anchorBid
	delete(graph.blocks, roots[0])
	graph.anchorRoot = roots[0]

	require.Equal(t, ForkChoiceNode{Root: roots[0], PayloadStatus: cltypes.PayloadStatusFull}, store.Ancestor(roots[2], 10))
	require.Equal(t, ForkChoiceNode{PayloadStatus: cltypes.PayloadStatusPending}, store.Ancestor(roots[2], 9))
}

func TestAncestorWithoutTheChildBlockIsUnknown(t *testing.T) {
	store, graph, roots := ancestorChain(clparams.GloasVersion, 6)
	delete(graph.blocks, roots[3])
	require.Equal(t, ForkChoiceNode{PayloadStatus: cltypes.PayloadStatusPending}, store.Ancestor(roots[5], 12))
}

// pruningForkGraph prunes a block and its header when that block is read, as a concurrent Prune can.
type pruningForkGraph struct {
	*getFinalizedExecutionHashForkGraph
	prune common.Hash
}

func (g *pruningForkGraph) GetBlock(root common.Hash) (*cltypes.SignedBeaconBlock, bool) {
	if root == g.prune {
		delete(g.blocks, root)
		delete(g.headers, root)
	}
	return g.getFinalizedExecutionHashForkGraph.GetBlock(root)
}

// A prune while the ancestor's bid is read gives the result after the prune, not the ancestor with a wrong status.
func TestAncestorIsNotMixedByAConcurrentPrune(t *testing.T) {
	store, graph, roots := ancestorChain(clparams.GloasVersion, 4)
	store.forkGraph = &pruningForkGraph{getFinalizedExecutionHashForkGraph: graph, prune: roots[1]}

	require.Equal(t, ForkChoiceNode{PayloadStatus: cltypes.PayloadStatusPending}, store.Ancestor(roots[3], 11))
}
