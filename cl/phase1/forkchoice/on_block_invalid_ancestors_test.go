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
	"errors"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/optimistic"
	"github.com/erigontech/erigon/common"
)

type invalidTrackingGraph struct {
	*getFinalizedExecutionHashForkGraph
	invalid  map[common.Hash]bool
	accepted map[common.Hash]bool
}

func (g *invalidTrackingGraph) MarkHeaderAsInvalid(root common.Hash) { g.invalid[root] = true }
func (g *invalidTrackingGraph) IsBlockInvalid(root common.Hash) bool { return g.invalid[root] }
func (g *invalidTrackingGraph) PayloadAccepted(root common.Hash) (bool, bool) {
	verified, ok := g.accepted[root]
	return verified, ok
}

// chain: unstored(0) <- valid(1) <- optimistic(2) <- optimistic(3); root n carries payload {0xe, n}.
func invalidAncestorsFixture(t *testing.T) (*ForkChoiceStore, *invalidTrackingGraph) {
	t.Helper()
	cfg := &clparams.MainnetBeaconConfig
	graph := &invalidTrackingGraph{
		getFinalizedExecutionHashForkGraph: &getFinalizedExecutionHashForkGraph{
			blocks:  map[common.Hash]*cltypes.SignedBeaconBlock{},
			headers: map[common.Hash]*cltypes.BeaconBlockHeader{},
		},
		invalid:  map[common.Hash]bool{},
		accepted: map[common.Hash]bool{},
	}
	store := &ForkChoiceStore{beaconCfg: cfg, forkGraph: graph, optimisticStore: optimistic.NewOptimisticStore()}
	store.finalizedCheckpoint.Store(solid.Checkpoint{})
	store.justifiedCheckpoint.Store(solid.Checkpoint{})
	for n := byte(1); n <= 3; n++ {
		block := cltypes.NewSignedBeaconBlock(cfg, clparams.BellatrixVersion)
		block.Block.Slot = uint64(n)
		block.Block.ParentRoot = common.Hash{n - 1}
		block.Block.Body.ExecutionPayload.ParentHash = common.Hash{0xe, n - 1}
		block.Block.Body.ExecutionPayload.BlockHash = common.Hash{0xe, n}
		block.Block.Body.ExecutionPayload.BlockNumber = uint64(n)
		addInvalidAncestorsBlock(graph, common.Hash{n}, block)
		if n > 1 {
			require.NoError(t, store.optimisticStore.AddOptimisticCandidate(common.Hash{n}, block.Block))
		}
	}
	return store, graph
}

func addInvalidAncestorsBlock(graph *invalidTrackingGraph, root common.Hash, block *cltypes.SignedBeaconBlock) {
	graph.blocks[root] = block
	graph.headers[root] = &cltypes.BeaconBlockHeader{Slot: block.Block.Slot, ParentRoot: block.Block.ParentRoot}
}

func invalidChildOf3(store *ForkChoiceStore) *cltypes.BeaconBlock {
	block := cltypes.NewSignedBeaconBlock(store.beaconCfg, clparams.BellatrixVersion).Block
	block.Slot = 4
	block.ParentRoot = common.Hash{3}
	block.Body.ExecutionPayload.ParentHash = common.Hash{0xe, 3}
	block.Body.ExecutionPayload.BlockHash = common.Hash{0xe, 4}
	return block
}

func TestInvalidPayloadInvalidatesOptimisticAncestorsAfterLatestValidHash(t *testing.T) {
	store, graph := invalidAncestorsFixture(t)
	newPayloadErr := &execution_client.InvalidPayloadError{LatestValidHash: common.Hash{0xe, 1}, Err: errors.New("bad")}

	require.NoError(t, store.invalidateOptimisticAncestorsLocked(newPayloadErr, invalidChildOf3(store)))

	require.True(t, graph.invalid[common.Hash{2}])
	require.True(t, graph.invalid[common.Hash{3}])
	require.False(t, graph.invalid[common.Hash{1}])
	require.False(t, store.optimisticStore.IsOptimistic(common.Hash{2}))
	require.False(t, store.optimisticStore.IsOptimistic(common.Hash{3}))
}

func TestInvalidPayloadInvalidatesBackToUnstoredLatestValidBlock(t *testing.T) {
	store, graph := invalidAncestorsFixture(t)
	newPayloadErr := &execution_client.InvalidPayloadError{LatestValidHash: common.Hash{0xe, 0}}

	require.NoError(t, store.invalidateOptimisticAncestorsLocked(newPayloadErr, invalidChildOf3(store)))

	require.True(t, graph.invalid[common.Hash{1}])
	require.True(t, graph.invalid[common.Hash{2}])
	require.True(t, graph.invalid[common.Hash{3}])
}

func TestInvalidPayloadKeepsAncestorsOfAnotherExecutionParent(t *testing.T) {
	for name, parentHash := range map[string]common.Hash{
		"parent hash is the latest valid hash": {0xe, 1},
		"parent hash is unknown":               {0xe, 9},
	} {
		t.Run(name, func(t *testing.T) {
			store, graph := invalidAncestorsFixture(t)
			invalid := invalidChildOf3(store)
			invalid.Body.ExecutionPayload.ParentHash = parentHash
			newPayloadErr := &execution_client.InvalidPayloadError{LatestValidHash: common.Hash{0xe, 1}}

			require.NoError(t, store.invalidateOptimisticAncestorsLocked(newPayloadErr, invalid))

			require.Empty(t, graph.invalid)
		})
	}
}

func TestInvalidPayloadInvalidatesAncestorsDroppedFromOptimisticStore(t *testing.T) {
	store, graph := invalidAncestorsFixture(t)
	sibling := cltypes.NewSignedBeaconBlock(store.beaconCfg, clparams.BellatrixVersion)
	sibling.Block.Slot = 9
	sibling.Block.Body.ExecutionPayload.BlockHash = common.Hash{0xe, 9}
	sibling.Block.Body.ExecutionPayload.BlockNumber = 9
	require.NoError(t, store.optimisticStore.AddOptimisticCandidate(common.Hash{9}, sibling.Block))
	require.NoError(t, store.optimisticStore.ValidateBlock(common.Hash{9}, sibling.Block))
	require.False(t, store.optimisticStore.IsOptimistic(common.Hash{2}))
	newPayloadErr := &execution_client.InvalidPayloadError{LatestValidHash: common.Hash{0xe, 1}}

	require.NoError(t, store.invalidateOptimisticAncestorsLocked(newPayloadErr, invalidChildOf3(store)))

	require.True(t, graph.invalid[common.Hash{2}])
	require.True(t, graph.invalid[common.Hash{3}])
	require.False(t, graph.invalid[common.Hash{1}])
}

func TestInvalidPayloadStopsAtValidatedAncestor(t *testing.T) {
	for name, markValidated := range map[string]func(*ForkChoiceStore, *invalidTrackingGraph){
		"status cache": func(store *ForkChoiceStore, _ *invalidTrackingGraph) {
			var err error
			store.payloadStatusByRoot, err = lru.New[common.Hash, execution_client.PayloadStatus](8)
			require.NoError(t, err)
			store.payloadStatusByRoot.Add(common.Hash{2}, execution_client.PayloadStatusValidated)
		},
		"fork graph": func(_ *ForkChoiceStore, graph *invalidTrackingGraph) {
			graph.accepted[common.Hash{2}] = true
		},
	} {
		t.Run(name, func(t *testing.T) {
			store, graph := invalidAncestorsFixture(t)
			markValidated(store, graph)
			newPayloadErr := &execution_client.InvalidPayloadError{LatestValidHash: common.Hash{0xe, 1}}

			require.NoError(t, store.invalidateOptimisticAncestorsLocked(newPayloadErr, invalidChildOf3(store)))

			require.Empty(t, graph.invalid)
		})
	}
}

func TestInvalidPayloadStopsAtFinalizedSlot(t *testing.T) {
	store, graph := invalidAncestorsFixture(t)
	store.finalizedCheckpoint.Store(solid.Checkpoint{Epoch: 1})
	newPayloadErr := &execution_client.InvalidPayloadError{LatestValidHash: common.Hash{0xe, 1}}

	require.NoError(t, store.invalidateOptimisticAncestorsLocked(newPayloadErr, invalidChildOf3(store)))

	require.Empty(t, graph.invalid)
}

func TestInvalidPayloadKeepsJustifiedRootAndItsAncestors(t *testing.T) {
	store, graph := invalidAncestorsFixture(t)
	store.justifiedCheckpoint.Store(solid.Checkpoint{Root: common.Hash{2}})
	newPayloadErr := &execution_client.InvalidPayloadError{LatestValidHash: common.Hash{0xe, 0}}

	require.NoError(t, store.invalidateOptimisticAncestorsLocked(newPayloadErr, invalidChildOf3(store)))

	require.True(t, graph.invalid[common.Hash{3}])
	require.False(t, graph.invalid[common.Hash{2}])
	require.False(t, graph.invalid[common.Hash{1}])
	require.True(t, store.optimisticStore.IsOptimistic(common.Hash{2}))
}

func TestInvalidPayloadKeepsAncestorsOfJustifiedRootOnAnotherBranch(t *testing.T) {
	store, graph := invalidAncestorsFixture(t)
	justified := cltypes.NewSignedBeaconBlock(store.beaconCfg, clparams.BellatrixVersion)
	justified.Block.Slot = 3
	justified.Block.ParentRoot = common.Hash{2}
	addInvalidAncestorsBlock(graph, common.Hash{0x32}, justified)
	require.NoError(t, store.optimisticStore.AddOptimisticCandidate(common.Hash{0x32}, justified.Block))
	store.justifiedCheckpoint.Store(solid.Checkpoint{Root: common.Hash{0x32}})
	newPayloadErr := &execution_client.InvalidPayloadError{LatestValidHash: common.Hash{0xe, 1}}

	require.NoError(t, store.invalidateOptimisticAncestorsLocked(newPayloadErr, invalidChildOf3(store)))

	require.True(t, graph.invalid[common.Hash{3}])
	require.False(t, graph.invalid[common.Hash{2}])
	require.True(t, store.optimisticStore.IsOptimistic(common.Hash{0x32}))
}

func TestInvalidPayloadInvalidatesBranchBelowJustifiedSlotOfAnotherBranch(t *testing.T) {
	store, graph := invalidAncestorsFixture(t)
	justified := cltypes.NewSignedBeaconBlock(store.beaconCfg, clparams.BellatrixVersion)
	justified.Block.Slot = 32
	justified.Block.ParentRoot = common.Hash{1}
	addInvalidAncestorsBlock(graph, common.Hash{0x77}, justified)
	store.justifiedCheckpoint.Store(solid.Checkpoint{Epoch: 1, Root: common.Hash{0x77}})
	newPayloadErr := &execution_client.InvalidPayloadError{LatestValidHash: common.Hash{0xe, 1}}

	require.NoError(t, store.invalidateOptimisticAncestorsLocked(newPayloadErr, invalidChildOf3(store)))

	require.True(t, graph.invalid[common.Hash{2}])
	require.True(t, graph.invalid[common.Hash{3}])
}

func TestInvalidPayloadChecksJustifiedChainInLinearTime(t *testing.T) {
	const n = 64
	store, graph := invalidAncestorsFixture(t)
	justifiedRoot := common.Hash{1}
	for i := range n {
		block := cltypes.NewSignedBeaconBlock(store.beaconCfg, clparams.BellatrixVersion)
		block.Block.Slot = uint64(2 + i)
		block.Block.ParentRoot = justifiedRoot
		justifiedRoot = common.Hash{0x40, byte(i)}
		addInvalidAncestorsBlock(graph, justifiedRoot, block)
	}
	store.justifiedCheckpoint.Store(solid.Checkpoint{Root: justifiedRoot})
	tipRoot, tipHash := common.Hash{3}, common.Hash{0xe, 3}
	for i := range n {
		block := cltypes.NewSignedBeaconBlock(store.beaconCfg, clparams.BellatrixVersion)
		block.Block.Slot = uint64(4 + i)
		block.Block.ParentRoot = tipRoot
		block.Block.Body.ExecutionPayload.ParentHash = tipHash
		tipRoot, tipHash = common.Hash{0x50, byte(i)}, common.Hash{0x5e, byte(i)}
		block.Block.Body.ExecutionPayload.BlockHash = tipHash
		addInvalidAncestorsBlock(graph, tipRoot, block)
	}
	invalid := cltypes.NewSignedBeaconBlock(store.beaconCfg, clparams.BellatrixVersion).Block
	invalid.Slot = n + 4
	invalid.ParentRoot = tipRoot
	invalid.Body.ExecutionPayload.ParentHash = tipHash
	graph.getBlockCalls, graph.getHeaderCalls = 0, 0

	require.NoError(t, store.invalidateOptimisticAncestorsLocked(&execution_client.InvalidPayloadError{LatestValidHash: common.Hash{0xe, 1}}, invalid))

	require.True(t, graph.invalid[tipRoot])
	require.Less(t, graph.getBlockCalls+graph.getHeaderCalls, 8*n)
}

func TestInvalidPayloadKeepsAncestorsUpToOptimisticLatestValidHash(t *testing.T) {
	store, graph := invalidAncestorsFixture(t)
	newPayloadErr := &execution_client.InvalidPayloadError{LatestValidHash: common.Hash{0xe, 2}}

	require.NoError(t, store.invalidateOptimisticAncestorsLocked(newPayloadErr, invalidChildOf3(store)))

	require.True(t, graph.invalid[common.Hash{3}])
	require.False(t, graph.invalid[common.Hash{2}])
}

func TestInvalidPayloadWithoutKnownLatestValidHashKeepsAncestors(t *testing.T) {
	for name, newPayloadErr := range map[string]error{
		"plain error":      errors.New("bad"),
		"unknown ancestor": &execution_client.InvalidPayloadError{LatestValidHash: common.Hash{0xff}},
	} {
		t.Run(name, func(t *testing.T) {
			store, graph := invalidAncestorsFixture(t)

			require.NoError(t, store.invalidateOptimisticAncestorsLocked(newPayloadErr, invalidChildOf3(store)))

			require.Empty(t, graph.invalid)
			require.True(t, store.optimisticStore.IsOptimistic(common.Hash{2}))
			require.True(t, store.optimisticStore.IsOptimistic(common.Hash{3}))
		})
	}
}

// justified <- a <- a1; justified <- b <- invalid <- c. Votes for invalid and its descendants must not reach b.
func TestPreGloasHeadIgnoresVotesForInvalidatedBlocks(t *testing.T) {
	f, graph, justified := newPreGloasHeadTestStore()
	a, a1, b, invalid, c := common.Hash{0x0a}, common.Hash{0x0e}, common.Hash{0x0b}, common.Hash{0x0c}, common.Hash{0x0f}
	for _, n := range []struct {
		root, parent common.Hash
		slot         uint64
	}{{a, justified.Root, 321}, {a1, a, 322}, {b, justified.Root, 321}, {invalid, b, 322}, {c, invalid, 323}} {
		block := cltypes.NewSignedBeaconBlock(f.beaconCfg, clparams.BellatrixVersion)
		block.Block.Slot = n.slot
		block.Block.ParentRoot = n.parent
		graph.blocks[n.root] = block
		graph.headers[n.root] = &cltypes.BeaconBlockHeader{Slot: n.slot, ParentRoot: n.parent}
		f.updateChildren(graph.headers[n.parent].Slot, n.parent, n.root)
	}
	graph.invalid = map[common.Hash]bool{invalid: true}
	f.headSet = map[common.Hash]struct{}{a1: {}, c: {}}
	f.timeMs.Store(324 * f.beaconCfg.SecondsPerSlot * 1000)
	f.checkpointStates.Store(justified, &checkpointState{
		beaconConfig: f.beaconCfg, validatorSetSize: 5,
		balances: []uint64{32, 32, 32, 32, 32}, actives: []byte{0b11111}, slasheds: []byte{0},
	})
	for i, root := range []common.Hash{a1, a1, b, c, c} {
		f.setLatestMessage(uint64(i), LatestMessage{Epoch: 10, Root: root})
	}

	head, _, err := f.getHead(nil, false)

	require.NoError(t, err)
	require.Equal(t, a1, head.Root)
	require.Equal(t, uint64(64), f.weights[a])
	require.Equal(t, uint64(32), f.weights[b])
}
