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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
)

func newGloasWeightTreeTestStore() *ForkChoiceStore {
	f := &ForkChoiceStore{
		beaconCfg:      &clparams.MainnetBeaconConfig,
		latestMessages: newLatestMessagesStore(16),
	}
	f.justifiedCheckpoint.Store(solid.Checkpoint{})
	f.gloasWeightTree = newGloasWeightTree(f)
	return f
}

func TestPreGloasDoesNotDirtyGloasWeightTree(t *testing.T) {
	f := newGloasWeightTreeTestStore()

	att := &solid.Attestation{
		Data: &solid.AttestationData{
			Slot:            64,
			BeaconBlockRoot: common.HexToHash("0xbeef"),
			Target:          solid.Checkpoint{Epoch: 2, Root: common.HexToHash("0xaaaa")},
		},
	}
	f.updateLatestMessagesPreGloas(att, []uint64{1, 2, 3})

	msg, has := f.getLatestMessage(2)
	require.True(t, has)
	require.Equal(t, att.Data.BeaconBlockRoot, msg.Root)
	require.Empty(t, f.gloasWeightTree.dirty)
}

func TestPreGloasEquivocationDoesNotDirtyGloasWeightTree(t *testing.T) {
	f := newGloasWeightTreeTestStore()

	f.setUnequivocating(4)

	require.True(t, f.isUnequivocating(4))
	require.Empty(t, f.gloasWeightTree.dirty)
}

func TestEquivocationAfterBaselineDirtiesWeightTree(t *testing.T) {
	f := newGloasWeightTreeTestStore()
	f.gloasWeightTree.state = &checkpointState{}

	f.setUnequivocating(4)

	require.True(t, f.isUnequivocating(4))
	require.Contains(t, f.gloasWeightTree.dirty, uint64(4))
}

func TestGloasMarksDirtyWeightTree(t *testing.T) {
	f := newGloasWeightTreeTestStore()
	f.gloasWeightTree.state = &checkpointState{}

	att := &solid.Attestation{
		Data: &solid.AttestationData{
			Slot:            64,
			BeaconBlockRoot: common.HexToHash("0xbeef"),
			Target:          solid.Checkpoint{Epoch: 2, Root: common.HexToHash("0xaaaa")},
		},
	}
	f.updateLatestMessagesGloas(att, []uint64{1, 2, 3})

	require.Contains(t, f.gloasWeightTree.dirty, uint64(1))
	require.Contains(t, f.gloasWeightTree.dirty, uint64(2))
	require.Contains(t, f.gloasWeightTree.dirty, uint64(3))
}

// Before the first prepare there is no applied baseline to delta against, so
// marks are dropped; the first prepare's full rebuild covers every validator.
func TestGloasMarksBeforeBaselineAreDropped(t *testing.T) {
	f := newGloasWeightTreeTestStore()

	att := &solid.Attestation{
		Data: &solid.AttestationData{
			Slot:            64,
			BeaconBlockRoot: common.HexToHash("0xbeef"),
			Target:          solid.Checkpoint{Epoch: 2, Root: common.HexToHash("0xaaaa")},
		},
	}
	f.updateLatestMessagesGloas(att, []uint64{1, 2, 3})

	require.Empty(t, f.gloasWeightTree.dirty)
}

// Latest messages can outgrow the justified checkpoint's validator registry;
// vote counting must skip those indices instead of panicking.
func TestComputeVotesSkipsMessagesBeyondJustifiedRegistry(t *testing.T) {
	f := newGloasWeightTreeTestStore()
	f.proposerBoostRoot.Store(common.Hash{})
	f.latestMessages.set(20, LatestMessage{Root: common.HexToHash("0x01"), Slot: 5})
	cs := &checkpointState{
		validatorSetSize: 1,
		actives:          make([]byte, 1),
		slasheds:         make([]byte, 1),
		balances:         []uint64{32_000_000_000},
	}

	votes := f.computeVotes(solid.Checkpoint{}, cs, nil)

	require.Empty(t, votes)
}

func decodeDiffBlock(t *testing.T, enc []byte) (*cltypes.SignedBeaconBlock, common.Hash) {
	t.Helper()
	b := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	require.NoError(t, utils.DecodeSSZSnappy(b, enc, int(clparams.AltairVersion)))
	root, err := b.Block.HashSSZ()
	require.NoError(t, err)
	return b, root
}

func TestGloasWeightTreePrepareIsIdempotent(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	f.mu.Lock()
	defer f.mu.Unlock()

	tree := f.gloasWeightTree.prepare(justified, cs)
	root := justified.Root
	first := tree.GetAttestationScore(ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusPending})
	tree = f.gloasWeightTree.prepare(justified, cs)
	second := tree.GetAttestationScore(ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusPending})

	require.Equal(t, first, second)
	require.NotZero(t, first)
	require.Empty(t, f.gloasWeightTree.dirty)
}

func TestGloasWeightTreePayloadStatusChangeSkipsDirectRebuild(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	f.mu.Lock()
	defer f.mu.Unlock()

	f.gloasWeightTree.prepare(justified, cs)
	children := f.gloasWeightTree.nodes[justified.Root].children
	require.NotEmpty(t, children)

	childNode := f.gloasWeightTree.nodes[children[0]]
	actualStatus := childNode.parentPayloadStatus
	childNode.parentPayloadStatus = cltypes.PayloadStatusPending
	if actualStatus == cltypes.PayloadStatusPending {
		childNode.parentPayloadStatus = cltypes.PayloadStatusEmpty
	}

	f.gloasWeightTree.ensureTopology(justified.Root)
	require.Equal(t, actualStatus, childNode.parentPayloadStatus)
	require.False(t, f.gloasWeightTree.allDirty)
}

func TestGloasWeightTreeEquivocationDeltaMatchesFullScan(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	f.mu.Lock()
	defer f.mu.Unlock()

	tree := f.gloasWeightTree.prepare(justified, cs)
	node := ForkChoiceNode{Root: justified.Root, PayloadStatus: cltypes.PayloadStatusPending}
	require.Equal(t, NewWeightStore(f).GetAttestationScore(node), tree.GetAttestationScore(node))

	var validatorIndex uint64
	found := false
	for i, applied := range f.gloasWeightTree.applied {
		if applied.set && f.isAncestor(f.getSupportedNode(applied.message), node) {
			validatorIndex = uint64(i)
			found = true
			break
		}
	}
	require.True(t, found)

	f.setUnequivocating(validatorIndex)
	require.Contains(t, f.gloasWeightTree.dirty, validatorIndex)
	tree = f.gloasWeightTree.prepare(justified, cs)

	require.Equal(t, NewWeightStore(f).GetAttestationScore(node), tree.GetAttestationScore(node))
}

func TestGloasWeightTreeFirstPrepareIncludesPreReadyEquivocation(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	f.mu.Lock()
	defer f.mu.Unlock()

	node := ForkChoiceNode{Root: justified.Root, PayloadStatus: cltypes.PayloadStatusPending}
	var validatorIndex uint64
	found := false
	for i := 0; i < f.latestMessages.latestMessagesCount(); i++ {
		message, has := f.latestMessages.get(i)
		if has && message != (LatestMessage{}) && f.isAncestor(f.getSupportedNode(message), node) {
			validatorIndex = uint64(i)
			found = true
			break
		}
	}
	require.True(t, found)

	f.setUnequivocating(validatorIndex)
	require.Empty(t, f.gloasWeightTree.dirty)

	tree := f.gloasWeightTree.prepare(justified, cs)

	require.Equal(t, NewWeightStore(f).GetAttestationScore(node), tree.GetAttestationScore(node))
	require.Empty(t, f.gloasWeightTree.dirty)
}

func TestGloasWeightTreeRebuildsOnCheckpointStateChange(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	f.mu.Lock()
	defer f.mu.Unlock()

	tree := f.gloasWeightTree.prepare(justified, cs)
	node := ForkChoiceNode{Root: justified.Root, PayloadStatus: cltypes.PayloadStatusPending}
	first := tree.GetAttestationScore(node)
	require.NotZero(t, first)

	next := *cs
	next.balances = append([]uint64(nil), cs.balances...)
	for i, applied := range f.gloasWeightTree.applied {
		if applied.set {
			next.balances[i] += 1
			break
		}
	}
	tree = f.gloasWeightTree.prepare(justified, &next)

	require.Greater(t, tree.GetAttestationScore(node), first)
}

func TestGloasWeightTreeNilCheckpointStateReturnsZero(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	f.mu.Lock()
	defer f.mu.Unlock()

	node := ForkChoiceNode{Root: justified.Root, PayloadStatus: cltypes.PayloadStatusPending}
	tree := f.gloasWeightTree.prepare(justified, cs)
	require.NotZero(t, tree.GetAttestationScore(node))

	tree = f.gloasWeightTree.prepare(justified, nil)

	require.Zero(t, tree.GetAttestationScore(node))
	require.Zero(t, tree.GetWeight(node))
}

func TestGloasWeightTreeIgnoresStaleChildEdges(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	f.mu.Lock()
	defer f.mu.Unlock()

	staleChild := common.HexToHash("0xdead")
	f.updateChildren(0, justified.Root, staleChild)
	f.gloasWeightTree.prepare(justified, cs)

	require.NotContains(t, f.gloasWeightTree.nodes[justified.Root].children, staleChild)
}

func TestGloasWeightTreeTracksVotesForMissingRoots(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)
	_, missingRoot := decodeDiffBlock(t, diffBlockc2Enc)

	f.mu.Lock()
	defer f.mu.Unlock()

	var validatorIndex uint64
	found := false
	for i := 0; i < cs.validatorSetSize; i++ {
		if readFromBitset(cs.actives, i) && !readFromBitset(cs.slasheds, i) && cs.balances[i] > 0 {
			validatorIndex = uint64(i)
			found = true
			break
		}
	}
	require.True(t, found)

	f.setLatestMessage(validatorIndex, LatestMessage{Root: missingRoot})
	f.gloasWeightTree.applied = growGloasContributions(f.gloasWeightTree.applied, int(validatorIndex)+1)
	f.gloasWeightTree.allDirty = false
	delete(f.gloasWeightTree.nodes, missingRoot)
	f.gloasWeightTree.addValidatorContribution(validatorIndex, cs)
	require.Contains(t, f.gloasWeightTree.missingRootVotes[missingRoot], validatorIndex)
	require.False(t, f.gloasWeightTree.applied[validatorIndex].direct)

	f.gloasWeightTree.ensureTopology(justified.Root)
	require.Contains(t, f.gloasWeightTree.dirty, validatorIndex)
	require.NotContains(t, f.gloasWeightTree.missingRootVotes, missingRoot)
	require.Contains(t, f.gloasWeightTree.nodes, missingRoot)

	f.gloasWeightTree.applyDirtyValidators(cs)
	require.True(t, f.gloasWeightTree.applied[validatorIndex].direct)
	require.Equal(t, cs.balances[validatorIndex], f.gloasWeightTree.nodes[missingRoot].directPending)
	require.Empty(t, f.gloasWeightTree.dirty)
	require.False(t, f.gloasWeightTree.allDirty)
}

func TestGloasWeightTreeClearsMissingRootVoteWhenValidatorMovesAway(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	f.mu.Lock()
	defer f.mu.Unlock()

	missingRoot := common.HexToHash("0xdeadbeef")
	_, liveRoot := decodeDiffBlock(t, diffBlockc2Enc)
	var validatorIndex uint64
	found := false
	for i := 0; i < cs.validatorSetSize; i++ {
		if readFromBitset(cs.actives, i) && !readFromBitset(cs.slasheds, i) && cs.balances[i] > 0 {
			validatorIndex = uint64(i)
			found = true
			break
		}
	}
	require.True(t, found)

	f.gloasWeightTree.state = cs
	f.setLatestMessage(validatorIndex, LatestMessage{Root: missingRoot})
	f.gloasWeightTree.applied = growGloasContributions(f.gloasWeightTree.applied, int(validatorIndex)+1)
	f.gloasWeightTree.allDirty = false
	f.gloasWeightTree.dirty = make(map[uint64]struct{})
	f.gloasWeightTree.addValidatorContribution(validatorIndex, cs)
	require.Contains(t, f.gloasWeightTree.missingRootVotes[missingRoot], validatorIndex)

	f.setLatestMessage(validatorIndex, LatestMessage{Root: liveRoot})
	f.gloasWeightTree.nodes[liveRoot] = &gloasWeightNode{}
	f.gloasWeightTree.applyDirtyValidators(cs)

	require.NotContains(t, f.gloasWeightTree.missingRootVotes, missingRoot)
	require.True(t, f.gloasWeightTree.applied[validatorIndex].direct)
	require.Equal(t, cs.balances[validatorIndex], f.gloasWeightTree.nodes[liveRoot].directPending)

	f.gloasWeightTree.markMissingRootDirty(missingRoot)
	require.Empty(t, f.gloasWeightTree.dirty)
}

func TestOnNewFinalizedPrunesGloasWeightTree(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	_, rootC2 := decodeDiffBlock(t, diffBlockc2Enc)

	f.mu.Lock()
	defer f.mu.Unlock()
	f.gloasWeightTree.prepare(justified, cs)
	require.Contains(t, f.gloasWeightTree.nodes, rootC2)

	f.onNewFinalized(solid.Checkpoint{Epoch: 1, Root: rootC2})

	require.NotContains(t, f.gloasWeightTree.nodes, rootC2)
	require.True(t, f.gloasWeightTree.allDirty)
}

func TestGloasWeightTreePruneFinalizedDropsBoundaryAndUnknownRoots(t *testing.T) {
	f := buildExAnteStore(t)
	justified := f.justifiedCheckpoint.Load().(solid.Checkpoint)
	cs, err := f.getCheckpointState(justified)
	require.NoError(t, err)
	require.NotNil(t, cs)

	_, rootC2 := decodeDiffBlock(t, diffBlockc2Enc)

	f.mu.Lock()
	defer f.mu.Unlock()

	f.gloasWeightTree.prepare(justified, cs)
	require.Contains(t, f.gloasWeightTree.nodes, rootC2)
	header, hasHeader := f.forkGraph.GetHeader(rootC2)
	require.True(t, hasHeader)
	require.NotZero(t, header.Slot)

	f.gloasWeightTree.pruneFinalized(header.Slot - 1)
	require.Contains(t, f.gloasWeightTree.nodes, rootC2)

	unknownRoot := common.HexToHash("0xdeadbeef")
	f.gloasWeightTree.nodes[unknownRoot] = &gloasWeightNode{}

	f.gloasWeightTree.pruneFinalized(header.Slot)

	require.NotContains(t, f.gloasWeightTree.nodes, rootC2)
	require.NotContains(t, f.gloasWeightTree.nodes, unknownRoot)
	require.True(t, f.gloasWeightTree.allDirty)
}

func TestApplyWeightDeltaDoesNotUnderflow(t *testing.T) {
	require.Equal(t, uint64(13), applyWeightDelta(10, 3, true))
	require.Equal(t, uint64(7), applyWeightDelta(10, 3, false))
	require.Zero(t, applyWeightDelta(3, 10, false))
}
