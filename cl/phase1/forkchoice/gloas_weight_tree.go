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
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

type gloasVoteContribution struct {
	message      LatestMessage
	contribution uint64
	direct       bool
	set          bool
}

type gloasWeightNode struct {
	root                common.Hash
	parent              common.Hash
	parentPayloadStatus cltypes.PayloadStatus
	children            []common.Hash

	directPending uint64
	directEmpty   uint64
	directFull    uint64

	pendingWeight uint64
	emptyWeight   uint64
	fullWeight    uint64
}

type gloasWeightTree struct {
	f *ForkChoiceStore

	nodes      map[common.Hash]*gloasWeightNode
	applied    []gloasVoteContribution
	dirty      map[uint64]struct{}
	allDirty   bool
	checkpoint solid.Checkpoint
	state      *checkpointState
	ready      bool

	missingRootVotes map[common.Hash]map[uint64]struct{}

	boostKnown bool
	boost      bool

	topologySeen map[common.Hash]struct{}
	weightSeen   map[common.Hash]struct{}
	stack        []common.Hash
}

func newGloasWeightTree(f *ForkChoiceStore) *gloasWeightTree {
	return &gloasWeightTree{
		f:        f,
		nodes:    make(map[common.Hash]*gloasWeightNode),
		dirty:    make(map[uint64]struct{}),
		allDirty: true,
	}
}

func (t *gloasWeightTree) markDirty(validatorIndex uint64) {
	if t == nil {
		return
	}
	t.dirty[validatorIndex] = struct{}{}
}

func (t *gloasWeightTree) markAllDirty() {
	if t == nil {
		return
	}
	t.allDirty = true
	for vi := range t.dirty {
		delete(t.dirty, vi)
	}
}

func (t *gloasWeightTree) prepare(justified solid.Checkpoint, cs *checkpointState) WeightStore {
	t.boostKnown = false
	if cs == nil {
		t.state = nil
		t.allDirty = true
		return t
	}
	if t.state != nil && t.state != cs {
		t.allDirty = true
	}
	if !t.ready || t.checkpoint != justified {
		t.checkpoint = justified
		t.ready = true
		t.allDirty = true
	}
	t.state = cs
	t.ensureTopology(justified.Root)
	if t.allDirty {
		t.rebuildDirectWeights(cs)
	} else {
		t.applyDirtyValidators(cs)
	}
	t.recompute(justified.Root)
	return t
}

func (t *gloasWeightTree) ensureTopology(root common.Hash) {
	if t.topologySeen == nil {
		t.topologySeen = make(map[common.Hash]struct{})
	} else {
		clear(t.topologySeen)
	}
	t.stack = append(t.stack[:0], root)
	for len(t.stack) > 0 {
		current := t.stack[len(t.stack)-1]
		t.stack = t.stack[:len(t.stack)-1]
		if _, ok := t.topologySeen[current]; ok {
			continue
		}
		t.topologySeen[current] = struct{}{}

		node, ok := t.nodes[current]
		if !ok {
			node = &gloasWeightNode{root: current, parentPayloadStatus: cltypes.PayloadStatusPending}
			t.nodes[current] = node
			t.markMissingRootDirty(current)
		}

		t.filterLiveChildren(current, node)
		for _, child := range node.children {
			childNode, ok := t.nodes[child]
			if !ok {
				childNode = &gloasWeightNode{root: child}
				t.nodes[child] = childNode
				t.markMissingRootDirty(child)
			}
			block, ok := t.f.forkGraph.GetBlock(child)
			if !ok || block == nil {
				continue
			}
			parentPayloadStatus := t.f.getParentPayloadStatus(block.Block)
			if childNode.parent != current {
				childNode.parent = current
			}
			childNode.parentPayloadStatus = parentPayloadStatus
			t.stack = append(t.stack, child)
		}
	}
}

func (t *gloasWeightTree) filterLiveChildren(root common.Hash, node *gloasWeightNode) bool {
	children := t.f.children(root)
	liveCount := 0
	changed := false
	for _, child := range children {
		if _, hasHeader := t.f.forkGraph.GetHeader(child); !hasHeader {
			continue
		}
		block, hasBlock := t.f.forkGraph.GetBlock(child)
		if !hasBlock || block == nil {
			continue
		}
		if liveCount >= len(node.children) {
			node.children = append(node.children, child)
			changed = true
		} else {
			if node.children[liveCount] != child {
				changed = true
			}
			node.children[liveCount] = child
		}
		liveCount++
	}
	if liveCount != len(node.children) {
		changed = true
		node.children = node.children[:liveCount]
	}
	return changed
}

func (t *gloasWeightTree) rebuildDirectWeights(cs *checkpointState) {
	for _, node := range t.nodes {
		node.directPending = 0
		node.directEmpty = 0
		node.directFull = 0
	}
	t.applied = growGloasContributions(t.applied, t.f.latestMessages.latestMessagesCount())
	for i := range t.applied {
		t.applied[i] = gloasVoteContribution{}
	}
	if t.missingRootVotes != nil {
		clear(t.missingRootVotes)
	}
	for i := 0; i < t.f.latestMessages.latestMessagesCount(); i++ {
		t.addValidatorContribution(uint64(i), cs)
	}
	t.allDirty = false
	for vi := range t.dirty {
		delete(t.dirty, vi)
	}
}

func (t *gloasWeightTree) applyDirtyValidators(cs *checkpointState) {
	if len(t.dirty) == 0 {
		return
	}
	maxIndex := -1
	for vi := range t.dirty {
		if int(vi) >= cs.validatorSetSize || int(vi) >= len(cs.balances) {
			delete(t.dirty, vi)
			continue
		}
		if int(vi) > maxIndex {
			maxIndex = int(vi)
		}
	}
	if maxIndex < 0 {
		return
	}
	t.applied = growGloasContributions(t.applied, maxIndex+1)
	for vi := range t.dirty {
		t.removeAppliedContribution(vi)
		t.addValidatorContribution(vi, cs)
		delete(t.dirty, vi)
	}
}

func growGloasContributions(applied []gloasVoteContribution, size int) []gloasVoteContribution {
	if len(applied) >= size {
		return applied
	}
	next := make([]gloasVoteContribution, size)
	copy(next, applied)
	return next
}

func (t *gloasWeightTree) addValidatorContribution(validatorIndex uint64, cs *checkpointState) {
	vi := int(validatorIndex)
	if vi >= cs.validatorSetSize || vi >= len(cs.balances) {
		return
	}
	if !readFromBitset(cs.actives, vi) || readFromBitset(cs.slasheds, vi) || t.f.isUnequivocating(validatorIndex) {
		return
	}
	message, has := t.f.latestMessages.get(vi)
	if !has || message == (LatestMessage{}) {
		return
	}
	contribution := cs.balances[vi]
	if contribution == 0 {
		return
	}
	direct := false
	if t.nodes[message.Root] == nil {
		t.trackMissingRootVote(message.Root, validatorIndex)
	} else {
		direct = t.addDirectContribution(message, contribution)
	}
	t.applied[vi] = gloasVoteContribution{
		message:      message,
		contribution: contribution,
		direct:       direct,
		set:          true,
	}
}

func (t *gloasWeightTree) removeAppliedContribution(validatorIndex uint64) {
	vi := int(validatorIndex)
	if vi >= len(t.applied) || !t.applied[vi].set {
		return
	}
	if t.applied[vi].direct {
		t.subtractDirectContribution(t.applied[vi].message, t.applied[vi].contribution)
	} else {
		t.untrackMissingRootVote(t.applied[vi].message.Root, validatorIndex)
	}
	t.applied[vi] = gloasVoteContribution{}
}

func (t *gloasWeightTree) addDirectContribution(message LatestMessage, contribution uint64) bool {
	return t.applyDirectContribution(message, contribution, true)
}

func (t *gloasWeightTree) subtractDirectContribution(message LatestMessage, contribution uint64) {
	t.applyDirectContribution(message, contribution, false)
}

func (t *gloasWeightTree) applyDirectContribution(message LatestMessage, contribution uint64, add bool) bool {
	node := t.nodes[message.Root]
	if node == nil {
		return false
	}
	supported := t.f.getSupportedNode(message)
	if supported.Root != message.Root {
		return false
	}
	switch supported.PayloadStatus {
	case cltypes.PayloadStatusPending:
		node.directPending = applyWeightDelta(node.directPending, contribution, add)
	case cltypes.PayloadStatusEmpty:
		node.directEmpty = applyWeightDelta(node.directEmpty, contribution, add)
	case cltypes.PayloadStatusFull:
		node.directFull = applyWeightDelta(node.directFull, contribution, add)
	}
	return true
}

func (t *gloasWeightTree) trackMissingRootVote(root common.Hash, validatorIndex uint64) {
	if t.missingRootVotes == nil {
		t.missingRootVotes = make(map[common.Hash]map[uint64]struct{})
	}
	votes := t.missingRootVotes[root]
	if votes == nil {
		votes = make(map[uint64]struct{})
		t.missingRootVotes[root] = votes
	}
	votes[validatorIndex] = struct{}{}
}

func (t *gloasWeightTree) untrackMissingRootVote(root common.Hash, validatorIndex uint64) {
	votes := t.missingRootVotes[root]
	if votes == nil {
		return
	}
	delete(votes, validatorIndex)
	if len(votes) == 0 {
		delete(t.missingRootVotes, root)
	}
}

func (t *gloasWeightTree) markMissingRootDirty(root common.Hash) {
	votes := t.missingRootVotes[root]
	if votes == nil {
		return
	}
	for validatorIndex := range votes {
		t.markDirty(validatorIndex)
	}
	delete(t.missingRootVotes, root)
}

func applyWeightDelta(weight, delta uint64, add bool) uint64 {
	if add {
		return weight + delta
	}
	if delta > weight {
		return 0
	}
	return weight - delta
}

func (t *gloasWeightTree) recompute(root common.Hash) {
	if t.weightSeen == nil {
		t.weightSeen = make(map[common.Hash]struct{})
	} else {
		clear(t.weightSeen)
	}
	var walk func(common.Hash)
	walk = func(current common.Hash) {
		if _, ok := t.weightSeen[current]; ok {
			return
		}
		t.weightSeen[current] = struct{}{}
		node := t.nodes[current]
		if node == nil {
			return
		}
		node.pendingWeight = node.directPending + node.directEmpty + node.directFull
		node.emptyWeight = node.directEmpty
		node.fullWeight = node.directFull
		for _, child := range node.children {
			walk(child)
			childNode := t.nodes[child]
			if childNode == nil {
				continue
			}
			node.pendingWeight += childNode.pendingWeight
			switch childNode.parentPayloadStatus {
			case cltypes.PayloadStatusEmpty:
				node.emptyWeight += childNode.pendingWeight
			case cltypes.PayloadStatusFull:
				node.fullWeight += childNode.pendingWeight
			}
		}
	}
	walk(root)
}

func (t *gloasWeightTree) GetWeight(node ForkChoiceNode) uint64 {
	return getWeight(t, t.f, node)
}

func (t *gloasWeightTree) GetAttestationScore(node ForkChoiceNode) uint64 {
	if t.state == nil {
		return 0
	}
	weightNode := t.nodes[node.Root]
	if weightNode == nil {
		return 0
	}
	switch node.PayloadStatus {
	case cltypes.PayloadStatusPending:
		return weightNode.pendingWeight
	case cltypes.PayloadStatusEmpty:
		return weightNode.emptyWeight
	case cltypes.PayloadStatusFull:
		return weightNode.fullWeight
	default:
		return 0
	}
}

func (t *gloasWeightTree) GetProposerScore() uint64 {
	return getProposerScore(t.f, t.state)
}

func (t *gloasWeightTree) ShouldApplyProposerBoost() bool {
	if t.boostKnown {
		return t.boost
	}
	proposerBoostRoot := t.f.ProposerBoostRoot()
	if proposerBoostRoot == (common.Hash{}) {
		t.boostKnown = true
		t.boost = false
		return false
	}
	fullScan := newWeightStoreFromCheckpointState(t.f, t.state)
	t.boost = t.f.shouldApplyProposerBoostGloasWith(proposerBoostRoot, func(root common.Hash) bool {
		return t.f.isHeadWeakWith(root, t.state, func(root common.Hash) uint64 {
			return fullScan.GetAttestationScore(ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusPending})
		})
	})
	t.boostKnown = true
	return t.boost
}

func (t *gloasWeightTree) pruneFinalized(finalizedSlot uint64) {
	for root := range t.nodes {
		header, has := t.f.forkGraph.GetHeader(root)
		if !has || header.Slot <= finalizedSlot {
			delete(t.nodes, root)
		}
	}
	t.markAllDirty()
}
