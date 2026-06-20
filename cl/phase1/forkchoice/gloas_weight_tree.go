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
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

type gloasVoteContribution struct {
	message      LatestMessage
	contribution uint64
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
	if t.ensureTopology(justified.Root) {
		t.allDirty = true
	}
	if t.allDirty {
		t.rebuildDirectWeights(cs)
	} else {
		t.applyDirtyValidators(cs)
	}
	t.recompute(justified.Root)
	return t
}

func (t *gloasWeightTree) ensureTopology(root common.Hash) bool {
	changed := false
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
			changed = true
		}

		if t.filterLiveChildren(current, node) {
			changed = true
		}
		for _, child := range node.children {
			childNode, ok := t.nodes[child]
			if !ok {
				childNode = &gloasWeightNode{root: child}
				t.nodes[child] = childNode
				changed = true
			}
			block, _ := t.f.forkGraph.GetBlock(child)
			parentPayloadStatus := t.f.getParentPayloadStatus(block.Block)
			if childNode.parent != current || childNode.parentPayloadStatus != parentPayloadStatus {
				childNode.parent = current
				childNode.parentPayloadStatus = parentPayloadStatus
				changed = true
			}
			t.stack = append(t.stack, child)
		}
	}
	return changed
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
	t.addDirectContribution(message, contribution)
	t.applied[vi] = gloasVoteContribution{
		message:      message,
		contribution: contribution,
		set:          true,
	}
}

func (t *gloasWeightTree) removeAppliedContribution(validatorIndex uint64) {
	vi := int(validatorIndex)
	if vi >= len(t.applied) || !t.applied[vi].set {
		return
	}
	t.addDirectContribution(t.applied[vi].message, ^(t.applied[vi].contribution - 1))
	t.applied[vi] = gloasVoteContribution{}
}

func (t *gloasWeightTree) addDirectContribution(message LatestMessage, contribution uint64) {
	node := t.nodes[message.Root]
	if node == nil {
		return
	}
	supported := t.f.getSupportedNode(message)
	if supported.Root != message.Root {
		return
	}
	switch supported.PayloadStatus {
	case cltypes.PayloadStatusPending:
		node.directPending += contribution
	case cltypes.PayloadStatusEmpty:
		node.directEmpty += contribution
	case cltypes.PayloadStatusFull:
		node.directFull += contribution
	}
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
	if t.f.isPreviousSlotPayloadDecision(node) {
		return 0
	}
	attestationScore := t.GetAttestationScore(node)
	if !t.ShouldApplyProposerBoost() {
		return attestationScore
	}
	proposerBoostRoot := t.f.ProposerBoostRoot()
	if proposerBoostRoot == (common.Hash{}) {
		return attestationScore
	}
	proposerBoostNode := ForkChoiceNode{Root: proposerBoostRoot, PayloadStatus: cltypes.PayloadStatusPending}
	if t.f.isAncestor(proposerBoostNode, node) {
		return attestationScore + t.GetProposerScore()
	}
	return attestationScore
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
	cs := t.state
	if cs == nil {
		return 0
	}
	committeeWeight := cs.activeBalance / t.f.beaconCfg.SlotsPerEpoch
	return (committeeWeight * t.f.beaconCfg.ProposerScoreBoost) / 100
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
	t.boost = t.shouldApplyProposerBoostGloas(proposerBoostRoot)
	t.boostKnown = true
	return t.boost
}

func (t *gloasWeightTree) shouldApplyProposerBoostGloas(proposerBoostRoot common.Hash) bool {
	boostBlock, ok := t.f.forkGraph.GetBlock(proposerBoostRoot)
	if !ok || boostBlock == nil {
		return false
	}
	parentRoot := boostBlock.Block.ParentRoot
	slot := boostBlock.Block.Slot

	parentBlock, ok := t.f.forkGraph.GetBlock(parentRoot)
	if !ok || parentBlock == nil {
		return false
	}
	if parentBlock.Block.Slot+1 < slot {
		return true
	}
	if !t.isHeadWeak(parentRoot) {
		return true
	}

	parentProposerIndex := parentBlock.Block.ProposerIndex
	hasEquivocation := false
	t.f.blockTimeliness.Range(func(key, value any) bool {
		root := key.(common.Hash)
		if root == parentRoot {
			return true
		}
		timeliness := value.([clparams.NumBlockTimelinessDeadlines]bool)
		if !timeliness[clparams.PtcTimelinessIndex] {
			return true
		}
		blk, blkOk := t.f.forkGraph.GetBlock(root)
		if !blkOk || blk == nil {
			return true
		}
		if blk.Block.ProposerIndex == parentProposerIndex && blk.Block.Slot+1 == slot {
			hasEquivocation = true
			return false
		}
		return true
	})
	return !hasEquivocation
}

func (t *gloasWeightTree) isHeadWeak(root common.Hash) bool {
	cs := t.state
	if cs == nil {
		return false
	}
	committeeWeight := cs.activeBalance / t.f.beaconCfg.SlotsPerEpoch
	reorgThreshold := committeeWeight * clparams.ReorgHeadWeightThreshold / 100
	headWeight := t.GetAttestationScore(ForkChoiceNode{Root: root, PayloadStatus: cltypes.PayloadStatusPending})

	headBlock, ok := t.f.forkGraph.GetBlock(root)
	if !ok || headBlock == nil {
		return false
	}
	headSlot := headBlock.Block.Slot
	epoch := t.f.computeEpochAtSlot(headSlot)
	lenIndicies := uint64(len(cs.shuffledSet))
	if lenIndicies > 0 {
		committeesPerSlot := cs.committeeCount(epoch, lenIndicies)
		count := committeesPerSlot * t.f.beaconCfg.SlotsPerEpoch
		for ci := uint64(0); ci < committeesPerSlot; ci++ {
			index := (headSlot%t.f.beaconCfg.SlotsPerEpoch)*committeesPerSlot + ci
			start := (lenIndicies * index) / count
			end := (lenIndicies * (index + 1)) / count
			for _, validatorIndex := range cs.shuffledSet[start:end] {
				if !t.f.isUnequivocating(validatorIndex) {
					continue
				}
				vi := int(validatorIndex)
				if vi < cs.validatorSetSize && readFromBitset(cs.actives, vi) && !readFromBitset(cs.slasheds, vi) {
					headWeight += cs.balances[vi]
				}
			}
		}
	}
	return headWeight < reorgThreshold
}

func (t *gloasWeightTree) isParentStrong(root common.Hash) bool {
	cs := t.state
	if cs == nil {
		return false
	}
	committeeWeight := cs.activeBalance / t.f.beaconCfg.SlotsPerEpoch
	parentThreshold := committeeWeight * clparams.ReorgParentWeightThreshold / 100

	block, ok := t.f.forkGraph.GetBlock(root)
	if !ok || block == nil {
		return false
	}
	parentNode := ForkChoiceNode{
		Root:          block.Block.ParentRoot,
		PayloadStatus: t.f.getParentPayloadStatus(block.Block),
	}
	return t.GetAttestationScore(parentNode) > parentThreshold
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
