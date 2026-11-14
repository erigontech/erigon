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

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

type fastConfirmation struct {
	confirmedRoot      common.Hash
	previousSlotHead   common.Hash
	currentSlotHead    common.Hash
	previousObserved   solid.Checkpoint
	currentObserved    solid.Checkpoint
	greatestUnrealized solid.Checkpoint
}

func newFastConfirmation(finalized solid.Checkpoint) *fastConfirmation {
	return &fastConfirmation{confirmedRoot: finalized.Root, previousSlotHead: finalized.Root, currentSlotHead: finalized.Root, previousObserved: finalized, currentObserved: finalized, greatestUnrealized: finalized}
}

type fcrBalances struct {
	balances []uint64
	active   []bool
	slashed  []bool
	total    uint64
}

type fcrVote struct {
	root         common.Hash
	status       cltypes.PayloadStatus
	epoch        uint64
	equivocating bool
}

type fcrView struct {
	slot             uint64
	slotsPerEpoch    uint64
	balanceIncrement uint64
	proposerBoost    uint64
	committees       map[uint64][]uint64
	votes            []fcrVote
	blocks           map[common.Hash]fcrBlock
	head             common.Hash
	finalized        solid.Checkpoint
	unrealized       solid.Checkpoint
	headBalances     *fcrBalances
	gloas            bool
	ranges           map[fcrRange]fcrRangeWeights
	scores           map[*fcrBalances]map[common.Hash]uint64
	seen             []uint32
	generation       uint32
}

type fcrRange struct {
	balances   *fcrBalances
	start, end uint64
}

type fcrSupportedNode struct {
	root   common.Hash
	status cltypes.PayloadStatus
}

type fcrRangeWeights struct {
	total, equivocating uint64
	support             map[fcrSupportedNode]uint64
}

func (v *fcrView) rangeWeights(b *fcrBalances, start, end uint64) fcrRangeWeights {
	key := fcrRange{b, start, end}
	if cached, ok := v.ranges[key]; ok {
		return cached
	}
	weights := fcrRangeWeights{support: make(map[fcrSupportedNode]uint64)}
	if start > end {
		return weights
	}
	if len(v.seen) < len(b.balances) {
		v.seen = make([]uint32, len(b.balances))
	}
	v.generation++
	for slot := start; slot <= end; slot++ {
		for _, index := range v.committees[slot] {
			if int(index) >= len(b.balances) || !b.active[index] || v.seen[index] == v.generation {
				continue
			}
			v.seen[index] = v.generation
			balance := b.balances[index]
			weights.total += balance
			if int(index) >= len(v.votes) {
				continue
			}
			vote := v.votes[index]
			if vote.equivocating {
				weights.equivocating += balance
			} else if !b.slashed[index] && vote.root != (common.Hash{}) {
				weights.support[fcrSupportedNode{vote.root, vote.status}] += balance
			}
		}
	}
	if (start+v.slotsPerEpoch-1)/v.slotsPerEpoch < (end+1)/v.slotsPerEpoch {
		weights.total = b.total
	} else {
		weights.total = max(weights.total, v.balanceIncrement)
	}
	if v.ranges != nil {
		v.ranges[key] = weights
	}
	return weights
}

func (v *fcrView) committeeWeight(b *fcrBalances, start, end uint64) uint64 {
	return v.rangeWeights(b, start, end).total
}

func (v *fcrView) adversarialWeight(b *fcrBalances, start, end uint64) uint64 {
	weights := v.rangeWeights(b, start, end)
	return fcrSaturatingSub(weights.total/100*25, weights.equivocating)
}

func (v *fcrView) attestationScores(b *fcrBalances) map[common.Hash]uint64 {
	if cached, ok := v.scores[b]; ok {
		return cached
	}
	scores := make(map[common.Hash]uint64)
	for i, vote := range v.votes {
		if i < len(b.balances) && b.active[i] && !b.slashed[i] && !vote.equivocating && vote.root != (common.Hash{}) {
			scores[vote.root] += b.balances[i]
		}
	}
	roots := make([]common.Hash, 0, len(v.blocks))
	for root := range v.blocks {
		roots = append(roots, root)
	}
	slices.SortFunc(roots, func(a, b common.Hash) int {
		if v.blocks[a].slot > v.blocks[b].slot {
			return -1
		}
		if v.blocks[a].slot < v.blocks[b].slot {
			return 1
		}
		return 0
	})
	for _, root := range roots {
		if parent := v.blocks[root].parent; parent != (common.Hash{}) {
			scores[parent] += scores[root]
		}
	}
	if v.scores != nil {
		v.scores[b] = scores
	}
	return scores
}

func fcrSaturatingSub(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return 0
}

type fcrBlock struct {
	slot         uint64
	parent       common.Hash
	parentStatus cltypes.PayloadStatus
	valid        bool
	unrealized   solid.Checkpoint
	votingSource solid.Checkpoint
}

func (v *fcrView) oneConfirmed(b *fcrBalances, root common.Hash) bool {
	block, ok := v.blocks[root]
	if !ok || !block.valid || v.slot == 0 {
		return false
	}
	parent, ok := v.blocks[block.parent]
	if !ok {
		return false
	}
	maximum := v.committeeWeight(b, parent.slot+1, v.slot-1)
	discount := v.emptySlotDiscount(b, root)
	if discount > maximum {
		panic("fast confirmation support discount exceeds committee weight")
	}
	start := block.slot
	if block.slot/v.slotsPerEpoch > parent.slot/v.slotsPerEpoch {
		start = block.slot / v.slotsPerEpoch * v.slotsPerEpoch
	}
	adversarial := v.adversarialWeight(b, start, v.slot-1)
	proposer := b.total / v.slotsPerEpoch * v.proposerBoost / 100
	threshold := (maximum + proposer + 2*adversarial - discount) / 2
	return v.attestationScores(b)[root] > threshold
}

func (v *fcrView) emptySlotDiscount(b *fcrBalances, root common.Hash) uint64 {
	block := v.blocks[root]
	parent := v.blocks[block.parent]
	if parent.slot+1 >= block.slot {
		return 0
	}
	weights := v.rangeWeights(b, parent.slot+1, block.slot-1)
	support := weights.support[fcrSupportedNode{block.parent, cltypes.PayloadStatusPending}]
	if v.gloas && block.parentStatus != cltypes.PayloadStatusPending {
		support += weights.support[fcrSupportedNode{block.parent, block.parentStatus}]
	}
	return fcrSaturatingSub(support, fcrSaturatingSub(weights.total/100*25, weights.equivocating))
}

func (v *fcrView) ancestor(root common.Hash, slot uint64) common.Hash {
	for root != (common.Hash{}) {
		block, ok := v.blocks[root]
		if !ok {
			return common.Hash{}
		}
		if block.slot <= slot {
			return root
		}
		root = block.parent
	}
	return common.Hash{}
}

func (v *fcrView) checkpoint(root common.Hash, epoch uint64) solid.Checkpoint {
	return solid.Checkpoint{Root: v.ancestor(root, epoch*v.slotsPerEpoch), Epoch: epoch}
}

func (v *fcrView) epoch(root common.Hash) uint64 { return v.blocks[root].slot / v.slotsPerEpoch }

func (v *fcrView) chain(root, terminal common.Hash) []common.Hash {
	var roots []common.Hash
	for v.blocks[root].slot > v.blocks[terminal].slot {
		roots = append(roots, root)
		root = v.blocks[root].parent
		if root == terminal {
			slices.Reverse(roots)
			return roots
		}
	}
	return nil
}

func (f *fastConfirmation) updateVariables(v *fcrView) {
	f.previousSlotHead = f.currentSlotHead
	f.currentSlotHead = v.head
	if (v.slot+1)%v.slotsPerEpoch == 0 {
		f.greatestUnrealized = v.unrealized
	}
	if v.slot%v.slotsPerEpoch == 0 {
		f.previousObserved = f.currentObserved
		f.currentObserved = f.greatestUnrealized
	}
}

func (v *fcrView) honestFFGSupport() uint64 {
	b := v.headBalances
	epoch := v.slot / v.slotsPerEpoch
	target := v.checkpoint(v.head, epoch)
	var support uint64
	for i, vote := range v.votes {
		if i < len(b.balances) && b.active[i] && !b.slashed[i] && !vote.equivocating && vote.root != (common.Hash{}) && v.checkpoint(vote.root, vote.epoch) == target {
			support += b.balances[i]
		}
	}
	var weight, adversarial uint64
	if v.slot > 0 {
		weight = v.committeeWeight(b, epoch*v.slotsPerEpoch, v.slot-1)
		adversarial = v.adversarialWeight(b, epoch*v.slotsPerEpoch, v.slot-1)
	}
	return fcrSaturatingSub(support, adversarial) + (b.total-weight)/100*75
}

func (v *fcrView) noConflictingJustification() bool {
	return v.checkpoint(v.head, v.slot/v.slotsPerEpoch) == v.unrealized || 3*v.honestFFGSupport() > v.headBalances.total
}

func (v *fcrView) targetWillBeJustified() bool {
	return 3*v.honestFFGSupport() >= 2*v.headBalances.total
}

func (f *fastConfirmation) latestConfirmed(v *fcrView, previous, current *fcrBalances) common.Hash {
	v.ranges = make(map[fcrRange]fcrRangeWeights)
	v.scores = make(map[*fcrBalances]map[common.Hash]uint64)
	confirmed := f.confirmedRoot
	epoch := v.slot / v.slotsPerEpoch
	epochStart := v.slot%v.slotsPerEpoch == 0
	if v.epoch(confirmed)+1 < epoch || v.ancestor(v.head, v.blocks[confirmed].slot) != confirmed || (epochStart && !f.chainSafe(v, previous, confirmed)) {
		confirmed = v.finalized.Root
	}
	observedSlot := v.blocks[f.currentObserved.Root].slot
	if epochStart && observedSlot/v.slotsPerEpoch+1 == epoch && f.currentObserved == v.blocks[v.head].unrealized && v.blocks[confirmed].slot < observedSlot {
		confirmed = f.currentObserved.Root
	}
	if v.epoch(confirmed)+1 < epoch {
		return confirmed
	}
	return f.latestDescendant(v, current, confirmed)
}

func (f *fastConfirmation) chainSafe(v *fcrView, b *fcrBalances, root common.Hash) bool {
	if f.currentObserved != v.checkpoint(root, f.currentObserved.Epoch) {
		return false
	}
	epoch := v.slot / v.slotsPerEpoch
	terminal := f.currentObserved.Root
	if f.currentObserved.Epoch+1 < epoch {
		terminal = v.ancestor(root, (epoch-1)*v.slotsPerEpoch)
		if v.epoch(terminal)+1 == epoch {
			terminal = v.blocks[terminal].parent
		}
	}
	for _, candidate := range v.chain(root, terminal) {
		if !v.oneConfirmed(b, candidate) {
			return false
		}
	}
	return true
}

func (f *fastConfirmation) latestDescendant(v *fcrView, b *fcrBalances, confirmed common.Hash) common.Hash {
	epoch := v.slot / v.slotsPerEpoch
	epochStart := v.slot%v.slotsPerEpoch == 0
	previous := v.blocks[f.previousSlotHead]
	head := v.blocks[v.head]
	if v.epoch(confirmed)+1 == epoch && previous.votingSource.Epoch+2 >= epoch && (epochStart || (v.noConflictingJustification() && (previous.unrealized.Epoch+1 >= epoch || head.unrealized.Epoch+1 >= epoch))) {
		for _, root := range v.chain(v.head, confirmed) {
			if v.epoch(root) == epoch || v.ancestor(f.previousSlotHead, v.blocks[root].slot) != root || !v.oneConfirmed(b, root) {
				break
			}
			confirmed = root
		}
	}
	if epochStart || head.unrealized.Epoch+1 >= epoch {
		tentative := confirmed
		for _, root := range v.chain(v.head, confirmed) {
			if v.epoch(root) > v.epoch(tentative) && !v.targetWillBeJustified() {
				break
			}
			if !v.oneConfirmed(b, root) {
				break
			}
			tentative = root
		}
		if v.epoch(tentative) == epoch || (v.blocks[tentative].votingSource.Epoch+2 >= epoch && (epochStart || v.noConflictingJustification())) {
			confirmed = tentative
		}
	}
	return confirmed
}
