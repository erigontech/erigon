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

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestFastConfirmationStartsAtFinalized(t *testing.T) {
	finalized := solid.Checkpoint{Epoch: 7, Root: common.Hash{7}}
	got := newFastConfirmation(finalized)
	require.Equal(t, finalized.Root, got.confirmedRoot)
	require.Equal(t, finalized.Root, got.currentSlotHead)
	require.Equal(t, finalized.Root, got.previousSlotHead)
	require.Equal(t, finalized, got.currentObserved)
	require.Equal(t, finalized, got.previousObserved)
	require.Equal(t, finalized, got.greatestUnrealized)
}

func TestFastConfirmationCommitteeWeight(t *testing.T) {
	b := &fcrBalances{balances: []uint64{100, 200, 400, 800}, active: []bool{true, true, true, false}, slashed: make([]bool, 4), total: 700}
	v := &fcrView{slotsPerEpoch: 4, balanceIncrement: 1, committees: map[uint64][]uint64{3: {0, 1}, 4: {1, 2, 3}}, votes: make([]fcrVote, 4)}
	require.EqualValues(t, 300, v.committeeWeight(b, 3, 3))
	require.EqualValues(t, 700, v.committeeWeight(b, 3, 4))
	require.EqualValues(t, 700, v.committeeWeight(b, 0, 3))
	require.Zero(t, v.committeeWeight(b, 4, 3))
	v.votes[0].equivocating = true
	require.EqualValues(t, 75, v.adversarialWeight(b, 3, 4))
	require.Zero(t, v.adversarialWeight(b, 3, 3))
}

func TestFastConfirmationRequiresStrictSupportAndValidExecution(t *testing.T) {
	parent, child := common.Hash{1}, common.Hash{2}
	b := &fcrBalances{balances: []uint64{300, 100}, active: []bool{true, true}, slashed: []bool{false, false}, total: 400}
	v := &fcrView{slot: 2, slotsPerEpoch: 4, balanceIncrement: 1, committees: map[uint64][]uint64{1: {0, 1}}, votes: []fcrVote{{root: child}, {}}, blocks: map[common.Hash]fcrBlock{parent: {slot: 0}, child: {slot: 1, parent: parent, valid: true}}}
	require.False(t, v.oneConfirmed(b, child), "support equal to threshold is insufficient")
	v.votes[1].root = child
	require.True(t, v.oneConfirmed(b, child))
	block := v.blocks[child]
	block.valid = false
	v.blocks[child] = block
	require.False(t, v.oneConfirmed(b, child))
	block.valid = true
	v.blocks[child] = block
	v.votes[0].equivocating = true
	require.False(t, v.oneConfirmed(b, child))
	v.votes[0].equivocating = false
	b.slashed[0] = true
	require.False(t, v.oneConfirmed(b, child))
}

func TestFastConfirmationGloasEmptySlotDiscount(t *testing.T) {
	parent, child := common.Hash{1}, common.Hash{2}
	b := &fcrBalances{balances: []uint64{100, 100, 100, 100}, active: []bool{true, true, true, true}, slashed: make([]bool, 4), total: 400}
	v := &fcrView{slot: 4, slotsPerEpoch: 8, balanceIncrement: 1, gloas: true, committees: map[uint64][]uint64{1: {0, 1, 2, 3}, 2: {}}, blocks: map[common.Hash]fcrBlock{parent: {slot: 0}, child: {slot: 3, parent: parent, parentStatus: cltypes.PayloadStatusFull}}, votes: []fcrVote{
		{root: parent, status: cltypes.PayloadStatusPending},
		{root: parent, status: cltypes.PayloadStatusFull},
		{root: parent, status: cltypes.PayloadStatusFull},
		{root: parent, status: cltypes.PayloadStatusEmpty},
	}}
	require.EqualValues(t, 200, v.emptySlotDiscount(b, child))
	v.gloas = false
	require.Zero(t, v.emptySlotDiscount(b, child))
}

func TestFastConfirmationEpochRotation(t *testing.T) {
	anchor := solid.Checkpoint{Root: common.Hash{1}}
	observed := solid.Checkpoint{Epoch: 1, Root: common.Hash{4}}
	f := newFastConfirmation(anchor)
	v := &fcrView{slot: 7, slotsPerEpoch: 4, head: common.Hash{7}, unrealized: observed}
	f.updateVariables(v)
	require.Equal(t, observed, f.greatestUnrealized)
	require.Equal(t, anchor, f.currentObserved)
	v.slot = 8
	v.head = common.Hash{8}
	v.unrealized = solid.Checkpoint{Epoch: 2, Root: v.head}
	f.updateVariables(v)
	require.Equal(t, common.Hash{7}, f.previousSlotHead)
	require.Equal(t, v.head, f.currentSlotHead)
	require.Equal(t, anchor, f.previousObserved)
	require.Equal(t, observed, f.currentObserved)
}

func TestFastConfirmationHonestFFGSupport(t *testing.T) {
	root := common.Hash{4}
	b := &fcrBalances{balances: []uint64{100, 100, 100, 100}, active: []bool{true, true, true, true}, slashed: make([]bool, 4), total: 400}
	v := &fcrView{
		slot: 5, slotsPerEpoch: 4, balanceIncrement: 1, head: root, headBalances: b,
		blocks: map[common.Hash]fcrBlock{root: {slot: 4}}, committees: map[uint64][]uint64{4: {0}},
		votes: []fcrVote{{root: root, epoch: 1}, {root: root, epoch: 0}, {}, {}},
	}
	require.EqualValues(t, 300, v.honestFFGSupport())
	v.committees[4] = []uint64{0, 1, 2, 3}
	require.Zero(t, v.honestFFGSupport())
}

func TestFastConfirmationRestartsAndAdvancesAtEpochBoundary(t *testing.T) {
	roots := []common.Hash{{1}, {4}, {5}, {6}, {7}}
	finalized := solid.Checkpoint{Root: roots[0]}
	justified := solid.Checkpoint{Epoch: 1, Root: roots[1]}
	b := &fcrBalances{balances: []uint64{100, 100, 100, 100}, active: []bool{true, true, true, true}, slashed: make([]bool, 4), total: 400}
	v := &fcrView{
		slot: 8, slotsPerEpoch: 4, balanceIncrement: 1, head: roots[4], finalized: finalized, unrealized: justified, headBalances: b,
		committees: map[uint64][]uint64{4: {0}, 5: {1}, 6: {2}, 7: {3}}, votes: make([]fcrVote, 4), blocks: make(map[common.Hash]fcrBlock),
	}
	v.blocks[roots[0]] = fcrBlock{slot: 0, valid: true}
	for i := 1; i < len(roots); i++ {
		v.blocks[roots[i]] = fcrBlock{slot: uint64(i + 3), parent: roots[i-1], valid: true, unrealized: justified, votingSource: justified}
	}
	for i := range v.votes {
		v.votes[i] = fcrVote{root: v.head, epoch: 1}
	}
	f := newFastConfirmation(finalized)
	f.greatestUnrealized = justified
	f.currentSlotHead = v.head
	f.updateVariables(v)
	require.Equal(t, v.head, f.latestConfirmed(v, b, b))
	v.votes[0].root = roots[1]
	v.votes[1].root = roots[1]
	v.votes[2].root = roots[1]
	require.Equal(t, justified.Root, f.latestConfirmed(v, b, b), "a failed ancestor prevents confirming its descendants")
}

func TestFastConfirmationFallsBackAfterReorgAndStaleConfirmation(t *testing.T) {
	finalized, old, head := common.Hash{1}, common.Hash{2}, common.Hash{3}
	b := &fcrBalances{total: 100}
	v := &fcrView{slot: 13, slotsPerEpoch: 4, head: head, finalized: solid.Checkpoint{Root: finalized}, headBalances: b, blocks: map[common.Hash]fcrBlock{
		finalized: {slot: 0}, old: {slot: 9, parent: finalized}, head: {slot: 12, parent: finalized},
	}}
	f := newFastConfirmation(v.finalized)
	f.confirmedRoot = old
	require.Equal(t, finalized, f.latestConfirmed(v, b, b))
	v.head = old
	v.slot = 17
	require.Equal(t, finalized, f.latestConfirmed(v, b, b))
}
