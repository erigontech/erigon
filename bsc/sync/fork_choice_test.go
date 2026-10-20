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

package bscsync

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/bsc/parlia"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

type forkChoiceNode struct {
	td        uint64
	number    uint64
	time      uint64
	coinbase  byte
	hash      byte
	justified uint64
}

func (n forkChoiceNode) build() *forkTreeNode {
	return &forkTreeNode{
		header: &types.Header{
			Number:   *uint256.NewInt(n.number),
			Time:     n.time,
			Coinbase: common.Address{n.coinbase},
		},
		headerHash:      common.Hash{n.hash},
		totalDifficulty: n.td,
		snapshot:        &parlia.Snapshot{Attestation: &parlia.VoteData{TargetNumber: n.justified}},
	}
}

func TestReorgNeededHigherJustifiedWins(t *testing.T) {
	t.Parallel()
	current := forkChoiceNode{td: 10, number: 5, time: 5, coinbase: 1, hash: 1, justified: 3}
	candidate := forkChoiceNode{td: 4, number: 3, time: 3, coinbase: 2, hash: 2, justified: 4}
	require.True(t, reorgNeeded(current.build(), candidate.build()))
	require.False(t, reorgNeeded(candidate.build(), current.build()))
}

func TestReorgNeededHigherTotalDifficultyWins(t *testing.T) {
	t.Parallel()
	current := forkChoiceNode{td: 4, number: 3, time: 3, coinbase: 1, hash: 9, justified: 1}
	candidate := forkChoiceNode{td: 5, number: 4, time: 4, coinbase: 2, hash: 1, justified: 1}
	require.True(t, reorgNeeded(current.build(), candidate.build()))
	require.False(t, reorgNeeded(candidate.build(), current.build()))
}

func TestReorgNeededLowerNumberWins(t *testing.T) {
	t.Parallel()
	current := forkChoiceNode{td: 4, number: 3, time: 3, coinbase: 1, hash: 9, justified: 1}
	candidate := forkChoiceNode{td: 4, number: 2, time: 4, coinbase: 2, hash: 1, justified: 1}
	require.True(t, reorgNeeded(current.build(), candidate.build()))
	require.False(t, reorgNeeded(candidate.build(), current.build()))
}

func TestReorgNeededEarlierTimeWins(t *testing.T) {
	t.Parallel()
	current := forkChoiceNode{td: 4, number: 3, time: 7, coinbase: 1, hash: 2, justified: 1}
	candidate := forkChoiceNode{td: 4, number: 3, time: 6, coinbase: 2, hash: 1, justified: 1}
	require.True(t, reorgNeeded(current.build(), candidate.build()))
	require.False(t, reorgNeeded(candidate.build(), current.build()))
}

func TestReorgNeededDoubleSignLowerHashWins(t *testing.T) {
	t.Parallel()
	current := forkChoiceNode{td: 4, number: 3, time: 7, coinbase: 1, hash: 2, justified: 1}
	candidate := forkChoiceNode{td: 4, number: 3, time: 7, coinbase: 1, hash: 1, justified: 1}
	require.True(t, reorgNeeded(current.build(), candidate.build()))
	require.False(t, reorgNeeded(candidate.build(), current.build()))
}

func TestReorgNeededKeepsCurrentOnTie(t *testing.T) {
	t.Parallel()
	a := forkChoiceNode{td: 4, number: 3, time: 7, coinbase: 1, hash: 1, justified: 1}
	b := forkChoiceNode{td: 4, number: 3, time: 7, coinbase: 2, hash: 2, justified: 1}
	require.False(t, reorgNeeded(a.build(), b.build()))
	require.False(t, reorgNeeded(b.build(), a.build()))
}

func TestCCBTipFollowsHigherJustified(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	test, root := newConnectCCBTest(t)
	heavy := test.makeHeaders(root, []uint64{2, 2})
	justified := test.makeHeader(root, 1)
	test.verifier.justified[justified.Hash()] = &parlia.VoteData{TargetNumber: 1, TargetHash: justified.Hash()}

	_, err := test.builder.Connect(ctx, heavy)
	require.NoError(t, err)
	require.Equal(t, heavy[1].Hash(), test.builder.Tip().Hash())

	_, err = test.builder.Connect(ctx, []*types.Header{justified})
	require.NoError(t, err)
	require.Equal(t, justified.Hash(), test.builder.Tip().Hash())
}

func TestCCBRecalcTipIsDeterministicOnTie(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	for range 20 {
		test, root := newConnectCCBTest(t)
		a := test.makeHeader(root, 1)
		b := test.makeHeader(root, 1)
		b.Time = a.Time
		b.Coinbase = common.Address{1}
		c := test.makeHeader(root, 2)
		_, err := test.builder.Connect(ctx, []*types.Header{a})
		require.NoError(t, err)
		_, err = test.builder.Connect(ctx, []*types.Header{b})
		require.NoError(t, err)
		_, err = test.builder.Connect(ctx, []*types.Header{c})
		require.NoError(t, err)
		require.Equal(t, c.Hash(), test.builder.Tip().Hash())

		require.NoError(t, test.builder.PruneNode(c.Hash()))
		want := a
		if b.Hash().Cmp(a.Hash()) < 0 {
			want = b
		}
		require.Equal(t, want.Hash(), test.builder.Tip().Hash())
	}
}
