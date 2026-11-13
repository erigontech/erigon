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

package services

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Every validator votes once per epoch, so a node that receives all subnets sees about a thirty-second of the
// validator set per slot; the record must hold all of them for the whole epoch.
func TestValidatorVotesKeepsEveryVoterOfAnEpoch(t *testing.T) {
	votes := newValidatorVotes()
	for index := range uint64(1_200_000) {
		require.True(t, votes.claim(index, 10))
	}

	require.True(t, votes.seen(0, 10))
	require.False(t, votes.claim(0, 10))
}

// Gossip accepts votes of the previous and the current epoch, so a validator can have one vote in each.
func TestValidatorVotesKeepsPreviousAndCurrentEpochApart(t *testing.T) {
	votes := newValidatorVotes()

	require.True(t, votes.claim(7, 9))
	require.True(t, votes.claim(7, 10))
	require.False(t, votes.claim(7, 9))
	require.False(t, votes.claim(7, 10))
	require.False(t, votes.seen(8, 10))
}

func TestValidatorVotesDropsEpochsBeforeThePreviousOne(t *testing.T) {
	votes := newValidatorVotes()
	require.True(t, votes.claim(7, 8))
	require.True(t, votes.claim(7, 9))

	require.True(t, votes.claim(1, 10))

	require.False(t, votes.seen(7, 8))
	require.True(t, votes.seen(7, 9))
	require.Len(t, votes.byEpoch, 2)
}
