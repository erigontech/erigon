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

package testutil

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
)

// TestSelectCommitmentVariantLatches: the trie the fixtures run under is a
// process-global choice, so a mixed run has to fail rather than re-root one
// network's fixtures under the other's engine. No t.Parallel here — the test
// writes the same globals a concurrent one would read.
func TestSelectCommitmentVariantLatches(t *testing.T) {
	t.Run("bin refuses to share the process", func(t *testing.T) {
		require.NoError(t, selectCommitmentVariant(t, true))
		require.True(t, statecfg.ExperimentalBinCommitment)
		require.Equal(t, commitment.PBinHashBlake3, commitment.PBinHashSuiteName())

		require.NoError(t, selectCommitmentVariant(t, true), "the same variant twice is the ordinary case")
		require.Error(t, selectCommitmentVariant(t, false))
	})

	require.False(t, statecfg.ExperimentalBinCommitment, "the subtest has to hand the process back")
	require.Equal(t, commitment.PBinHashKeccak, commitment.PBinHashSuiteName())

	t.Run("hex refuses to share the process", func(t *testing.T) {
		require.NoError(t, selectCommitmentVariant(t, false))
		require.False(t, statecfg.ExperimentalBinCommitment)
		require.Error(t, selectCommitmentVariant(t, true))
	})
}

// TestSelectCommitmentVariantHoldsForOverlappingUsers: fixture files run as
// parallel subtests, so two of them hold the same variant at once and the one
// that finishes first must not hand the process back under the other.
func TestSelectCommitmentVariantHoldsForOverlappingUsers(t *testing.T) {
	first, err := acquireCommitmentVariant(true)
	require.NoError(t, err)
	second, err := acquireCommitmentVariant(true)
	require.NoError(t, err)

	first()
	require.True(t, statecfg.ExperimentalBinCommitment, "a variant is still held, so it cannot be handed back")
	require.Equal(t, commitment.PBinHashBlake3, commitment.PBinHashSuiteName())

	second()
	require.False(t, statecfg.ExperimentalBinCommitment, "the last holder has to hand the process back")
	require.Equal(t, commitment.PBinHashKeccak, commitment.PBinHashSuiteName())
}
