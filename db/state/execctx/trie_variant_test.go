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

package execctx

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestPickTrieVariant(t *testing.T) {
	parallel, v4 := statecfg.ExperimentalParallelCommitment, statecfg.ExperimentalCommitmentV4
	t.Cleanup(func() {
		statecfg.ExperimentalParallelCommitment, statecfg.ExperimentalCommitmentV4 = parallel, v4
	})

	for _, tc := range []struct {
		parallel, v4 bool
		want         commitment.TrieVariant
	}{
		{false, false, commitment.VariantHexPatriciaTrie},
		{true, false, commitment.VariantParallelHexPatricia},
		{false, true, commitment.VariantCommitmentV4},
		{true, true, commitment.VariantCommitmentV4},
	} {
		statecfg.ExperimentalParallelCommitment, statecfg.ExperimentalCommitmentV4 = tc.parallel, tc.v4
		require.Equal(t, tc.want, PickTrieVariant(), "parallel=%v v4=%v", tc.parallel, tc.v4)
	}
}

func TestParseTrieVariantKnowsV4(t *testing.T) {
	require.Equal(t, commitment.VariantCommitmentV4, commitment.ParseTrieVariant("v4"))
	require.Equal(t, commitment.VariantParallelHexPatricia, commitment.ParseTrieVariant("parallel"))
	require.Equal(t, commitment.VariantHexPatriciaTrie, commitment.ParseTrieVariant("hex"))
}
