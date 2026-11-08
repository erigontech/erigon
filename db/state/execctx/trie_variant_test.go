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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestPickTrieVariant(t *testing.T) {
	for _, tc := range []struct {
		parallel, v3 bool
		want         commitment.TrieVariant
	}{
		{false, false, commitment.VariantHexPatriciaTrie},
		{true, false, commitment.VariantParallelHexPatricia},
		{false, true, commitment.VariantCommitmentV3},
		{true, true, commitment.VariantCommitmentV3},
	} {
		t.Run(fmt.Sprintf("parallel=%t/v3=%t", tc.parallel, tc.v3), func(t *testing.T) {
			t.Cleanup(statecfg.SetCommitmentV3(tc.v3))
			parallel := statecfg.ExperimentalParallelCommitment
			t.Cleanup(func() { statecfg.ExperimentalParallelCommitment = parallel })
			statecfg.ExperimentalParallelCommitment = tc.parallel
			require.Equal(t, tc.want, PickTrieVariant(), "parallel=%v v3=%v", tc.parallel, tc.v3)
		})
	}
}

func TestParseTrieVariantKnowsV3(t *testing.T) {
	require.Equal(t, commitment.VariantCommitmentV3, commitment.ParseTrieVariant("v3"))
	require.Equal(t, commitment.VariantParallelHexPatricia, commitment.ParseTrieVariant("parallel"))
	require.Equal(t, commitment.VariantHexPatriciaTrie, commitment.ParseTrieVariant("hex"))
}
