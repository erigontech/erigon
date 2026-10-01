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

package state_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/commitment"
)

func TestRebuildTargetResolve(t *testing.T) {
	for _, variant := range []commitment.TrieVariant{
		commitment.VariantHexPatriciaTrie,
		commitment.VariantParallelHexPatricia,
		commitment.VariantCommitmentV3,
	} {
		resolved, err := state.RebuildTarget{Variant: variant}.Resolve()
		require.NoError(t, err)
		require.Equal(t, variant, resolved.Variant)
	}

	_, err := state.RebuildTarget{Variant: commitment.VariantBinPatriciaTrie}.Resolve()
	require.ErrorContains(t, err, "convert-pbt")

	_, err = state.RebuildTarget{Variant: "verkle"}.Resolve()
	require.Error(t, err)

	resolved, err := state.RebuildTarget{MaxShardSteps: 16}.Resolve()
	require.NoError(t, err)
	require.Equal(t, uint64(16), resolved.MaxShardSteps)
}
