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

package stagedsync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
)

// The commitment context panics on a deferral request under the bin variant, so
// ExecV3 must never make one.
func TestPBinDeferCommitmentUpdatesExcludesBin(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name             string
		variant          commitment.TrieVariant
		isForkValidation bool
		parallel         bool
		isApplyingBlocks bool
		want             bool
	}{
		{name: "hex fork validation", variant: commitment.VariantHexPatriciaTrie, isForkValidation: true, want: true},
		{name: "hex parallel apply", variant: commitment.VariantHexPatriciaTrie, parallel: true, isApplyingBlocks: true, want: true},
		{name: "hex parallel not applying", variant: commitment.VariantHexPatriciaTrie, parallel: true},
		{name: "hex serial apply", variant: commitment.VariantHexPatriciaTrie, isApplyingBlocks: true},
		{name: "parallel trie fork validation", variant: commitment.VariantParallelHexPatricia, isForkValidation: true, want: true},
		{name: "bin fork validation", variant: commitment.VariantBinPatriciaTrie, isForkValidation: true},
		{name: "bin parallel apply", variant: commitment.VariantBinPatriciaTrie, parallel: true, isApplyingBlocks: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := deferCommitmentUpdates(tc.variant, tc.isForkValidation, tc.parallel, tc.isApplyingBlocks)
			require.Equal(t, tc.want, got)
		})
	}
}
