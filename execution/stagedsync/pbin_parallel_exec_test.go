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

// The parallel executor's normalized write set roots differently under the bin trie
// than the serial path, so bin must run serially whatever the parallel toggles say.
func TestPBinExecuteInParallelExcludesBin(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name            string
		variant         commitment.TrieVariant
		exec3Parallel   bool
		experimentalBAL bool
		want            bool
	}{
		{name: "hex parallel", variant: commitment.VariantHexPatriciaTrie, exec3Parallel: true, want: true},
		{name: "hex bal", variant: commitment.VariantHexPatriciaTrie, experimentalBAL: true, want: true},
		{name: "hex serial", variant: commitment.VariantHexPatriciaTrie},
		{name: "parallel trie parallel", variant: commitment.VariantParallelHexPatricia, exec3Parallel: true, want: true},
		{name: "bin parallel", variant: commitment.VariantBinPatriciaTrie, exec3Parallel: true},
		{name: "bin bal", variant: commitment.VariantBinPatriciaTrie, experimentalBAL: true},
		{name: "bin serial", variant: commitment.VariantBinPatriciaTrie},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := executeInParallel(tc.variant, tc.exec3Parallel, tc.experimentalBAL)
			require.Equal(t, tc.want, got)
		})
	}
}
