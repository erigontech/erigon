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

package commitmentdb

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
)

// The bin trie hashes code_size into BASIC_DATA and gates code chunking on it.
// A context that leaves it unread hands the trie a codeless account, which the
// trie refuses rather than hashing a tree with no code zone.
func TestPBinTrieContextReadsCodeSizeOnlyUnderBin(t *testing.T) {
	t.Parallel()

	binCtx := pbinStateTestCtx(t, commitment.VariantBinPatriciaTrie)
	require.True(t, binCtx.trieContext(nil, 0, 0, nil).readCodeSize)

	hexCtx := pbinStateTestCtx(t, commitment.VariantHexPatriciaTrie)
	require.False(t, hexCtx.trieContext(nil, 0, 0, nil).readCodeSize)
}
