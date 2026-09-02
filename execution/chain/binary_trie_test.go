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

package chain

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsBinaryTrie(t *testing.T) {
	t.Parallel()

	ten := uint64(10)
	c := &Config{BinaryTrieTime: &ten}
	require.False(t, c.IsBinaryTrie(9))
	require.True(t, c.IsBinaryTrie(10))
	require.True(t, c.IsBinaryTrie(11))

	require.False(t, (&Config{}).IsBinaryTrie(0), "an unscheduled binary trie is never active")
}
