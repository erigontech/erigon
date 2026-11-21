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

package exec

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
	commitmentv3 "github.com/erigontech/erigon/execution/commitment/v3"
)

func TestV3TrieKeysAreDistinctAndCoverEveryPath(t *testing.T) {
	addr := make([]byte, 20)
	addr[19] = 1
	s1, s2 := append(append([]byte{}, addr...), make([]byte, 32)...), append(append([]byte{}, addr...), make([]byte, 32)...)
	s2[51] = 2
	hashed := [][]byte{commitment.KeyToHexNibbleHash(addr), commitment.KeyToHexNibbleHash(s1), commitment.KeyToHexNibbleHash(s2)}

	keys := v3TrieKeys(hashed)
	seen := map[string]bool{}
	for _, k := range keys {
		require.Falsef(t, seen[string(k)], "duplicate key %x", k)
		seen[string(k)] = true
	}
	for _, hk := range hashed {
		for _, k := range commitmentv3.PathNodeKeys(nil, hk, prefetchFlatDepth) {
			require.Truef(t, seen[string(k)], "path key %x missing", k)
		}
	}
	require.Less(t, len(keys), 3*(prefetchFlatDepth+1), "the slots share their storage root")
}
