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

package state

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/murmur3"
	"github.com/erigontech/erigon/db/datastruct/existence"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

const pruneTestSalt = uint32(0x5eed)

func filterOver(t *testing.T, keys ...[]byte) *existence.Filter {
	t.Helper()
	path := filepath.Join(t.TempDir(), "test.kvei")
	w, err := existence.NewFilter(uint64(len(keys)), path)
	require.NoError(t, err)
	w.DisableFsync()
	for _, k := range keys {
		hi, _ := murmur3.Sum128WithSeed(k, pruneTestSalt)
		require.NoError(t, w.AddHash(hi))
	}
	require.NoError(t, w.Build())
	// NewFilter yields a writer; only OpenFilter can be queried.
	f, err := existence.OpenFilter(path, false)
	require.NoError(t, err)
	t.Cleanup(f.Close)
	return f
}

func unrelatedKeys(n int) [][]byte {
	keys := make([][]byte, 0, n)
	for i := range n {
		keys = append(keys, []byte{0xff, 0xff, byte(i)})
	}
	return keys
}

func pruneProbe(t *testing.T, filter *existence.Filter, nodeKey []byte, missing uint16) bool {
	t.Helper()
	childKey := make([]byte, len(nodeKey)+1)
	copy(childKey, nodeKey)
	var hashes [16]uint64
	var hashed uint16
	return childMayBeInFile(pruneTestSalt, filter, nodeKey, missing, childKey, &hashes, &hashed)
}

// Skipping a file that holds a wanted record loses it silently and corrupts the root, so the
// probe must answer "may be present" for every child actually in the filter.
func TestExistencePruneNeverHidesAPresentChild(t *testing.T) {
	t.Parallel()

	nodeKey := []byte{0x12, 0x34, 0x00}
	present := nibbles.ChildKeyV3(nodeKey, 7)

	filter := filterOver(t, append(unrelatedKeys(64), present)...)

	require.True(t, pruneProbe(t, filter, nodeKey, 1<<7), "the child in the filter must never be pruned")
	require.True(t, pruneProbe(t, filter, nodeKey, ^uint16(0)), "a full mask covers the present child")
	require.True(t, pruneProbe(t, filter, nodeKey, 1<<7|1<<9), "a mask containing the present child")
}

// An empty filter is fail-open: a file with too few keys to build one must still be scanned.
func TestExistencePruneFailsOpenOnEmptyFilter(t *testing.T) {
	t.Parallel()

	filter := filterOver(t)
	require.True(t, pruneProbe(t, filter, []byte{0x00}, ^uint16(0)))
}

// The per-nibble hash cache is reused across files; a wrong index would probe one child's hash
// for another and prune the wrong file.
func TestExistencePruneHashesEachNibbleSeparately(t *testing.T) {
	t.Parallel()

	nodeKey := []byte{0xab, 0x00}
	childKey := make([]byte, len(nodeKey)+1)
	copy(childKey, nodeKey)
	var hashes [16]uint64
	var hashed uint16

	// A filter holding none of this node's children: the probe runs every nibble and prunes.
	filter := filterOver(t, unrelatedKeys(64)...)
	require.False(t, childMayBeInFile(pruneTestSalt, filter, nodeKey, ^uint16(0), childKey, &hashes, &hashed),
		"a file holding none of the wanted children must be skipped")

	for nibble := range 16 {
		want, _ := murmur3.Sum128WithSeed(nibbles.ChildKeyV3(nodeKey, byte(nibble)), pruneTestSalt)
		require.Equalf(t, want, hashes[nibble], "cached hash for nibble %d is not that nibble's child key", nibble)
	}
}
