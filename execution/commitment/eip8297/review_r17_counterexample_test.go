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

package eip8297

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReviewR17ParallelRejectsCrossShardPrefix(t *testing.T) {
	serial, err := NewStreamRootBuilder(streamRootHash(HashKeccak))
	require.NoError(t, err)
	parallel, err := NewParallelStreamRoot(streamRootHash(HashKeccak), 2)
	require.NoError(t, err)
	value := streamRootValue(1)
	require.NoError(t, serial.Add([]byte{0x12}, value))
	require.ErrorContains(t, serial.Add([]byte{0x12, 0x80}, value), "not prefix-free")
	require.NoError(t, parallel.Add([]byte{0x12}, value))
	require.NoError(t, parallel.Add([]byte{0x12, 0x80}, value))
	root, err := parallel.RootHash()
	require.Error(t, err, "parallel accepted prefix-related keys 12 and 1280: root=%x, err=%v", root, err)
}

func TestReviewR17ParallelBufferReuse(t *testing.T) {
	entries := parallelStreamRootAccountEntries(2)
	sum := streamRootHash(HashKeccak)
	serial, err := NewStreamRootBuilder(sum)
	require.NoError(t, err)
	for _, e := range entries {
		require.NoError(t, serial.Add(e.Key, e.Value))
	}
	want, err := serial.RootHash()
	require.NoError(t, err)
	parallel, err := NewParallelStreamRoot(sum, 1)
	require.NoError(t, err)
	require.NoError(t, parallel.Add(entries[0].Key, entries[0].Value))
	require.NoError(t, parallel.Add(entries[1].Key, entries[1].Value))
	entries[1].Value[0] = 0x7f
	got, err := parallel.RootHash()
	require.NoError(t, err)
	require.Equal(t, want, got, "root changed after reuse of a buffer passed to Add")
}
