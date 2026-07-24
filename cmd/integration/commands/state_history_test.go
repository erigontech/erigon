// Copyright 2024 The Erigon Authors
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

package commands

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHistDupScan(t *testing.T) {
	t.Parallel()

	feed := func(s *histDupScan, entries [][2]string) {
		for _, e := range entries {
			s.observe([]byte(e[0]), []byte(e[1]))
		}
		s.finish()
	}

	t.Run("mixed", func(t *testing.T) {
		t.Parallel()
		s := &histDupScan{sampleLimit: 10}
		feed(s, [][2]string{
			{"A", "v1"}, {"A", "v1"}, {"A", "v2"}, {"A", "v2"}, {"A", "v2"}, // 3 dup pairs
			{"B", "v1"}, {"B", "v2"}, // no dup
			{"C", "v1"}, {"C", "v1"}, // 1 dup pair
		})
		require.Equal(t, uint64(9), s.Entries)
		require.Equal(t, uint64(3), s.DistinctKeys)
		require.Equal(t, uint64(2), s.KeysWithDup) // A, C
		require.Equal(t, uint64(4), s.DupPairs)    // A:3 + C:1
		require.Equal(t, [][]byte{[]byte("A"), []byte("C")}, s.SampleKeys)
	})

	t.Run("no duplicates", func(t *testing.T) {
		t.Parallel()
		s := &histDupScan{sampleLimit: 10}
		feed(s, [][2]string{{"A", "v1"}, {"A", "v2"}, {"B", "v1"}})
		require.Equal(t, uint64(3), s.Entries)
		require.Equal(t, uint64(2), s.DistinctKeys)
		require.Zero(t, s.KeysWithDup)
		require.Zero(t, s.DupPairs)
		require.Empty(t, s.SampleKeys)
	})

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		s := &histDupScan{sampleLimit: 10}
		feed(s, nil)
		require.Zero(t, s.Entries)
		require.Zero(t, s.DistinctKeys)
		require.Zero(t, s.KeysWithDup)
		require.Zero(t, s.DupPairs)
	})

	t.Run("sample limit respected", func(t *testing.T) {
		t.Parallel()
		s := &histDupScan{sampleLimit: 1}
		feed(s, [][2]string{{"A", "v"}, {"A", "v"}, {"B", "v"}, {"B", "v"}})
		require.Equal(t, uint64(2), s.KeysWithDup)
		require.Len(t, s.SampleKeys, 1) // capped at sampleLimit
	})
}
