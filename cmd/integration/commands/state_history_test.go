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
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
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

// TestHistDupSorter_FileOrderIndependent pins the reason the scan sorts at all:
// HistoryDump yields entries file-major, so a key's chain arrives split across
// files with unrelated keys in between. Counting on that raw order misses a
// duplicate pair straddling a file boundary and counts one key many times.
func TestHistDupSorter_FileOrderIndependent(t *testing.T) {
	t.Parallel()

	type entry struct {
		key   string
		txNum uint64
		val   string
	}
	// Key A: v1@1, v1@3 — a duplicate pair straddling the file boundary.
	// Key B: v1@2, v2@4 — no duplicate, and it separates A's two entries.
	fileMajor := []entry{
		{"A", 1, "v1"}, {"B", 2, "v1"}, // .ef file 1
		{"A", 3, "v1"}, {"B", 4, "v2"}, // .ef file 2
	}

	run := func(t *testing.T, entries []entry) *histDupScan {
		t.Helper()
		sorter := newHistDupSorter(t.Name(), t.TempDir(), log.New())
		t.Cleanup(sorter.Close)
		for _, e := range entries {
			require.NoError(t, sorter.add([]byte(e.key), e.txNum, []byte(e.val)))
		}
		scan, err := sorter.scan(t.Context(), 10)
		require.NoError(t, err)
		return scan
	}

	got := run(t, fileMajor)
	require.Equal(t, uint64(4), got.Entries)
	require.Equal(t, uint64(2), got.DistinctKeys, "a key spanning two files is still one key")
	require.Equal(t, uint64(1), got.DupPairs, "the pair straddling the file boundary must be counted")
	require.Equal(t, uint64(1), got.KeysWithDup)
	require.Equal(t, [][]byte{[]byte("A")}, got.SampleKeys)

	// Same entries handed over in a different order must produce the same report.
	shuffled := []entry{fileMajor[3], fileMajor[0], fileMajor[2], fileMajor[1]}
	require.Equal(t, got, run(t, shuffled))
}

func TestHistDupSorter_KeepsEmptyValues(t *testing.T) {
	t.Parallel()

	sorter := newHistDupSorter(t.Name(), t.TempDir(), log.New())
	t.Cleanup(sorter.Close)
	// An empty value is a deletion marker, and two in a row are as redundant as
	// any other repeat — ETL must not drop them.
	require.NoError(t, sorter.add([]byte("A"), 1, nil))
	require.NoError(t, sorter.add([]byte("A"), 2, []byte{}))

	scan, err := sorter.scan(t.Context(), 10)
	require.NoError(t, err)
	require.Equal(t, uint64(2), scan.Entries)
	require.Equal(t, uint64(1), scan.DupPairs)
}

func TestStepToTxNum_SaturatesInsteadOfWrapping(t *testing.T) {
	t.Parallel()

	// The --to default: 1e18 steps times any real step size overflows uint64.
	got, err := stepToTxNum(1e18, 1_562_500)
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxUint64), got, "an out-of-range bound must saturate, not wrap")

	got, err = stepToTxNum(4, 1_562_500)
	require.NoError(t, err)
	require.Equal(t, uint64(6_250_000), got)

	_, err = stepToTxNum(1, 0)
	require.Error(t, err)
}

func TestDumpBounds_UnboundedWhenOutOfIntRange(t *testing.T) {
	t.Parallel()

	from, to := dumpBounds(10, math.MaxUint64)
	require.Equal(t, 10, from)
	require.Equal(t, -1, to, "HistoryDump reads -1 as unbounded")

	from, to = dumpBounds(0, 100)
	require.Equal(t, 0, from)
	require.Equal(t, 100, to)
}
