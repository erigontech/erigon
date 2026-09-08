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

package posidx

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func build(t *testing.T, name string, pageSize uint64, itemsPerRun, pageValues []uint64) *Index {
	t.Helper()
	var items uint64
	for _, n := range itemsPerRun {
		items += n
	}
	path := filepath.Join(t.TempDir(), name)
	w, err := NewWriter(path, t.TempDir(), pageSize, uint64(len(itemsPerRun)), items, pageValues[len(pageValues)-1])
	require.NoError(t, err)
	defer w.Close()
	w.NoFsync()
	for _, n := range itemsPerRun {
		w.AddRun(n)
	}
	for _, v := range pageValues {
		w.AddPage(v)
	}
	require.NoError(t, w.Build())

	idx, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(idx.Close)
	return idx
}

func TestGet(t *testing.T) {
	itemsPerRun := []uint64{3, 1, 5, 2, 1}
	const pageSize = 2
	var items uint64
	for _, n := range itemsPerRun {
		items += n
	}
	pageValues := make([]uint64, (items+pageSize-1)/pageSize)
	for i := range pageValues {
		pageValues[i] = uint64(i) * 37 // monotone, uneven gaps
	}

	idx := build(t, "get", pageSize, itemsPerRun, pageValues)

	var ordinal uint64
	for run, n := range itemsPerRun {
		for item := range n {
			v, ok := idx.Get(uint64(run), item)
			require.True(t, ok, "run %d item %d", run, item)
			require.Equal(t, pageValues[ordinal/pageSize], v, "run %d item %d", run, item)
			ordinal++
		}
	}
	require.Equal(t, items, ordinal)
}

func TestSingleItem(t *testing.T) {
	idx := build(t, "one", 64, []uint64{1}, []uint64{0})
	v, ok := idx.Get(0, 0)
	require.True(t, ok)
	require.Equal(t, uint64(0), v)
}

// A position from a file that does not belong to this index must not panic.
func TestOutOfRange(t *testing.T) {
	idx := build(t, "oob", 2, []uint64{3, 1}, []uint64{0, 37})
	_, ok := idx.Get(99, 0)
	require.False(t, ok, "run past the end")
	_, ok = idx.Get(0, 1<<40)
	require.False(t, ok, "item past the end")
}
func TestEmpty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "empty")
	w, err := NewWriter(path, t.TempDir(), 64, 0, 0, 0)
	require.NoError(t, err)
	defer w.Close()
	w.NoFsync()
	require.NoError(t, w.Build())

	idx, err := Open(path)
	require.NoError(t, err)
	defer idx.Close()

	require.True(t, idx.Empty())
	_, ok := idx.Get(0, 0)
	require.False(t, ok)
}

func TestPageSizeZeroRejected(t *testing.T) {
	_, err := NewWriter(filepath.Join(t.TempDir(), "bad"), t.TempDir(), 0, 1, 1, 1)
	require.Error(t, err)
}

// An item past its own run belongs to no run, even when the ordinal it lands on
// is a valid position in the next one.
func TestItemPastRunEnd(t *testing.T) {
	idx := build(t, "past-run", 2, []uint64{3, 1}, []uint64{0, 37})
	_, ok := idx.Get(0, 3) // ordinal 3 is run 1's only item
	require.False(t, ok)
	_, ok = idx.Get(1, 1)
	require.False(t, ok, "one past the last item of the last run")
}

// A truncated or forged file must be reported, not panicked on: openDirtyAccessor
// rebuilds an accessor it cannot open.
func TestOpenCorrupt(t *testing.T) {
	good := filepath.Join(t.TempDir(), "good")
	w, err := NewWriter(good, t.TempDir(), 2, 2, 4, 100)
	require.NoError(t, err)
	defer w.Close()
	w.NoFsync()
	w.AddRun(3)
	w.AddRun(1)
	w.AddPage(0)
	w.AddPage(100)
	require.NoError(t, w.Build())
	full, err := os.ReadFile(good)
	require.NoError(t, err)

	for _, size := range []int{headerLen + 1, headerLen + 16, len(full) - 1} {
		path := filepath.Join(t.TempDir(), "trunc")
		require.NoError(t, os.WriteFile(path, full[:size], 0o644))
		idx, err := Open(path)
		require.Error(t, err, "size %d", size)
		require.Nil(t, idx, "size %d", size)
	}
}

// The sequences are read as []uint64 out of the mapping, so the header must
// leave them 8-byte aligned. A header that is not a multiple of 8 makes every
// lookup an unaligned load.
func TestHeaderKeepsSequencesAligned(t *testing.T) {
	require.Zero(t, headerLen%8, "header must be a multiple of 8, is %d", headerLen)
	// ReadEliasFano puts its data 16 bytes into what it is given, so both
	// sequences inherit the header's alignment.
	require.Zero(t, (headerLen+16)%8)
}

func FuzzOpen(f *testing.F) {
	good := filepath.Join(f.TempDir(), "seed.vi")
	w, err := NewWriter(good, f.TempDir(), 2, 3, 6, 90)
	require.NoError(f, err)
	defer w.Close()
	w.NoFsync()
	for _, n := range []uint64{3, 1, 2} {
		w.AddRun(n)
	}
	for _, v := range []uint64{0, 40, 90} {
		w.AddPage(v)
	}
	require.NoError(f, w.Build())
	body, err := os.ReadFile(good)
	require.NoError(f, err)
	f.Add(body)
	f.Add(body[:headerLen])
	f.Add(body[:len(body)-1])

	dir := f.TempDir()
	f.Fuzz(func(t *testing.T, b []byte) {
		path := filepath.Join(dir, "fuzz.vi")
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Skip()
		}
		// Open must reject a malformed file rather than crash on it, and must
		// not accept one that a lookup then crashes on.
		idx, err := Open(path)
		if err != nil {
			return
		}
		defer idx.Close()
		for run := range uint64(4) {
			for item := range uint64(4) {
				idx.Get(run, item)
			}
		}
	})
}

// Elias-Fano decoding walks the upper bits until it has seen as many set bits
// as the index asked for, indexing the slice unchecked. Open must reject a file
// whose bits no longer match its header, or that walk runs off the end during a
// lookup instead.
func TestOpenRejectsBitStreamNotMatchingHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "src.vi")
	w, err := NewWriter(path, t.TempDir(), 1, 3, 3, 30)
	require.NoError(t, err)
	defer w.Close()
	w.NoFsync()
	for range 3 {
		w.AddRun(1)
	}
	for _, v := range []uint64{10, 20, 30} {
		w.AddPage(v)
	}
	require.NoError(t, w.Build())
	body, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NotPanics(t, func() {
		idx, err := Open(path)
		require.NoError(t, err)
		idx.Close()
	})

	// Clearing bits anywhere past the header leaves the counts intact but the
	// stream short of the values they promise.
	for i := headerLen + 16; i < len(body); i++ {
		if body[i] == 0 {
			continue
		}
		corrupt := append([]byte(nil), body...)
		corrupt[i] = 0
		bad := filepath.Join(t.TempDir(), "corrupt.vi")
		require.NoError(t, os.WriteFile(bad, corrupt, 0o644))
		idx, err := Open(bad)
		if err == nil {
			idx.Close()
			continue // that byte was not part of the upper bits
		}
		require.ErrorIs(t, err, ErrCorrupt)
		return
	}
	t.Fatal("no byte of the sequence was load-bearing")
}

// Open must not accept a file that a lookup then walks off the end of. One bit
// at a time over a whole index covers what a partial write or a bad block does
// to the layout, without the header stopping being plausible.
func TestNoAcceptedMutationPanics(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seed.vi")
	w, err := NewWriter(path, t.TempDir(), 2, 3, 6, 90)
	require.NoError(t, err)
	defer w.Close()
	w.NoFsync()
	for _, n := range []uint64{3, 1, 2} {
		w.AddRun(n)
	}
	for _, v := range []uint64{0, 40, 90} {
		w.AddPage(v)
	}
	require.NoError(t, w.Build())
	body, err := os.ReadFile(path)
	require.NoError(t, err)

	dir := t.TempDir()
	accepted := 0
	for i := range body {
		for bit := range 8 {
			mutated := append([]byte(nil), body...)
			mutated[i] ^= 1 << bit
			mutatedPath := filepath.Join(dir, "mutated.vi")
			require.NoError(t, os.WriteFile(mutatedPath, mutated, 0o644))
			idx, err := Open(mutatedPath)
			if err != nil {
				continue
			}
			accepted++
			require.NotPanics(t, func() {
				for run := range uint64(4) {
					for item := range uint64(4) {
						idx.Get(run, item)
					}
				}
			}, "byte %d bit %d", i, bit)
			idx.Close()
		}
	}
	require.NotZero(t, accepted, "nothing was accepted, the survey proves nothing")
}
