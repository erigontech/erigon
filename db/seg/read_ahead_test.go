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

package seg

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

// ps is a fixed page size used in unit tests for determinism.
const ps = uint64(4096)

// newTestRAC builds a ReadAheadController for unit-testing computeRanges
// without starting the goroutine.
func newTestRAC(dataLen int, aheadSize, trailSize int64) *ReadAheadController {
	return &ReadAheadController{
		data:      make([]byte, dataLen),
		aheadSize: aheadSize,
		trailSize: trailSize,
		pageSize:  ps,
	}
}

func TestComputeRanges(t *testing.T) {
	tests := []struct {
		name            string
		dataLen         uint64
		aheadSize       int64
		trailSize       int64
		curPos          uint64
		prefetchedUntil uint64
		releasedUntil   uint64
		// expected frontiers
		wantPrefUntil uint64
		wantRelUntil  uint64
		// expected slices: nil means we assert the slice IS nil; non-nil means
		// we assert it is NOT nil and check its length.
		wantPrefLen *uint64
		wantRelLen  *uint64
	}{
		{
			name:          "initial state: prefetch window issued, no release",
			dataLen:       32 * ps,
			aheadSize:     4 * int64(ps),
			curPos:        0,
			wantPrefUntil: 4 * ps,
			wantPrefLen:   ptr(4 * ps),
		},
		{
			name:            "no progress: curPos unchanged, no new work",
			dataLen:         32 * ps,
			aheadSize:       4 * int64(ps),
			curPos:          0,
			prefetchedUntil: 4 * ps, // already seeded
			wantPrefUntil:   4 * ps,
		},
		{
			name:            "advance: reader moves forward, window extends",
			dataLen:         32 * ps,
			aheadSize:       4 * int64(ps),
			curPos:          4 * ps,
			prefetchedUntil: 4 * ps,
			wantPrefUntil:   8 * ps,
			wantPrefLen:     ptr(4 * ps),
		},
		{
			name:          "trail disabled: no release even when reader is deep",
			dataLen:       32 * ps,
			aheadSize:     4 * int64(ps),
			trailSize:     0,
			curPos:        20 * ps,
			wantPrefUntil: 24 * ps,
			wantPrefLen:   ptr(24 * ps),
		},
		{
			name:          "trail enabled: release issued when reader is far enough",
			dataLen:       32 * ps,
			aheadSize:     4 * int64(ps),
			trailSize:     4 * int64(ps),
			curPos:        8 * ps,
			wantPrefUntil: 12 * ps,
			wantRelUntil:  4 * ps,
			wantPrefLen:   ptr(12 * ps),
			wantRelLen:    ptr(4 * ps),
		},
		{
			name:          "trail: curPos < trailSize, nothing released yet",
			dataLen:       32 * ps,
			aheadSize:     4 * int64(ps),
			trailSize:     4 * int64(ps),
			curPos:        2 * ps,
			wantPrefUntil: 6 * ps,
			wantPrefLen:   ptr(6 * ps),
		},
		{
			name:          "end clamping: aheadSize overshoots dataLen",
			dataLen:       6 * ps,
			aheadSize:     4 * int64(ps),
			curPos:        4 * ps,
			wantPrefUntil: 6 * ps,
			wantPrefLen:   ptr(6 * ps),
		},
		{
			name:            "already fully prefetched: no new work",
			dataLen:         8 * ps,
			aheadSize:       4 * int64(ps),
			curPos:          4 * ps,
			prefetchedUntil: 8 * ps,
			wantPrefUntil:   8 * ps,
		},
		{
			name:          "page alignment: non-page-multiple aheadSize and trailSize",
			dataLen:       64 * ps,
			aheadSize:     5*int64(ps) + 17,
			trailSize:     3*int64(ps) + 13,
			curPos:        10*ps + 500,
			wantPrefUntil: 16 * ps, // alignUp(15*ps + 517)
			wantRelUntil:  7 * ps,  // alignDown(7*ps + 487)
			wantPrefLen:   ptr(16 * ps),
			wantRelLen:    ptr(7 * ps),
		},
	} //nolint:govet

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rac := newTestRAC(int(tc.dataLen), tc.aheadSize, tc.trailSize)
			pNew, rNew, prefetch, release := rac.computeRanges(tc.curPos, tc.prefetchedUntil, tc.releasedUntil)

			// Frontiers must always be page-aligned.
			require.Zero(t, pNew%ps, "leading frontier must be page-aligned")
			require.Zero(t, rNew%ps, "trailing frontier must be page-aligned")

			require.Equal(t, tc.wantPrefUntil, pNew)
			require.Equal(t, tc.wantRelUntil, rNew)

			if tc.wantPrefLen != nil {
				require.NotNil(t, prefetch)
				require.Equal(t, *tc.wantPrefLen, uint64(len(prefetch)))
			} else {
				require.Nil(t, prefetch)
			}

			if tc.wantRelLen != nil {
				require.NotNil(t, release)
				require.Equal(t, *tc.wantRelLen, uint64(len(release)))
			} else {
				require.Nil(t, release)
			}
		})
	}
}

func TestPageAlign(t *testing.T) {
	const pageSize = uint64(4096)

	tests := []struct {
		v        uint64
		wantDown uint64
		wantUp   uint64
	}{
		{0, 0, 0},
		{1, 0, 4096},
		{4095, 0, 4096},
		{4096, 4096, 4096},
		{4097, 4096, 8192},
		{8191, 4096, 8192},
		{8192, 8192, 8192},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("v=%d", tc.v), func(t *testing.T) {
			require.Equal(t, tc.wantDown, pageAlignDown(tc.v, pageSize))
			require.Equal(t, tc.wantUp, pageAlignUp(tc.v, pageSize))
		})
	}
}

// ---- integration tests (compress + decompress round-trip) ------------------

// makeCompressedFile creates a compressed file and returns an open Decompressor
// and the original words.  The caller is responsible for calling d.Close().
func makeCompressedFile(t *testing.T, wordCount, wordSize int) (*Decompressor, [][]byte) {
	t.Helper()
	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "compressed")

	words := make([][]byte, wordCount)
	for i := range words {
		buf := make([]byte, wordSize)
		for j := range buf {
			buf[j] = byte('a' + (i*7+j*3)%26)
		}
		words[i] = buf
	}

	cfg := DefaultCfg
	cfg.Workers = 2
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
	require.NoError(t, err)
	defer c.Close()
	for _, w := range words {
		require.NoError(t, c.AddWord(w))
	}
	require.NoError(t, c.Compress())

	d, err := NewDecompressor(file)
	require.NoError(t, err)
	return d, words
}

func TestReadAheadController(t *testing.T) {
	t.Run("nil safe", func(t *testing.T) {
		var rac *ReadAheadController
		rac.Close() // must not panic
	})

	t.Run("look-ahead only", func(t *testing.T) {
		d, words := makeCompressedFile(t, 1000, 1024)
		defer d.Close()

		g := d.MakeGetter()
		defer g.StartReadAhead(DefaultAheadSize, 0).Close()

		var buf []byte
		for i := 0; g.HasNext(); i++ {
			buf, _ = g.Next(buf[:0])
			if !bytes.Equal(buf, words[i]) {
				t.Fatalf("word %d mismatch: got %q, want %q", i, buf[:min(len(buf), 20)], words[i][:20])
			}
		}
		require.Equal(t, len(words), d.Count())
	})

	t.Run("look-ahead with trailing release", func(t *testing.T) {
		d, words := makeCompressedFile(t, 1000, 1024)
		defer d.Close()

		g := d.MakeGetter()
		defer g.StartReadAhead(DefaultAheadSize, DefaultTrailSize).Close()

		var buf []byte
		for i := 0; g.HasNext(); i++ {
			buf, _ = g.Next(buf[:0])
			require.Equal(t, words[i], buf)
		}
	})

	t.Run("skip-based reading", func(t *testing.T) {
		d, words := makeCompressedFile(t, 500, 32)
		defer d.Close()

		g := d.MakeGetter()
		defer g.StartReadAhead(DefaultAheadSize, 0).Close()

		var buf []byte
		for i := 0; g.HasNext(); i++ {
			if i%2 == 0 {
				g.Skip()
			} else {
				buf, _ = g.Next(buf[:0])
				require.Equal(t, words[i], buf)
			}
		}
	})
}

// ptr returns a pointer to v, for use in table test fields.
func ptr(v uint64) *uint64 { return &v }

// TestMadvCoverage verifies that after a full sequential read (possibly with
// a mid-stream Reset), the union of all MADV_WILLNEED regions equals the
// union of all MADV_RANDOM regions (including Close's final flush).  This
// proves that no page is left in an unexpected madvise state regardless of
// how curPos moves.
//
// The test is fully deterministic: it drives computeRanges directly (no
// goroutine, no sleeps) and accumulates covered byte counts from the
// returned frontier deltas.
func TestMadvCoverage(t *testing.T) {
	// runScenario drives computeRanges for each position in the sequence,
	// accumulates the willneed and random byte counts from frontier deltas,
	// then simulates Close()'s final flush.  Returns (willneedBytes, randomBytes).
	//
	// Because frontiers are monotonically increasing, successive calls add
	// non-overlapping intervals so simple summation gives the total coverage.
	runScenario := func(t *testing.T, dataLen uint64, aheadSize, trailSize int64, positions []uint64) (uint64, uint64) {
		t.Helper()
		rac := &ReadAheadController{
			data:      make([]byte, dataLen),
			aheadSize: aheadSize,
			trailSize: trailSize,
			pageSize:  ps,
		}
		var willneedTotal, randomTotal uint64
		pUntil, rUntil := uint64(0), uint64(0)
		for _, pos := range positions {
			pNew, rNew, prefetch, release := rac.computeRanges(pos, pUntil, rUntil)
			if prefetch != nil {
				willneedTotal += pNew - pUntil
			}
			if release != nil {
				randomTotal += rNew - rUntil
			}
			pUntil, rUntil = pNew, rNew
		}
		// Simulate Close() final flush: reset remaining prefetched-but-not-released region.
		if rUntil < pUntil {
			randomTotal += pUntil - rUntil
		}
		return willneedTotal, randomTotal
	}

	t.Run("full read no trail: willneed==random==dataLen", func(t *testing.T) {
		dataLen := uint64(64 * ps)
		aheadSize := int64(8 * ps)
		// Simulate reader advancing one page at a time through the whole file.
		positions := make([]uint64, 0, int(dataLen/ps)+1)
		for pos := uint64(0); pos <= dataLen; pos += ps {
			positions = append(positions, pos)
		}
		wn, rnd := runScenario(t, dataLen, aheadSize, 0, positions)
		require.Equal(t, dataLen, wn, "willneed must cover entire file")
		require.Equal(t, dataLen, rnd, "random must cover entire file after Close flush")
	})

	t.Run("full read with trail: willneed==random==dataLen", func(t *testing.T) {
		dataLen := uint64(64 * ps)
		aheadSize := int64(8 * ps)
		trailSize := int64(4 * ps)
		positions := make([]uint64, 0, int(dataLen/ps)+1)
		for pos := uint64(0); pos <= dataLen; pos += ps {
			positions = append(positions, pos)
		}
		wn, rnd := runScenario(t, dataLen, aheadSize, trailSize, positions)
		require.Equal(t, dataLen, wn, "willneed must cover entire file")
		require.Equal(t, dataLen, rnd, "random must cover entire file after Close flush")
	})

	t.Run("reset mid-read: frontiers never regress, coverage still complete", func(t *testing.T) {
		dataLen := uint64(64 * ps)
		aheadSize := int64(8 * ps)
		trailSize := int64(4 * ps)
		// Advance to halfway, then Reset (curPos jumps to 0), then advance to end.
		// Monotonically-increasing frontiers must not regress on the backward jump,
		// and Close() must still flush the entire prefetched region to MADV_RANDOM.
		positions := make([]uint64, 0)
		for pos := uint64(0); pos <= dataLen/2; pos += ps {
			positions = append(positions, pos)
		}
		// Simulate Reset() — curPos jumps back to 0.
		positions = append(positions, 0)
		// Continue to end from the start (the second pass fills in the rest).
		for pos := uint64(0); pos <= dataLen; pos += ps {
			positions = append(positions, pos)
		}
		wn, rnd := runScenario(t, dataLen, aheadSize, trailSize, positions)
		require.Equal(t, dataLen, wn, "willneed must cover entire file despite reset")
		require.Equal(t, dataLen, rnd, "random must cover entire file despite reset")
	})
}
