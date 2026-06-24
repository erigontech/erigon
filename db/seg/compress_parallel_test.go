// Copyright 2025 The Erigon Authors
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
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

// The parallel cover phase hands each worker a batch of consecutive words. This
// verifies it round-trips and produces byte-identical output to the single-worker
// path, across enough words to cross several batch boundaries.
func TestCompressParallelBatchingRoundTrip(t *testing.T) {
	logger := log.New()

	// Repetitive corpus so patterns are mined and the cover phase actually runs;
	// 4000 words >> coverBatchSize (512) so multiple batches per worker.
	const n = 4000
	words := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		words = append(words, fmt.Appendf(nil, "prefix-deadbeefcafe-%04d-deadbeefcafe-suffix", i%37))
	}
	words = append(words, []byte{}, []byte("zzz-unique-tail"))

	compressTo := func(workers int) string {
		tmpDir := t.TempDir()
		file := filepath.Join(tmpDir, fmt.Sprintf("compressed-w%d", workers))
		cfg := DefaultCfg
		cfg.MinPatternScore = 64
		cfg.Workers = workers
		c, err := NewCompressor(t.Context(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
		require.NoError(t, err)
		defer c.Close()
		for _, w := range words {
			require.NoError(t, c.AddWord(w))
		}
		require.NoError(t, c.Compress())
		return file
	}

	readBack := func(file string) [][]byte {
		d, err := NewDecompressor(file)
		require.NoError(t, err)
		defer d.Close()
		g := d.MakeGetter()
		out := make([][]byte, 0, len(words))
		var buf []byte
		for g.HasNext() {
			buf, _ = g.Next(buf[:0])
			out = append(out, append([]byte{}, buf...))
		}
		return out
	}

	single := compressTo(1)
	got := readBack(single)
	require.Len(t, got, len(words))
	for i := range words {
		require.Equalf(t, words[i], got[i], "round-trip mismatch at word %d", i)
	}

	for _, workers := range []int{2, 4, 8} {
		parallel := compressTo(workers)
		require.Equalf(t, readBack(single), readBack(parallel), "decoded words differ at Workers=%d", workers)
		require.Equalf(t, checksum(single), checksum(parallel), "output not byte-identical at Workers=%d", workers)
	}
}
