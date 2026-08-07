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
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

// testWord carries the content plus how it should be added (compressed vs not), so a
// single corpus definition drives every worker count identically.
type testWord struct {
	data       []byte
	compressed bool
}

// compressCorpus writes corpus with the given worker count and returns the file path.
func compressCorpus(t *testing.T, corpus []testWord, workers int) string {
	t.Helper()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, fmt.Sprintf("compressed-w%d", workers))
	cfg := DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = workers
	c, err := NewCompressor(t.Context(), t.Name(), file, tmpDir, cfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	defer c.Close()
	for _, w := range corpus {
		if w.compressed {
			require.NoError(t, c.AddWord(w.data))
		} else {
			require.NoError(t, c.AddUncompressedWord(w.data))
		}
	}
	require.NoError(t, c.Compress())
	return file
}

func readBack(t *testing.T, file string) [][]byte {
	t.Helper()
	d, err := NewDecompressor(file)
	require.NoError(t, err)
	defer d.Close()
	g := d.MakeGetter()
	out := make([][]byte, 0)
	var buf []byte
	for g.HasNext() {
		buf, _ = g.Next(buf[:0])
		out = append(out, append([]byte{}, buf...))
	}
	return out
}

func assertSameOutputEveryWorkerCount(t *testing.T, corpus []testWord, wantSum uint32, workers ...int) {
	t.Helper()
	for _, w := range workers {
		file := compressCorpus(t, corpus, w)
		got := readBack(t, file)
		require.Lenf(t, got, len(corpus), "word count differs at Workers=%d", w)
		for i := range corpus {
			require.Equalf(t, corpus[i].data, got[i], "round-trip mismatch at word %d, Workers=%d", i, w)
		}
		require.Equalf(t, wantSum, checksum(file), "output differs from the pre-batching encoder at Workers=%d", w)
	}
}

// The cover phase hands each worker a batch of consecutive words. A corpus large enough to
// cross several batch boundaries must round-trip and keep the original encoding.
func TestCompressParallelBatchingRoundTrip(t *testing.T) {
	const n = 4000
	corpus := make([]testWord, 0, n+2)
	for i := range n {
		corpus = append(corpus, testWord{data: fmt.Appendf(nil, "prefix-deadbeefcafe-%04d-deadbeefcafe-suffix", i%37), compressed: true})
	}
	corpus = append(corpus, testWord{data: []byte{}, compressed: true}, testWord{data: []byte("zzz-unique-tail"), compressed: true})

	assertSameOutputEveryWorkerCount(t, corpus, 1432034479, 1, 2, 4, 8)
}

// Compressed, uncompressed and empty words interleaved exercise the queue's bypass paths
// alongside the batched cover path.
func TestCompressParallelBatchingMixedStream(t *testing.T) {
	const n = 6000
	corpus := make([]testWord, 0, n)
	for i := range n {
		switch i % 5 {
		case 0:
			corpus = append(corpus, testWord{data: []byte{}, compressed: true})
		case 1:
			corpus = append(corpus, testWord{data: fmt.Appendf(nil, "raw-%08d", i), compressed: false})
		default:
			corpus = append(corpus, testWord{data: fmt.Appendf(nil, "prefix-deadbeefcafe-%04d-suffix", i%53), compressed: true})
		}
	}

	assertSameOutputEveryWorkerCount(t, corpus, 660475778, 1, 2, 4, 8)
}

// A partial (unflushed) batch of compressible words followed by more than queueLimit
// uncompressible words holds the head-of-line word in curBatch while the queue fills. The
// backpressure loop must keep offering that batch to the workers instead of blocking on a
// result that can never arrive. Regression guard for the parallel-cover deadlock.
func TestCompressParallelBatchingBackpressureNoDeadlock(t *testing.T) {
	corpus := make([]testWord, 0, 145_000)
	// Not a whole multiple of coverBatchSize, so a non-empty partial batch is left in curBatch.
	for i := range 5000 {
		corpus = append(corpus, testWord{data: fmt.Appendf(nil, "prefix-deadbeefcafe-%04d-suffix", i), compressed: true})
	}
	// More than queueLimit (128*1024) uncompressible words piling onto the queue.
	for i := range 140_000 {
		corpus = append(corpus, testWord{data: fmt.Appendf(nil, "u-%08d", i), compressed: false})
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		assertSameOutputEveryWorkerCount(t, corpus, 2163753720, 4)
	}()
	select {
	case <-done:
	case <-time.After(120 * time.Second):
		t.Fatal("deadlock: parallel cover phase did not finish in 120s")
	}
}
