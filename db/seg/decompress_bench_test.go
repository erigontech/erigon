// Copyright 2021 The Erigon Authors
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
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

func BenchmarkDecompressNextBuf(b *testing.B) {
	d := prepareDict(b, 1, 1_000)
	defer d.Close()
	b.ReportAllocs()
	var k []byte
	g := d.MakeGetter()
	for b.Loop() {
		if !g.HasNext() {
			g.Reset(0)
		}
		k, _ = g.Next(k[:0])
		if len(k) > 0 {
			_, _ = k[0], k[len(k)-1]
		}
	}
}

func BenchmarkDecompressNextHeap(b *testing.B) {
	d := prepareDict(b, 1, 1_000)
	defer d.Close()

	b.ReportAllocs()
	g := d.MakeGetter()
	for b.Loop() {
		if !g.HasNext() {
			g.Reset(0)
		}
		k, _ := g.Next(nil)
		if len(k) > 0 {
			_, _ = k[0], k[len(k)-1]
		}
	}
}

func BenchmarkDecompressSkip(b *testing.B) {
	d := prepareDict(b, 1, 1_000_000)
	defer d.Close()

	b.Run("skip", func(b *testing.B) {
		b.ReportAllocs()
		g := d.MakeGetter()
		for b.Loop() {
			g.Reset(0)
			for g.HasNext() {
				_, _ = g.Skip()
			}
		}
	})

	//b.Run("matchcmp_non_existing_key", func(b *testing.B) {
	//	b.ReportAllocs()
	//	g := d.MakeGetter()
	//	for b.Loop() {
	//		_ = g.MatchCmp([]byte("longlongword"))
	//		if !g.HasNext() {
	//			g.Reset(0)
	//		}
	//	}
	//})
}

// prepareBinaryDict creates a file with sorted binary keys (like storage keys).
// If compressed is true, keys are added with AddWord (huffman); otherwise AddUncompressedWord.
func prepareBinaryDict(b testing.TB, keyCount int, keySize int, compressed bool) *Decompressor {
	b.Helper()
	logger := log.New()
	tmpDir := b.TempDir()
	file := filepath.Join(tmpDir, "binkeys")
	cfg := DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 2
	c, err := NewCompressor(b.Context(), b.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
	require.NoError(b, err)

	rng := rand.New(rand.NewSource(42))
	keys := make([][]byte, keyCount)
	for i := range keys {
		k := make([]byte, keySize)
		binary.BigEndian.PutUint32(k[:4], uint32(i%16))
		rng.Read(k[4:])
		binary.BigEndian.PutUint64(k[keySize-8:], uint64(i))
		keys[i] = k
	}
	slices.SortFunc(keys, bytes.Compare)

	for _, k := range keys {
		if compressed {
			require.NoError(b, c.AddWord(k))
		} else {
			require.NoError(b, c.AddUncompressedWord(k))
		}
	}
	require.NoError(b, c.Compress())
	c.Close()

	d, err := NewDecompressor(file)
	require.NoError(b, err)
	return d
}

// BenchmarkMatchCmp compares MatchCmp vs Next+bytes.Compare on compressed binary keys.
func BenchmarkMatchCmp(b *testing.B) {
	const keyCount = 50_000
	const keySize = 52
	d := prepareBinaryDict(b, keyCount, keySize, true)
	defer d.Close()

	// Collect all words and offsets
	g := d.MakeGetter()
	words := make([][]byte, 0, keyCount)
	offsets := make([]uint64, 0, keyCount)
	for g.HasNext() {
		offsets = append(offsets, g.dataP)
		w, _ := g.Next(nil)
		words = append(words, w)
	}

	b.Run("MatchCmp_hit", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			i := b.N % len(words)
			g.Reset(offsets[i])
			cmp := g.MatchCmp(words[i])
			if cmp != 0 {
				b.Fatal("expected match")
			}
		}
	})

	b.Run("MatchCmp_miss", func(b *testing.B) {
		miss := make([]byte, keySize)
		miss[0] = 0xff // greater than any key
		b.ReportAllocs()
		for b.Loop() {
			i := b.N % len(offsets)
			g.Reset(offsets[i])
			g.MatchCmp(miss)
		}
	})

	b.Run("Next_buf_reuse", func(b *testing.B) {
		buf := make([]byte, 0, keySize)
		b.ReportAllocs()
		for b.Loop() {
			i := b.N % len(words)
			g.Reset(offsets[i])
			buf, _ = g.Next(buf[:0])
		}
	})

	b.Run("MatchPrefix_hit", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			i := b.N % len(words)
			g.Reset(offsets[i])
			if !g.MatchPrefix(words[i]) {
				b.Fatal("expected match")
			}
		}
	})

	b.Run("MatchPrefix_miss", func(b *testing.B) {
		miss := make([]byte, keySize)
		miss[0] = 0xff
		b.ReportAllocs()
		for b.Loop() {
			i := b.N % len(offsets)
			g.Reset(offsets[i])
			g.MatchPrefix(miss)
		}
	})
}

func BenchmarkMatchCmpUncompressed(b *testing.B) {
	const keyCount = 50_000
	const keySize = 52
	d := prepareBinaryDict(b, keyCount, keySize, false)
	defer d.Close()

	g := d.MakeGetter()
	words := make([][]byte, 0, keyCount)
	offsets := make([]uint64, 0, keyCount)
	for g.HasNext() {
		offsets = append(offsets, g.dataP)
		w, _ := g.NextUncompressed()
		words = append(words, append([]byte{}, w...))
	}

	b.Run("MatchCmpUncompressed_hit", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			i := b.N % len(words)
			g.Reset(offsets[i])
			cmp := g.MatchCmpUncompressed(words[i])
			if cmp != 0 {
				b.Fatal("expected match")
			}
		}
	})

	b.Run("MatchCmpUncompressed_miss", func(b *testing.B) {
		miss := make([]byte, keySize)
		miss[0] = 0xff
		b.ReportAllocs()
		for b.Loop() {
			i := b.N % len(offsets)
			g.Reset(offsets[i])
			g.MatchCmpUncompressed(miss)
		}
	})

	b.Run("MatchPrefixUncompressed_hit", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			i := b.N % len(words)
			g.Reset(offsets[i])
			if !g.MatchPrefixUncompressed(words[i]) {
				b.Fatal("expected match")
			}
		}
	})

	b.Run("MatchPrefixUncompressed_miss", func(b *testing.B) {
		miss := make([]byte, keySize)
		miss[0] = 0xff
		b.ReportAllocs()
		for b.Loop() {
			i := b.N % len(offsets)
			g.Reset(offsets[i])
			g.MatchPrefixUncompressed(miss)
		}
	})

	b.Run("NextUncompressed", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			i := b.N % len(words)
			g.Reset(offsets[i])
			g.NextUncompressed()
		}
	})
}

func BenchmarkDecompressTorrent(t *testing.B) {
	t.Skip()

	//fpath := "/Volumes/wotah/mainnet/snapshots/v1.0-013500-014000-bodies.seg"
	fpath := "/Volumes/wotah/mainnet/snapshots/v1.0-013500-014000-transactions.seg"
	//fpath := "./v1.0-006000-006500-transactions.seg"
	st, err := os.Stat(fpath)
	require.NoError(t, err)
	fmt.Printf("file: %v, size: %d\n", st.Name(), st.Size())

	t.Run("init", func(t *testing.B) {
		for i := 0; i < t.N; i++ {
			d, err := NewDecompressor(fpath)
			require.NoError(t, err)
			d.Close()
		}
	})
	t.Run("run", func(t *testing.B) {
		d, err := NewDecompressor(fpath)
		require.NoError(t, err)
		defer d.Close()

		getter := d.MakeGetter()

		for i := 0; i < t.N && getter.HasNext(); i++ {
			_, sz := getter.Next(nil)
			if sz == 0 {
				t.Fatal("sz == 0")
			}
		}
	})
}
