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

package patricia

import (
	"bytes"
	"fmt"
	"math/rand"
	"runtime"
	"slices"
	"sync"
	"testing"
)

// benchCorpus builds a pattern dictionary and a set of words to scan, in a way
// that mimics seg compression: patterns are common substrings, words are made
// of patterns interleaved with literal bytes so matches actually fire.
type benchCorpus struct {
	ac    *AhoCorasick
	words [][]byte
	bytes int
}

type corpusCfg struct {
	numPatterns, patMin, patMax int
	numWords, wordLen           int
	// alpha is the number of distinct byte values. A small alphabet gives more
	// overlap between patterns and deeper fail chains; only a full 256 produces
	// states wide enough to reach the binary-search edge lookup.
	alpha  int
	sorted bool
}

func makeCorpus(c corpusCfg) *benchCorpus {
	r := rand.New(rand.NewSource(42))
	randByte := func() byte { return byte(r.Intn(c.alpha)) }

	patterns := make([][]byte, c.numPatterns)
	for i := range patterns {
		l := c.patMin + r.Intn(c.patMax-c.patMin+1)
		p := make([]byte, l)
		for j := range p {
			p[j] = randByte()
		}
		patterns[i] = p
	}

	ac := NewAhoCorasick()
	for i, p := range patterns {
		ac.Insert(p, i)
	}
	ac.Build()

	words := make([][]byte, c.numWords)
	total := 0
	for i := range words {
		w := make([]byte, 0, c.wordLen)
		for len(w) < c.wordLen {
			if r.Intn(3) == 0 { // literal noise
				w = append(w, randByte())
			} else {
				w = append(w, patterns[r.Intn(c.numPatterns)]...)
			}
		}
		w = w[:c.wordLen]
		words[i] = w
		total += len(w)
	}
	if c.sorted {
		slices.SortFunc(words, bytes.Compare)
	}
	return &benchCorpus{ac: ac, words: words, bytes: total}
}

// Corpora are built once and shared: the testing package re-runs a benchmark
// body at every step of the b.N ladder, and rebuilding a multi-million-node
// automaton each time costs more wall clock than the measurement itself.
func corpus(c corpusCfg) func() *benchCorpus {
	return sync.OnceValue(func() *benchCorpus { return makeCorpus(c) })
}

var (
	corpusSorted     = corpus(corpusCfg{numPatterns: 2000, patMin: 4, patMax: 32, numWords: 4096, wordLen: 96, alpha: 16, sorted: true})
	corpusUnsorted   = corpus(corpusCfg{numPatterns: 2000, patMin: 4, patMax: 32, numWords: 4096, wordLen: 96, alpha: 16})
	corpusShortWords = corpus(corpusCfg{numPatterns: 2000, patMin: 4, patMax: 32, numWords: 8192, wordLen: 32, alpha: 16, sorted: true})
	corpusLongWords  = corpus(corpusCfg{numPatterns: 2000, patMin: 4, patMax: 32, numWords: 512, wordLen: 1024, alpha: 16, sorted: true})
	// ~64k patterns -> millions of nodes, the cache-bound regime real seg
	// compression runs in (MaxDictPatterns default is 64*1024).
	corpusBigDict = corpus(corpusCfg{numPatterns: 64 * 1024, patMin: 8, patMax: 96, numWords: 4096, wordLen: 128, alpha: 16, sorted: true})
	// full byte alphabet: the only corpus whose states fan out past
	// wideBsearchMin, so the binary-search edge lookup is actually measured.
	corpusByteAlphabet = corpus(corpusCfg{numPatterns: 64 * 1024, patMin: 8, patMax: 96, numWords: 4096, wordLen: 128, alpha: 256, sorted: true})
)

func benchFind(b *testing.B, c *benchCorpus) {
	b.ReportAllocs()
	b.SetBytes(int64(c.bytes))
	m := NewACMatcher(c.ac)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, w := range c.words {
			m.FindLongestMatches(w)
		}
	}
}

func BenchmarkFindLongestMatches_Sorted(b *testing.B)       { benchFind(b, corpusSorted()) }
func BenchmarkFindLongestMatches_Unsorted(b *testing.B)     { benchFind(b, corpusUnsorted()) }
func BenchmarkFindLongestMatches_ShortWords(b *testing.B)   { benchFind(b, corpusShortWords()) }
func BenchmarkFindLongestMatches_LongWords(b *testing.B)    { benchFind(b, corpusLongWords()) }
func BenchmarkFindLongestMatches_BigDict(b *testing.B)      { benchFind(b, corpusBigDict()) }
func BenchmarkFindLongestMatches_ByteAlphabet(b *testing.B) { benchFind(b, corpusByteAlphabet()) }

func BenchmarkBuild(b *testing.B) {
	r := rand.New(rand.NewSource(7))
	const alpha = 16
	patterns := make([][]byte, 4000)
	for i := range patterns {
		l := 4 + r.Intn(28)
		p := make([]byte, l)
		for j := range p {
			p[j] = byte(r.Intn(alpha))
		}
		patterns[i] = p
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ac := NewAhoCorasick()
		for j, p := range patterns {
			ac.Insert(p, j)
		}
		ac.Build()
	}
}

// BenchmarkEdgeLookup pins the fanout at which binary search starts to beat
// scanning eight labels a word, which is what wideBsearchMin encodes.
func BenchmarkEdgeLookup(b *testing.B) {
	r := rand.New(rand.NewSource(3))
	for _, fanout := range []int32{2, 4, 8, 12, 16, 24, 32, 48, 64, 128, 256} {
		perm := r.Perm(256)[:fanout]
		labels := make([]byte, fanout)
		for i, v := range perm {
			labels[i] = byte(v)
		}
		slices.Sort(labels)
		children := make([]int32, fanout)
		for i := range children {
			children[i] = int32(i)
		}
		padded := append(slices.Clone(labels), make([]byte, swarPad)...)
		probes := make([]byte, 256)
		for i := range probes {
			probes[i] = byte(r.Intn(256))
		}
		b.Run(fmt.Sprintf("swar/fanout=%d", fanout), func(b *testing.B) {
			var sink int32
			for i := 0; i < b.N; i++ {
				sink += swarEdge(padded, children, 0, fanout, probes[i&255])
			}
			runtime.KeepAlive(sink)
		})
		b.Run(fmt.Sprintf("bsearch/fanout=%d", fanout), func(b *testing.B) {
			var sink int32
			for i := 0; i < b.N; i++ {
				sink += bsearchEdge(padded, children, 0, fanout, probes[i&255])
			}
			runtime.KeepAlive(sink)
		})
	}
}
