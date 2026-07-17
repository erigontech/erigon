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
	"math/rand"
	"sort"
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

func makeCorpus(numPatterns, patMin, patMax, numWords, wordLen int, sorted bool) *benchCorpus {
	r := rand.New(rand.NewSource(42))
	// small alphabet -> more overlap between patterns, deeper fail chains,
	// closer to real (hex-ish / rlp-ish) compression input than 256-wide random
	const alpha = 16
	randByte := func() byte { return byte(r.Intn(alpha)) }

	patterns := make([][]byte, numPatterns)
	for i := range patterns {
		l := patMin + r.Intn(patMax-patMin+1)
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

	words := make([][]byte, numWords)
	total := 0
	for i := range words {
		w := make([]byte, 0, wordLen)
		for len(w) < wordLen {
			if r.Intn(3) == 0 { // literal noise
				w = append(w, randByte())
			} else {
				w = append(w, patterns[r.Intn(numPatterns)]...)
			}
		}
		w = w[:wordLen]
		words[i] = w
		total += len(w)
	}
	if sorted {
		sort.Slice(words, func(i, j int) bool { return string(words[i]) < string(words[j]) })
	}
	return &benchCorpus{ac: ac, words: words, bytes: total}
}

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

func BenchmarkFindLongestMatches_Sorted(b *testing.B) {
	c := makeCorpus(2000, 4, 32, 4096, 96, true)
	benchFind(b, c)
}

func BenchmarkFindLongestMatches_Unsorted(b *testing.B) {
	c := makeCorpus(2000, 4, 32, 4096, 96, false)
	benchFind(b, c)
}

func BenchmarkFindLongestMatches_ShortWords(b *testing.B) {
	c := makeCorpus(2000, 4, 32, 8192, 32, true)
	benchFind(b, c)
}

func BenchmarkFindLongestMatches_LongWords(b *testing.B) {
	c := makeCorpus(2000, 4, 32, 512, 1024, true)
	benchFind(b, c)
}

func BenchmarkFindLongestMatches_BigDict(b *testing.B) {
	// ~64k patterns -> millions of nodes, the cache-bound regime real seg
	// compression runs in (MaxDictPatterns default is 64*1024).
	c := makeCorpus(64*1024, 8, 96, 4096, 128, true)
	benchFind(b, c)
}

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
