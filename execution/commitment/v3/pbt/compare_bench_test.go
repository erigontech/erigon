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

package pbt

import "testing"

func benchmarkPBinProcess(b *testing.B, count int) {
	ops := make([]Op, count)
	for i := range ops {
		ops[i] = Op{Key: trieCodeKey(byte(i>>8), byte(i), byte(i)), Value: testTrieValue(byte(i))}
	}
	trie := NewTrie(newTrieTestContext())
	b.ResetTimer()
	for range b.N {
		trie.ResetContext(newTrieTestContext())
		if _, err := trie.Process(ops); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPBinCompareFeedTip(b *testing.B) { benchmarkPBinProcess(b, 5000) }

func BenchmarkPBinCompareFeedWhale(b *testing.B) { benchmarkPBinProcess(b, 512) }

func BenchmarkPBinCompareFeedRebuild(b *testing.B) { benchmarkPBinProcess(b, 5000) }
