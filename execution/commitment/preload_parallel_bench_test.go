// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package commitment

import "testing"

// buildFullTree builds a full 16-ary storage subtree rooted at the contract's
// 64-nibble path, fully populated down to leafDepth. The frontier at leafDepth
// is 16^(leafDepth-64) nodes (depth 68 => 65536, matching production).
func buildFullTree(hash []byte, leafDepth int) syntheticTree {
	tree := syntheticTree{}
	var rec func(path []byte, depth int)
	rec = func(path []byte, depth int) {
		if depth == leafDepth {
			tree[string(path)] = 0
			return
		}
		tree[string(path)] = 0xFFFF
		for n := range 16 {
			child := make([]byte, len(path)+1)
			copy(child, path)
			child[len(path)] = byte(n)
			rec(child, depth+1)
		}
	}
	rec(hexNibbles(hash), 64)
	return tree
}

func benchmarkDrain(b *testing.B, leafDepth, stepBudget int) {
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = 0x42
	}
	tree := buildFullTree(hash, leafDepth)
	resolve := fakeResolver(tree, nil, 40, "")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p, err := NewContractTrunkPreloadParallel(hash)
		if err != nil {
			b.Fatal(err)
		}
		c := NewBranchCache(64)
		for {
			_, done, err := p.Run(stepBudget, nil, resolve, c, nil)
			if err != nil {
				b.Fatal(err)
			}
			if done {
				break
			}
		}
	}
}

func BenchmarkPreloadDrain_d67(b *testing.B) { benchmarkDrain(b, 67, 1_000_000) }
func BenchmarkPreloadDrain_d68(b *testing.B) { benchmarkDrain(b, 68, 2_000_000) }

// randomFrontier builds n unsorted pathKeys with pseudo-random deep (depth-69)
// nibble paths under the contract root, so the sort has real work to do.
func randomFrontier(hash []byte, n int) []pathKey {
	root := hexNibbles(hash) // 64 nibbles
	f := make([]pathKey, n)
	seed := uint64(88172645463325252)
	for i := range f {
		path := make([]byte, 69)
		copy(path, root)
		for d := 64; d < 69; d++ {
			seed ^= seed << 13
			seed ^= seed >> 7
			seed ^= seed << 17
			path[d] = byte(seed & 0x0f)
		}
		f[i] = toPathKey(path)
	}
	return f
}

// benchmarkSortPartition isolates sortAndPartitionFrontier (the per-wave sort +
// db/file split) at frontier size n — the production hot spot (~1M entries).
func benchmarkSortPartition(b *testing.B, n int) {
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = 0x42
	}
	base := randomFrontier(hash, n)
	dbBranches := map[string][]byte{} // all file misses
	p, err := NewContractTrunkPreloadParallel(hash)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		p.frontier = append(p.frontier[:0], base...) // fresh unsorted copy each iter
		b.StartTimer()
		p.sortAndPartitionFrontier(dbBranches)
	}
}

func BenchmarkSortAndPartitionFrontier_65k(b *testing.B) { benchmarkSortPartition(b, 65_000) }

func BenchmarkSortAndPartitionFrontier_1M(b *testing.B) {
	if testing.Short() {
		b.Skip("long-running: ~1M-entry frontier setup + sort")
	}
	benchmarkSortPartition(b, 1_000_000)
}
