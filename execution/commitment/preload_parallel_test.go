// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package commitment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sort"
	"testing"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func hexNibbles(b []byte) []byte {
	out := make([]byte, len(b)*2)
	for i, x := range b {
		out[2*i] = x >> 4
		out[2*i+1] = x & 0x0f
	}
	return out
}

// branchVal builds a synthetic branch-node value: 2-byte touchMap (0) ||
// 2-byte afterMap (the child bitmap) || zero-padding to size sz (>= 4).
func branchVal(afterMap uint16, sz int) []byte {
	if sz < 4 {
		sz = 4
	}
	v := make([]byte, sz)
	binary.BigEndian.PutUint16(v[2:4], afterMap)
	return v
}

// syntheticTree describes a contract storage subtree: path-string -> afterMap.
// Root is the 64-nibble path of the contract hash; a node R is present iff R is
// a key here, and R's children are R||n for each set bit n in afterMap[R].
type syntheticTree map[string]uint16

func buildSyntheticTree(t *testing.T) (hash []byte, tree syntheticTree, allPaths [][]byte) {
	t.Helper()
	hash = make([]byte, 32)
	for i := range hash {
		hash[i] = 0x42
	}
	root := string(hexNibbles(hash))
	// R(64) -> {1,2} ; R1(65) -> {3} ; R2(65) -> {4,5} ;
	// R1.3(66) leaf ; R2.4(66) leaf ; R2.5(66) -> {6} ; R2.5.6(67) leaf.
	r := []byte(root)
	p := func(suffix ...byte) []byte { return append(append([]byte{}, r...), suffix...) }
	tree = syntheticTree{
		string(p()):        0b110,    // bits 1,2
		string(p(1)):       0b1000,   // bit 3
		string(p(2)):       0b110000, // bits 4,5
		string(p(1, 3)):    0,
		string(p(2, 4)):    0,
		string(p(2, 5)):    0b1000000, // bit 6
		string(p(2, 5, 6)): 0,
	}
	for k := range tree {
		allPaths = append(allPaths, []byte(k))
	}
	return hash, tree, allPaths
}

// fakeResolver returns a BatchBranchResolver backed by the synthetic tree.
// notFound (path-strings) are treated as absent from the file layer.
// valSz is the branch value size. If failOnKey is non-empty, the resolver
// returns an error when that key is requested.
func fakeResolver(tree syntheticTree, notFound map[string]bool, valSz int, failOnPath string) BatchBranchResolver {
	return func(keys [][]byte) ([][]byte, error) {
		// keys must be sorted ascending (the contract of BatchBranchResolver).
		for i := 1; i < len(keys); i++ {
			if bytes.Compare(keys[i-1], keys[i]) >= 0 {
				return nil, errors.New("resolver got unsorted keys")
			}
		}
		vals := make([][]byte, len(keys))
		for i, k := range keys {
			path := string(nibbles.CompactToHex(k))
			if failOnPath != "" && path == failOnPath {
				return nil, errors.New("synthetic resolver failure")
			}
			am, ok := tree[path]
			if !ok || notFound[path] {
				continue // nil
			}
			vals[i] = branchVal(am, valSz)
		}
		return vals, nil
	}
}

// breadthFirstOrder returns the synthetic tree's paths in the order
// PreloadContractTrunkParallel pins them: by depth, then by compact-key.
func breadthFirstOrder(tree syntheticTree, exclude map[string]bool) []string {
	type pk struct {
		path string
		key  []byte
	}
	var pks []pk
	// reachability from root, honoring exclude (an excluded node stops descent —
	// it and its subtree become unreachable)
	root := ""
	for p := range tree {
		if root == "" || len(p) < len(root) {
			root = p
		}
	}
	reach := map[string]bool{}
	var dfs func(p string)
	dfs = func(p string) {
		if exclude[p] {
			return
		}
		am, ok := tree[p]
		if !ok {
			return
		}
		reach[p] = true
		for n := 0; n < 16; n++ {
			if am&(1<<uint(n)) != 0 {
				dfs(p + string([]byte{byte(n)}))
			}
		}
	}
	dfs(root)
	for p := range reach {
		k := nibbles.HexToCompact([]byte(p))
		kc := make([]byte, len(k))
		copy(kc, k)
		pks = append(pks, pk{path: p, key: kc})
	}
	sort.Slice(pks, func(i, j int) bool {
		if len(pks[i].path) != len(pks[j].path) {
			return len(pks[i].path) < len(pks[j].path)
		}
		return bytes.Compare(pks[i].key, pks[j].key) < 0
	})
	out := make([]string, len(pks))
	for i := range pks {
		out[i] = pks[i].path
	}
	return out
}

func TestPreloadParallel_FullBudget_BreadthFirst(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	const valSz = 100
	c := NewBranchCache(64)
	n, err := PreloadContractTrunkParallel(hash, 1<<20, fakeResolver(tree, nil, valSz, ""), c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(tree) || c.PinnedCount() != len(tree) {
		t.Fatalf("pinned %d (cache %d), want %d", n, c.PinnedCount(), len(tree))
	}
	for path, am := range tree {
		key := nibbles.HexToCompact([]byte(path))
		v, _, ok := c.Get(key)
		if !ok {
			t.Fatalf("path %x not pinned", path)
		}
		if binary.BigEndian.Uint16(v[2:4]) != am {
			t.Fatalf("path %x: bitmap mismatch", path)
		}
	}
}

func TestPreloadParallel_BudgetCutoff(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	const valSz = 100
	order := breadthFirstOrder(tree, nil) // 7 entries: R, R1, R2, R1.3, R2.4, R2.5, R2.5.6
	// Budget for exactly the 3 shallowest (R + R1 + R2). compact-key lengths
	// at depths 64..66 are 33/33/34 bytes; use the actual lengths.
	want := 3
	budget := 0
	for i := 0; i < want; i++ {
		budget += estimatedEntryOverheadBytes + len(nibbles.HexToCompact([]byte(order[i]))) + valSz
	}
	budget += 10 // slack inside the want-th but below the (want+1)-th
	c := NewBranchCache(64)
	n, err := PreloadContractTrunkParallel(hash, budget, fakeResolver(tree, nil, valSz, ""), c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != want {
		t.Fatalf("pinned %d, want %d", n, want)
	}
	for i := 0; i < want; i++ {
		if _, _, ok := c.Get(nibbles.HexToCompact([]byte(order[i]))); !ok {
			t.Fatalf("shallowest #%d (%x) should be pinned", i, order[i])
		}
	}
	if _, _, ok := c.Get(nibbles.HexToCompact([]byte(order[want]))); ok {
		t.Fatalf("entry #%d (%x) should have been cut off by the budget", want, order[want])
	}
}

func TestPreloadParallel_NotFoundStopsDescent(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	root := ""
	for p := range tree {
		if root == "" || len(p) < len(root) {
			root = p
		}
	}
	r2 := root + string([]byte{2}) // R||2 — make it absent from the file layer
	c := NewBranchCache(64)
	n, err := PreloadContractTrunkParallel(hash, 1<<20, fakeResolver(tree, map[string]bool{r2: true}, 100, ""), c, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Reachable with R2 absent: R, R1, R1.3 = 3.
	want := breadthFirstOrder(tree, map[string]bool{r2: true})
	if n != len(want) {
		t.Fatalf("pinned %d, want %d (%v)", n, len(want), want)
	}
	// R2's subtree must NOT be pinned.
	for _, p := range []string{r2, r2 + string([]byte{4}), r2 + string([]byte{5}), r2 + string([]byte{5, 6})} {
		if _, _, ok := c.Get(nibbles.HexToCompact([]byte(p))); ok {
			t.Fatalf("%x is under the absent R2 and must not be pinned", p)
		}
	}
}

func TestPreloadParallel_CapsWaveFetch(t *testing.T) {
	// A wide wave (root with all 16 children) under a tiny budget: the depth-65
	// fetch must be capped well below the wave width — otherwise a budget-
	// truncated wide wave (depth 69 on the real workload is ~1.3M keys) would
	// fetch the whole thing to pin a handful.
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = 0x55
	}
	root := string(hexNibbles(hash))
	tree := syntheticTree{root: 0xffff}
	for n := 0; n < 16; n++ {
		tree[root+string([]byte{byte(n)})] = 0 // 16 depth-65 leaves
	}
	const valSz = 100
	entry := estimatedEntryOverheadBytes + len(nibbles.HexToCompact([]byte(root))) + valSz
	budget := 3*entry + 50 // ~3 entries

	maxBatch := 0
	base := fakeResolver(tree, nil, valSz, "")
	resolve := func(keys [][]byte) ([][]byte, error) {
		if len(keys) > maxBatch {
			maxBatch = len(keys)
		}
		return base(keys)
	}
	c := NewBranchCache(64)
	n, err := PreloadContractTrunkParallel(hash, budget, resolve, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n < 1 || n > 3 {
		t.Fatalf("pinned %d, expected 1..3 for a ~3-entry budget", n)
	}
	if maxBatch > 6 {
		t.Fatalf("depth-65 wave (width 16) should have been capped to ~remaining/minEntryBytes; resolver saw a batch of %d", maxBatch)
	}
}

func TestPreloadParallel_ResolverError(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	root := ""
	for p := range tree {
		if root == "" || len(p) < len(root) {
			root = p
		}
	}
	c := NewBranchCache(64)
	// Fail when R||1 (depth 65) is requested -> root pinned at depth 64, then error.
	_, err := PreloadContractTrunkParallel(hash, 1<<20, fakeResolver(tree, nil, 100, root+string([]byte{1})), c, nil)
	if err == nil {
		t.Fatal("expected error from the resolver")
	}
	if c.PinnedCount() == 0 {
		t.Fatal("the depth-64 root should have been pinned before the depth-65 failure")
	}
}
