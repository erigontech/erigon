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
	"cmp"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
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

// Layout: 2-byte touchMap(0) || 2-byte afterMap || zero-padding to sz (>=4).
func branchVal(afterMap uint16, sz int) []byte {
	if sz < 4 {
		sz = 4
	}
	v := make([]byte, sz)
	binary.BigEndian.PutUint16(v[2:4], afterMap)
	return v
}

// path-string -> afterMap; node R's children are R||n for each set bit n.
type syntheticTree map[string]uint16

func buildSyntheticTree(t *testing.T) (hash []byte, tree syntheticTree, allPaths [][]byte) {
	t.Helper()
	hash = make([]byte, 32)
	for i := range hash {
		hash[i] = 0x42
	}
	root := string(hexNibbles(hash))
	r := []byte(root)
	p := func(suffix ...byte) []byte { return append(append([]byte{}, r...), suffix...) }
	tree = syntheticTree{
		string(p()):        0b110,
		string(p(1)):       0b1000,
		string(p(2)):       0b110000,
		string(p(1, 3)):    0,
		string(p(2, 4)):    0,
		string(p(2, 5)):    0b1000000,
		string(p(2, 5, 6)): 0,
	}
	for k := range tree {
		allPaths = append(allPaths, []byte(k))
	}
	return hash, tree, allPaths
}

func fakeResolver(tree syntheticTree, notFound map[string]bool, valSz int, failOnPath string) BatchBranchResolver {
	return func(keys [][]byte) ([][]byte, error) {
		// BatchBranchResolver's contract requires ascending-sorted keys.
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
				continue
			}
			vals[i] = branchVal(am, valSz)
		}
		return vals, nil
	}
}

func breadthFirstOrder(tree syntheticTree, exclude map[string]bool) []string {
	type pk struct {
		path string
		key  []byte
	}
	var pks []pk
	// An excluded node stops descent; it and its subtree become unreachable.
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
		for n := range 16 {
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
	slices.SortFunc(pks, func(a, b pk) int {
		if len(a.path) != len(b.path) {
			return cmp.Compare(len(a.path), len(b.path))
		}
		return bytes.Compare(a.key, b.key)
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
	n, err := PreloadContractTrunkParallel(hash, 1<<20, nil, fakeResolver(tree, nil, valSz, ""), c, nil)
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
	order := breadthFirstOrder(tree, nil)
	want := 3
	budget := 0
	for i := range want {
		budget += estimatedEntryOverheadBytes + len(nibbles.HexToCompact([]byte(order[i]))) + valSz
	}
	budget += 10
	c := NewBranchCache(64)
	n, err := PreloadContractTrunkParallel(hash, budget, nil, fakeResolver(tree, nil, valSz, ""), c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != want {
		t.Fatalf("pinned %d, want %d", n, want)
	}
	for i := range want {
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
	r2 := root + string([]byte{2})
	c := NewBranchCache(64)
	n, err := PreloadContractTrunkParallel(hash, 1<<20, nil, fakeResolver(tree, map[string]bool{r2: true}, 100, ""), c, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := breadthFirstOrder(tree, map[string]bool{r2: true})
	if n != len(want) {
		t.Fatalf("pinned %d, want %d (%v)", n, len(want), want)
	}
	for _, p := range []string{r2, r2 + string([]byte{4}), r2 + string([]byte{5}), r2 + string([]byte{5, 6})} {
		if _, _, ok := c.Get(nibbles.HexToCompact([]byte(p))); ok {
			t.Fatalf("%x is under the absent R2 and must not be pinned", p)
		}
	}
}

// A wide wave under a tiny budget must cap the fetch well below the wave width.
func TestPreloadParallel_CapsWaveFetch(t *testing.T) {
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = 0x55
	}
	root := string(hexNibbles(hash))
	tree := syntheticTree{root: 0xffff}
	for n := range 16 {
		tree[root+string([]byte{byte(n)})] = 0
	}
	const valSz = 100
	entry := estimatedEntryOverheadBytes + len(nibbles.HexToCompact([]byte(root))) + valSz
	budget := 3*entry + 50

	maxBatch := 0
	base := fakeResolver(tree, nil, valSz, "")
	resolve := func(keys [][]byte) ([][]byte, error) {
		if len(keys) > maxBatch {
			maxBatch = len(keys)
		}
		return base(keys)
	}
	c := NewBranchCache(64)
	n, err := PreloadContractTrunkParallel(hash, budget, nil, resolve, c, nil)
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

func TestPreloadParallel_DbTombstoneDropsBranch(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	root := ""
	for p := range tree {
		if root == "" || len(p) < len(root) {
			root = p
		}
	}
	const valSz = 100
	rootKey := nibbles.HexToCompact([]byte(root))
	dbBranches := map[string][]byte{string(rootKey): {}}

	c := NewBranchCache(64)
	n, err := PreloadContractTrunkParallel(hash, 1<<20, dbBranches, fakeResolver(tree, nil, valSz, ""), c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("pinned %d, want 0: the DB tombstone must drop the root, not fall through to the stale file value", n)
	}
	if _, _, ok := c.Get(rootKey); ok {
		t.Fatal("root should not be pinned: dbBranches holds a deletion tombstone for it")
	}
}

func TestPreloadParallel_DbHitsShadowFiles(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	root := ""
	for p := range tree {
		if root == "" || len(p) < len(root) {
			root = p
		}
	}
	r1 := root + string([]byte{1})
	tree[r1+string([]byte{7})] = 0
	const valSz = 100

	freshR1 := branchVal(0b10001000, valSz)
	freshR1[4] = 0xAB
	dbBranches := map[string][]byte{string(nibbles.HexToCompact([]byte(r1))): freshR1}

	c := NewBranchCache(64)
	n, err := PreloadContractTrunkParallel(hash, 1<<20, dbBranches, fakeResolver(tree, nil, valSz, ""), c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(tree) {
		t.Fatalf("pinned %d, want %d (whole tree reachable via the fresh R1 bitmap)", n, len(tree))
	}
	gotR1, _, ok := c.Get(nibbles.HexToCompact([]byte(r1)))
	if !ok {
		t.Fatal("R1 not pinned")
	}
	if !bytes.Equal(gotR1, freshR1) {
		t.Fatalf("R1 cached value is not the DB value: got %x want %x", gotR1, freshR1)
	}
	if _, _, ok := c.Get(nibbles.HexToCompact([]byte(r1 + string([]byte{7})))); !ok {
		t.Fatal("R1.7 should be pinned (the DB value of R1 has it as a child); the stale file bitmap was used instead")
	}
}

func TestNextSubtree(t *testing.T) {
	cases := []struct{ in, want []byte }{
		{[]byte{0x01, 0x02}, []byte{0x01, 0x03}},
		{[]byte{0x01, 0xff}, []byte{0x02}},
		{[]byte{0x00}, []byte{0x01}},
	}
	for _, c := range cases {
		if got := NextSubtree(c.in); !bytes.Equal(got, c.want) {
			t.Fatalf("NextSubtree(%x) = %x, want %x", c.in, got, c.want)
		}
	}
	if NextSubtree([]byte{0xff, 0xff}) != nil {
		t.Fatalf("NextSubtree(0xffff) should be nil")
	}
}

func TestContractTrunkKeyRanges(t *testing.T) {
	hashA := make([]byte, 32)
	for i := range hashA {
		hashA[i] = byte(7*i + 3)
	}
	hashB := make([]byte, 32)
	for i := range hashB {
		hashB[i] = byte(251 - 3*i)
	}
	nibA := ContractNibbles(hashA)
	nibB := ContractNibbles(hashB)
	evenFrom, evenTo, oddFrom, oddTo := ContractTrunkKeyRanges(nibA)
	inRange := func(k, from, to []byte) bool {
		return bytes.Compare(k, from) >= 0 && (to == nil || bytes.Compare(k, to) < 0)
	}
	keyOf := func(contractNibbles, slotPath []byte) []byte {
		return nibbles.HexToCompact(append(append([]byte{}, contractNibbles...), slotPath...))
	}
	slotPaths := [][]byte{
		{},
		{0x0}, {0xf},
		{0x1, 0x2}, {0xf, 0xf},
		{0x3, 0x4, 0x5},
		{0x6, 0x7, 0x8, 0x9},
		{0xa, 0xb, 0xc, 0xd, 0xe},
		make([]byte, 64),
	}
	for _, sp := range slotPaths {
		k := keyOf(nibA, sp)
		total := 64 + len(sp)
		if total%2 == 0 {
			if !inRange(k, evenFrom, evenTo) || inRange(k, oddFrom, oddTo) {
				t.Fatalf("depth %d (even) branch %x: must be in [%x,%x), not in [%x,%x)", total, k, evenFrom, evenTo, oddFrom, oddTo)
			}
		} else {
			if !inRange(k, oddFrom, oddTo) || inRange(k, evenFrom, evenTo) {
				t.Fatalf("depth %d (odd) branch %x: must be in [%x,%x), not in [%x,%x)", total, k, oddFrom, oddTo, evenFrom, evenTo)
			}
		}
		if got := nibbles.CompactToHex(k); !bytes.Equal(got, append(append([]byte{}, nibA...), sp...)) {
			t.Fatalf("CompactToHex round-trip mismatch for slot %x", sp)
		}
	}
	for _, sp := range slotPaths[:6] {
		k := keyOf(nibB, sp)
		if inRange(k, evenFrom, evenTo) || inRange(k, oddFrom, oddTo) {
			t.Fatalf("foreign-contract branch %x leaked into A's ranges", k)
		}
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
	_, err := PreloadContractTrunkParallel(hash, 1<<20, nil, fakeResolver(tree, nil, 100, root+string([]byte{1})), c, nil)
	if err == nil {
		t.Fatal("expected error from the resolver")
	}
	if c.PinnedCount() == 0 {
		t.Fatal("the depth-64 root should have been pinned before the depth-65 failure")
	}
}

func TestContractTrunkPreloadParallel_ResumeAcrossSteps(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	const valSz = 100
	resolve := fakeResolver(tree, nil, valSz, "")

	cRef := NewBranchCache(64)
	if _, err := PreloadContractTrunkParallel(hash, 1<<20, nil, resolve, cRef, nil); err != nil {
		t.Fatal(err)
	}

	c := NewBranchCache(64)
	p, err := NewContractTrunkPreloadParallel(hash)
	if err != nil {
		t.Fatal(err)
	}
	perStep := 2 * (estimatedEntryOverheadBytes + 33 + valSz)
	const maxSteps = 50
	var steps int
	for ; steps < maxSteps; steps++ {
		_, done, err := p.Run(perStep, nil, resolve, c, nil)
		if err != nil {
			t.Fatalf("step %d: %v", steps, err)
		}
		if done {
			break
		}
	}
	if steps >= maxSteps {
		t.Fatalf("preload did not complete in %d steps; pinned=%d", maxSteps, p.PinnedTotal())
	}
	if p.PinnedTotal() != len(tree) {
		t.Fatalf("step-by-step pinned %d, want %d", p.PinnedTotal(), len(tree))
	}
	if c.PinnedCount() != cRef.PinnedCount() {
		t.Fatalf("step-by-step cache pinned %d != one-shot cache pinned %d", c.PinnedCount(), cRef.PinnedCount())
	}
	for path := range tree {
		key := nibbles.HexToCompact([]byte(path))
		vRef, _, okRef := cRef.Get(key)
		v, _, ok := c.Get(key)
		if !okRef || !ok {
			t.Fatalf("path %x: ref ok=%v, step ok=%v", path, okRef, ok)
		}
		if !bytes.Equal(v, vRef) {
			t.Fatalf("path %x: step value differs from ref", path)
		}
	}
}

func TestContractTrunkPreloadParallel_RunAfterCompleteIsNoOp(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	const valSz = 100
	resolve := fakeResolver(tree, nil, valSz, "")
	c := NewBranchCache(64)
	p, err := NewContractTrunkPreloadParallel(hash)
	if err != nil {
		t.Fatal(err)
	}
	n1, done1, err := p.Run(1<<20, nil, resolve, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !done1 {
		t.Fatalf("expected done after full budget, got done=false (queue=%d)", p.QueueRemaining())
	}
	if n1 != len(tree) {
		t.Fatalf("first Run pinned %d, want %d", n1, len(tree))
	}
	prevPinned := c.PinnedCount()
	n2, done2, err := p.Run(1<<20, nil, resolve, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !done2 {
		t.Fatal("expected done on second Run")
	}
	if n2 != 0 {
		t.Fatalf("second Run pinned %d new entries, want 0", n2)
	}
	if c.PinnedCount() != prevPinned {
		t.Fatalf("cache pinned count changed across no-op Run: %d -> %d", prevPinned, c.PinnedCount())
	}
}

func TestContractTrunkPreloadParallel_StepBudgetCaps(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	const valSz = 100
	resolve := fakeResolver(tree, nil, valSz, "")
	c := NewBranchCache(64)
	p, err := NewContractTrunkPreloadParallel(hash)
	if err != nil {
		t.Fatal(err)
	}
	rootKey := nibbles.HexToCompact(hexNibbles(hash))
	entry := estimatedEntryOverheadBytes + len(rootKey) + valSz
	smallBudget := 3*entry + 10
	n1, done1, err := p.Run(smallBudget, nil, resolve, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if done1 {
		t.Fatalf("expected NOT done after a 3-entry budget; got done=true (pinned=%d)", n1)
	}
	if n1 < 1 || n1 > 5 {
		t.Fatalf("expected ~3 pinned this step, got %d", n1)
	}
	if p.QueueRemaining() == 0 {
		t.Fatal("expected frontier to be non-empty after small-budget step")
	}
	_, done2, err := p.Run(1<<20, nil, resolve, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !done2 {
		t.Fatalf("expected done after large follow-on budget; queue=%d", p.QueueRemaining())
	}
	if p.PinnedTotal() <= n1 {
		t.Fatalf("follow-on Run made no progress: pinned still %d (step-1 was %d)", p.PinnedTotal(), n1)
	}
}

func TestContractTrunkPreloadParallel_ResumeAfterResolverError(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	root := ""
	for p := range tree {
		if root == "" || len(p) < len(root) {
			root = p
		}
	}
	const valSz = 100
	failingResolve := fakeResolver(tree, nil, valSz, root+string([]byte{1}))
	healthyResolve := fakeResolver(tree, nil, valSz, "")
	c := NewBranchCache(64)
	p, err := NewContractTrunkPreloadParallel(hash)
	if err != nil {
		t.Fatal(err)
	}
	_, done, err := p.Run(1<<20, nil, failingResolve, c, nil)
	if err == nil {
		t.Fatal("expected resolver error")
	}
	if done {
		t.Fatal("Run with resolver error should return done=false")
	}
	if c.PinnedCount() == 0 {
		t.Fatal("expected the depth-64 root pinned before the depth-65 wave failed")
	}
	preErrPinned := p.PinnedTotal()
	// The error path returns before updating p.frontier/p.nextDepth.
	n, done, err := p.Run(1<<20, nil, healthyResolve, c, nil)
	if err != nil {
		t.Fatalf("retry failed: %v", err)
	}
	if !done {
		t.Fatalf("retry should complete the preload; queue=%d", p.QueueRemaining())
	}
	if p.PinnedTotal()-preErrPinned != n {
		t.Fatalf("PinnedTotal delta %d != Run pinned %d", p.PinnedTotal()-preErrPinned, n)
	}
	if p.PinnedTotal() != len(tree) {
		t.Fatalf("after retry pinned %d, want %d", p.PinnedTotal(), len(tree))
	}
}

func TestContractTrunkPreloadParallel_DbBranchesPerStep(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	root := ""
	for p := range tree {
		if root == "" || len(p) < len(root) {
			root = p
		}
	}
	const valSz = 100
	resolve := fakeResolver(tree, nil, valSz, "")

	freshRoot := branchVal(tree[root], valSz)
	freshRoot[4] = 0xAB
	dbWave0 := map[string][]byte{string(nibbles.HexToCompact([]byte(root))): freshRoot}

	c := NewBranchCache(64)
	p, err := NewContractTrunkPreloadParallel(hash)
	if err != nil {
		t.Fatal(err)
	}
	rootKey := nibbles.HexToCompact([]byte(root))
	stepBudget := estimatedEntryOverheadBytes + len(rootKey) + valSz + 10
	if _, _, err := p.Run(stepBudget, dbWave0, resolve, c, nil); err != nil {
		t.Fatal(err)
	}
	if p.DbHitsPinned() != 1 {
		t.Fatalf("wave 0: expected 1 db-hit pinned, got %d", p.DbHitsPinned())
	}
	gotRoot, _, ok := c.Get(rootKey)
	if !ok {
		t.Fatal("root not pinned after wave 0")
	}
	if !bytes.Equal(gotRoot, freshRoot) {
		t.Fatalf("wave 0: root pinned with stale file value, expected fresh dbBranches value")
	}

	if _, done, err := p.Run(1<<20, nil, resolve, c, nil); err != nil {
		t.Fatal(err)
	} else if !done {
		t.Fatalf("expected done after large budget; queue=%d", p.QueueRemaining())
	}
	if p.DbHitsPinned() != 1 {
		t.Fatalf("expected db-hit count to remain 1, got %d", p.DbHitsPinned())
	}
	if p.PinnedTotal() != len(tree) {
		t.Fatalf("after wave 1 pinned %d, want %d", p.PinnedTotal(), len(tree))
	}
}

func TestContractTrunkPreloadParallel_PinnedPrefixesAccumulate(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	const valSz = 100
	resolve := fakeResolver(tree, nil, valSz, "")
	c := NewBranchCache(64)
	p, err := NewContractTrunkPreloadParallel(hash)
	if err != nil {
		t.Fatal(err)
	}
	rootKey := nibbles.HexToCompact(hexNibbles(hash))
	entry := estimatedEntryOverheadBytes + len(rootKey) + valSz
	for range 2 {
		if _, _, err := p.Run(2*entry+10, nil, resolve, c, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, done, err := p.Run(1<<20, nil, resolve, c, nil); err != nil {
		t.Fatal(err)
	} else if !done {
		t.Fatal("expected done after large step")
	}
	prefixes := p.PinnedPrefixes()
	if len(prefixes) != p.PinnedTotal() {
		t.Fatalf("PinnedPrefixes len %d != PinnedTotal %d", len(prefixes), p.PinnedTotal())
	}
	for _, pf := range prefixes {
		if _, _, ok := c.Get(pf); !ok {
			t.Fatalf("prefix %x in PinnedPrefixes but not in cache", pf)
		}
	}
	seen := map[string]bool{}
	for _, pf := range prefixes {
		if seen[string(pf)] {
			t.Fatalf("duplicate prefix %x in PinnedPrefixes", pf)
		}
		seen[string(pf)] = true
	}
}

func TestContractTrunkPreloadParallel_NilCacheError(t *testing.T) {
	hash := make([]byte, 32)
	p, err := NewContractTrunkPreloadParallel(hash)
	if err != nil {
		t.Fatal(err)
	}
	resolve := func(keys [][]byte) ([][]byte, error) { return make([][]byte, len(keys)), nil }
	if _, _, err := p.Run(1<<20, nil, resolve, nil, nil); err == nil {
		t.Fatal("expected error when cache is nil")
	}
}

// A nil cache only surfaces the Run error when a logger is set.
func TestPreloadContractTrunkParallel_NilCacheWithLoggerReturnsError(t *testing.T) {
	hash := make([]byte, 32)
	resolve := func(keys [][]byte) ([][]byte, error) { return make([][]byte, len(keys)), nil }

	_, err := PreloadContractTrunkParallel(hash, 1<<20, nil, resolve, nil, log.Root())
	if err == nil {
		t.Fatal("expected error when cache is nil")
	}
	if !strings.Contains(err.Error(), "cache is nil") {
		t.Fatalf("error %q, want it to name the nil cache", err)
	}
}

func TestContractTrunkPreloadParallel_NilResolverError(t *testing.T) {
	hash := make([]byte, 32)
	c := NewBranchCache(64)
	p, err := NewContractTrunkPreloadParallel(hash)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.Run(1<<20, nil, nil, c, nil); err == nil {
		t.Fatal("expected error when resolver is nil")
	}
}

// The reusable partition scratch must not keep the last wave's keys/values
// reachable after Run returns.
func TestContractTrunkPreloadParallel_RunReleasesScratch(t *testing.T) {
	hash, tree, allPaths := buildSyntheticTree(t)
	root := hexNibbles(hash)
	const valSz = 100
	dbBranches := map[string][]byte{}
	for _, p := range allPaths {
		if bytes.Equal(p, root) {
			continue
		}
		dbBranches[string(nibbles.HexToCompact(p))] = branchVal(tree[string(p)], valSz)
	}

	p, err := NewContractTrunkPreloadParallel(hash)
	if err != nil {
		t.Fatal(err)
	}
	c := NewBranchCache(64)
	n, queueEmpty, err := p.Run(1<<20, dbBranches, fakeResolver(tree, nil, valSz, ""), c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !queueEmpty || n != len(tree) {
		t.Fatalf("pinned %d (queueEmpty=%v), want the whole %d-node tree drained", n, queueEmpty, len(tree))
	}

	if cap(p.scratchDbHits) == 0 || cap(p.scratchDbVals) == 0 || cap(p.scratchFileMiss) == 0 {
		t.Fatalf("scratch capacity not retained for reuse: dbHits=%d dbVals=%d fileMiss=%d",
			cap(p.scratchDbHits), cap(p.scratchDbVals), cap(p.scratchFileMiss))
	}
	for i, pk := range p.scratchDbHits[:cap(p.scratchDbHits)] {
		if pk.key != nil || pk.path != nil {
			t.Fatalf("scratchDbHits[%d] still references wave data after Run", i)
		}
	}
	for i, v := range p.scratchDbVals[:cap(p.scratchDbVals)] {
		if v != nil {
			t.Fatalf("scratchDbVals[%d] still references a dbBranches value after Run", i)
		}
	}
	for i, pk := range p.scratchFileMiss[:cap(p.scratchFileMiss)] {
		if pk.key != nil || pk.path != nil {
			t.Fatalf("scratchFileMiss[%d] still references wave data after Run", i)
		}
	}
}

func TestContractTrunkPreloadParallel_BadHashLengthError(t *testing.T) {
	if _, err := NewContractTrunkPreloadParallel(make([]byte, 31)); err == nil {
		t.Fatal("expected error for 31-byte hash")
	}
	if _, err := NewContractTrunkPreloadParallel(make([]byte, 33)); err == nil {
		t.Fatal("expected error for 33-byte hash")
	}
}

// Run has no cancellation; a spinning goroutine would outlive t.Fatal.
func panicOnStuck(why string) {
	debug.SetTraceback("all")
	panic("ContractTrunkPreloadParallel.Run did not terminate: " + why)
}

// A wave landing usedBytes exactly on stepCap must defer the next wave's
// miss set rather than spin re-entering an unchanged frontier.
func TestContractTrunkPreloadParallel_ExactBudgetFillTerminates(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	root := string(hexNibbles(hash))
	const valSz = 100
	resolve := fakeResolver(tree, nil, valSz, "")

	rootKey := bytes.Clone(nibbles.HexToCompact([]byte(root)))
	rootVal := branchVal(tree[root], valSz)
	dbBranches := map[string][]byte{string(rootKey): rootVal}

	c := NewBranchCache(64)
	defer c.Close()
	p, err := NewContractTrunkPreloadParallel(hash)
	if err != nil {
		t.Fatal(err)
	}

	stepBudget := estimatedEntryCost(rootKey, rootVal)

	type runResult struct {
		pinned     int
		queueEmpty bool
		err        error
	}
	res := make(chan runResult, 1)
	go func() {
		n, done, err := p.Run(stepBudget, dbBranches, resolve, c, nil)
		res <- runResult{n, done, err}
	}()

	var got runResult
	select {
	case got = <-res:
	case <-time.After(10 * time.Second):
		panicOnStuck("a wave with no file budget defers the whole frontier without pinning, so the loop re-enters on identical state")
	}
	if got.err != nil {
		t.Fatal(got.err)
	}
	if got.queueEmpty {
		t.Fatal("queue reported empty, but the root's children were never pinned")
	}
	if got.pinned != 1 {
		t.Fatalf("pinned %d entries, want 1 (the root)", got.pinned)
	}

	if _, done, err := p.Run(1<<20, nil, resolve, c, nil); err != nil {
		t.Fatal(err)
	} else if !done {
		t.Fatalf("expected done after a large budget; queue=%d", p.QueueRemaining())
	}
	if p.PinnedTotal() != len(tree) {
		t.Fatalf("pinned %d entries, want the whole tree (%d)", p.PinnedTotal(), len(tree))
	}
}

// Every Run must return regardless of step budget.
func TestContractTrunkPreloadParallel_StepBudgetSweepTerminates(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	root := string(hexNibbles(hash))
	const valSz = 100
	resolve := fakeResolver(tree, nil, valSz, "")
	rootKey := bytes.Clone(nibbles.HexToCompact([]byte(root)))
	rootVal := branchVal(tree[root], valSz)
	dbBranches := map[string][]byte{string(rootKey): rootVal}
	exact := estimatedEntryCost(rootKey, rootVal)

	affordable := 0
	for path, afterMap := range tree {
		if cost := estimatedEntryCost(nibbles.HexToCompact([]byte(path)), branchVal(afterMap, valSz)); cost > affordable {
			affordable = cost
		}
	}

	const maxSteps = 200
	budgets := []int{1, minEntryBytes, exact - 1, exact, exact + 1, affordable, 1 << 20}
	slices.Sort(budgets)
	budgets = slices.Compact(budgets)

	type sweepResult struct {
		done   bool
		pinned int
		err    error
	}

	for _, budget := range budgets {
		t.Run(fmt.Sprintf("budget=%d", budget), func(t *testing.T) {
			p, err := NewContractTrunkPreloadParallel(hash)
			if err != nil {
				t.Fatal(err)
			}
			c := NewBranchCache(64)
			defer c.Close()

			results := make(chan sweepResult, 1)
			go func() {
				var r sweepResult
				for range maxSteps {
					_, done, err := p.Run(budget, dbBranches, resolve, c, nil)
					if err != nil {
						r.err = err
						break
					}
					if done {
						r.done = true
						break
					}
				}
				r.pinned = p.PinnedTotal()
				results <- r
			}()

			var r sweepResult
			select {
			case r = <-results:
			case <-time.After(30 * time.Second):
				panicOnStuck(fmt.Sprintf("step budget %d: a wave that pins nothing must end the step, not re-enter on an unchanged frontier", budget))
			}
			if r.err != nil {
				t.Fatal(r.err)
			}

			if budget < exact {
				if r.pinned != 0 {
					t.Fatalf("pinned %d entries on a sub-entry budget", r.pinned)
				}
				return
			}
			if budget < affordable {
				return
			}
			if !r.done {
				t.Fatalf("not complete after %d steps (pinned %d/%d)", maxSteps, r.pinned, len(tree))
			}
			if r.pinned != len(tree) {
				t.Fatalf("pinned %d entries, want the whole tree (%d)", r.pinned, len(tree))
			}
		})
	}
}

// A wave that pins nothing must still end the step, not grind through the
// rest of the depth in one Run.
func TestContractTrunkPreloadParallel_NoPinWaveEndsStep(t *testing.T) {
	hash := make([]byte, 32)
	for i := range hash {
		hash[i] = 0x55
	}
	root := string(hexNibbles(hash))
	tree := syntheticTree{root: 0xffff}
	const valSz = 100
	rootKey := nibbles.HexToCompact([]byte(root))
	rootCost := estimatedEntryCost(rootKey, branchVal(0xffff, valSz))

	for _, tc := range []struct {
		name       string
		stepBudget int
		wantCalls  int
	}{
		{"residual below one entry defers unfetched", rootCost + minEntryBytes - 1, 1},
		{"capped fetch pins nothing", rootCost + minEntryBytes, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			base := fakeResolver(tree, nil, valSz, "")
			resolve := func(keys [][]byte) ([][]byte, error) {
				calls++
				return base(keys)
			}

			c := NewBranchCache(64)
			defer c.Close()
			p, err := NewContractTrunkPreloadParallel(hash)
			if err != nil {
				t.Fatal(err)
			}

			done := make(chan struct{})
			var pinned int
			var queueEmpty bool
			go func() {
				pinned, queueEmpty, err = p.Run(tc.stepBudget, nil, resolve, c, nil)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				panicOnStuck("a wave that pins nothing must end the step, not re-enter on the deferred tail")
			}
			if err != nil {
				t.Fatal(err)
			}
			if pinned != 1 || queueEmpty {
				t.Fatalf("pinned %d queueEmpty=%v, want 1 and false (root only, children deferred)", pinned, queueEmpty)
			}
			if calls != tc.wantCalls {
				t.Fatalf("resolver called %d times for a 16-wide wave, want %d; the wave was re-entered per chunk", calls, tc.wantCalls)
			}
			if p.QueueRemaining() == 0 {
				t.Fatal("deferred children were dropped instead of resumed")
			}

			if _, done, err := p.Run(1<<20, nil, resolve, c, nil); err != nil {
				t.Fatal(err)
			} else if !done {
				t.Fatalf("expected done after a large budget; queue=%d", p.QueueRemaining())
			}
			if p.PinnedTotal() != 1 {
				t.Fatalf("pinned %d, want 1 (only the root exists in the file layer)", p.PinnedTotal())
			}
		})
	}
}

// Deferring a miss unfetched vs. fetching-then-failing leave an identical
// frontier; only the resolver call count distinguishes them.
func TestContractTrunkPreloadParallel_DeferWithDbHitsInSameWave(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	root := string(hexNibbles(hash))
	const valSz = 100
	calls := 0
	base := fakeResolver(tree, nil, valSz, "")
	resolve := func(keys [][]byte) ([][]byte, error) {
		calls++
		return base(keys)
	}

	r1 := root + string([]byte{1})
	r1Key := bytes.Clone(nibbles.HexToCompact([]byte(r1)))
	dbBranches := map[string][]byte{string(r1Key): branchVal(tree[r1], valSz)}

	c := NewBranchCache(64)
	defer c.Close()
	p, err := NewContractTrunkPreloadParallel(hash)
	if err != nil {
		t.Fatal(err)
	}

	rootKey := nibbles.HexToCompact([]byte(root))
	rootCost := estimatedEntryCost(rootKey, branchVal(tree[root], valSz))
	r1Cost := estimatedEntryCost(r1Key, branchVal(tree[r1], valSz))
	stepBudget := rootCost + r1Cost + minEntryBytes - 1

	done := make(chan struct{})
	var pinned int
	var queueEmpty bool
	go func() {
		pinned, queueEmpty, err = p.Run(stepBudget, dbBranches, resolve, c, nil)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		panicOnStuck("a wave that defers its misses while pinning db-hits must end the step")
	}
	if err != nil {
		t.Fatal(err)
	}
	if pinned != 2 || queueEmpty {
		t.Fatalf("pinned %d queueEmpty=%v, want 2 (root+R1) and false", pinned, queueEmpty)
	}
	if p.DbHitsPinned() != 1 {
		t.Fatalf("db-hits pinned %d, want 1 (R1 came from dbBranches)", p.DbHitsPinned())
	}
	if calls != 1 {
		t.Fatalf("resolver called %d times, want 1; the deferred miss was fetched anyway", calls)
	}
	if len(p.frontier) != 1 || len(p.pendingChildren) != 1 {
		t.Fatalf("frontier=%d pendingChildren=%d, want 1 and 1", len(p.frontier), len(p.pendingChildren))
	}
	if got := string(p.frontier[0].path); got != root+string([]byte{2}) {
		t.Fatalf("frontier holds %x, want the deferred R2", got)
	}
	if got := string(p.pendingChildren[0].path); got != r1+string([]byte{3}) {
		t.Fatalf("pendingChildren holds %x, want R1's child", got)
	}

	if _, done, err := p.Run(1<<20, dbBranches, resolve, c, nil); err != nil {
		t.Fatal(err)
	} else if !done {
		t.Fatalf("expected done after a large budget; queue=%d", p.QueueRemaining())
	}
	if p.PinnedTotal() != len(tree) {
		t.Fatalf("pinned %d, want the whole tree (%d)", p.PinnedTotal(), len(tree))
	}
	seen := map[string]bool{}
	for _, pf := range p.PinnedPrefixes() {
		if seen[string(pf)] {
			t.Fatalf("prefix %x pinned twice across the deferral boundary", pf)
		}
		seen[string(pf)] = true
	}
}

// A walk stopped at the depth ceiling must report done on any budget.
func TestContractTrunkPreloadParallel_DepthCeilingReportsDoneOnAnyBudget(t *testing.T) {
	hash := make([]byte, 32)
	resolve := func(keys [][]byte) ([][]byte, error) { return make([][]byte, len(keys)), nil }
	c := NewBranchCache(64)
	defer c.Close()
	p, err := NewContractTrunkPreloadParallel(hash)
	if err != nil {
		t.Fatal(err)
	}

	p.nextDepth = maxStorageTrunkDepth + 1
	if p.QueueRemaining() == 0 {
		t.Fatal("queue drained, so the queued-but-finished state is not exercised")
	}

	if _, queueEmpty, err := p.Run(1<<20, nil, resolve, c, nil); err != nil {
		t.Fatal(err)
	} else if !queueEmpty {
		t.Error("a spendable budget past the depth ceiling reported work remaining")
	}
	if _, queueEmpty, err := p.Run(0, nil, resolve, c, nil); err != nil {
		t.Fatal(err)
	} else if !queueEmpty {
		t.Error("a zero budget past the depth ceiling disagreed with the spendable one")
	}
}
