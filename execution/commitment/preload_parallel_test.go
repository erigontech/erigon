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
	n, err := PreloadContractTrunkParallel(hash, budget, nil, fakeResolver(tree, nil, valSz, ""), c, nil)
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
	n, err := PreloadContractTrunkParallel(hash, 1<<20, nil, fakeResolver(tree, map[string]bool{r2: true}, 100, ""), c, nil)
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

func TestPreloadParallel_DbHitsShadowFiles(t *testing.T) {
	// A branch present in both dbBranches (fresh) and the file layer (stale)
	// must resolve to the DB value — and the DB value's child bitmap must drive
	// the descent. Tree: as buildSyntheticTree but with an extra leaf R1.7; the
	// file value for R1 has bitmap {3} (so R1.7 unreachable via files), the DB
	// value for R1 has bitmap {3,7} — so R1.7 should get pinned iff the DB value
	// is the one used.
	hash, tree, _ := buildSyntheticTree(t)
	root := ""
	for p := range tree {
		if root == "" || len(p) < len(root) {
			root = p
		}
	}
	r1 := root + string([]byte{1})
	tree[r1+string([]byte{7})] = 0 // R1.7 leaf, present in the file layer
	const valSz = 100

	freshR1 := branchVal(0b10001000, valSz) // bits 3 and 7
	freshR1[4] = 0xAB                       // a marker so we can assert the exact bytes were pinned
	dbBranches := map[string][]byte{string(nibbles.HexToCompact([]byte(r1))): freshR1}

	c := NewBranchCache(64)
	n, err := PreloadContractTrunkParallel(hash, 1<<20, dbBranches, fakeResolver(tree, nil, valSz, ""), c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(tree) {
		t.Fatalf("pinned %d, want %d (whole tree reachable via the fresh R1 bitmap)", n, len(tree))
	}
	// R1's cached value is the DB one, not the stale file one.
	gotR1, _, ok := c.Get(nibbles.HexToCompact([]byte(r1)))
	if !ok {
		t.Fatal("R1 not pinned")
	}
	if !bytes.Equal(gotR1, freshR1) {
		t.Fatalf("R1 cached value is not the DB value: got %x want %x", gotR1, freshR1)
	}
	// R1.7 is reachable only because the DB bitmap has bit 7 — it must be pinned.
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
		{},           // 64 — subtree root (even)
		{0x0}, {0xf}, // 65 (odd)
		{0x1, 0x2}, {0xf, 0xf}, // 66 (even)
		{0x3, 0x4, 0x5},           // 67 (odd)
		{0x6, 0x7, 0x8, 0x9},      // 68 (even)
		{0xa, 0xb, 0xc, 0xd, 0xe}, // 69 (odd)
		make([]byte, 64),          // 128 (even) — deepest
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
	// A different contract's branches must be in neither of A's ranges.
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
	// Fail when R||1 (depth 65) is requested -> root pinned at depth 64, then error.
	_, err := PreloadContractTrunkParallel(hash, 1<<20, nil, fakeResolver(tree, nil, 100, root+string([]byte{1})), c, nil)
	if err == nil {
		t.Fatal("expected error from the resolver")
	}
	if c.PinnedCount() == 0 {
		t.Fatal("the depth-64 root should have been pinned before the depth-65 failure")
	}
}

// --- Resumable ContractTrunkPreloadParallel tests (Run-by-Run) ---

// TestContractTrunkPreloadParallel_ResumeAcrossSteps confirms that splitting a
// full preload into multiple Run calls yields the same pinned set as a
// one-shot run with the equivalent total budget.
func TestContractTrunkPreloadParallel_ResumeAcrossSteps(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	const valSz = 100
	resolve := fakeResolver(tree, nil, valSz, "")

	// Reference: one-shot.
	cRef := NewBranchCache(64)
	if _, err := PreloadContractTrunkParallel(hash, 1<<20, nil, resolve, cRef, nil); err != nil {
		t.Fatal(err)
	}

	// Step-by-step: budget exactly one entry per Run (the budget is checked
	// before the entry is pinned; with overhead we need at least one entry's
	// worth per step to make progress).
	c := NewBranchCache(64)
	p, err := NewContractTrunkPreloadParallel(hash)
	if err != nil {
		t.Fatal(err)
	}
	// Each entry is ~estimatedEntryOverheadBytes + 33 + valSz; over-allocate a
	// bit per step so we always pin at least one new entry.
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
	// Spot-check: every path in the reference is in the step-by-step cache.
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

// TestContractTrunkPreloadParallel_RunAfterCompleteIsNoOp confirms that once
// the BFS reaches an empty frontier, further Run calls are no-ops.
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

// TestContractTrunkPreloadParallel_StepBudgetCaps confirms that a small step
// budget stops the BFS even when the frontier has more work — and the saved
// state has the queue position preserved for the next call.
func TestContractTrunkPreloadParallel_StepBudgetCaps(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	const valSz = 100
	resolve := fakeResolver(tree, nil, valSz, "")
	c := NewBranchCache(64)
	p, err := NewContractTrunkPreloadParallel(hash)
	if err != nil {
		t.Fatal(err)
	}
	// Budget for ~3 entries (depth-64 root + 2 depth-65 children).
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
	// Now exhaust with a full follow-on budget.
	_, done2, err := p.Run(1<<20, nil, resolve, c, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !done2 {
		t.Fatalf("expected done after large follow-on budget; queue=%d", p.QueueRemaining())
	}
	// In our synthetic tree only 3 of 7 paths sit at depths 64-65; the rest
	// require descending past the truncated wave. The cap-per-wave logic
	// drops the truncated wave's tail (BFS-wise) but the children of the
	// pinned ones progress on the next call. Cumulative pinned should be
	// >= the step-1 count.
	if p.PinnedTotal() <= n1 {
		t.Fatalf("follow-on Run made no progress: pinned still %d (step-1 was %d)", p.PinnedTotal(), n1)
	}
}

// TestContractTrunkPreloadParallel_ResumeAfterResolverError confirms that a
// resolver error preserves the partial state — a retry on the next Run picks
// up from the same wave once the resolver is healthy again.
func TestContractTrunkPreloadParallel_ResumeAfterResolverError(t *testing.T) {
	hash, tree, _ := buildSyntheticTree(t)
	root := ""
	for p := range tree {
		if root == "" || len(p) < len(root) {
			root = p
		}
	}
	const valSz = 100
	// Fail when R||1 is requested (a depth-65 key) — depth-64 wave succeeds.
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
	// Depth-64 root should still be pinned (it was the previous wave).
	if c.PinnedCount() == 0 {
		t.Fatal("expected the depth-64 root pinned before the depth-65 wave failed")
	}
	preErrPinned := p.PinnedTotal()
	// Retry with a healthy resolver — should pick up where we left off and
	// finish. The previous partial wave will be re-attempted in the new
	// call (the failing wave's frontier WAS advanced past the depth where
	// the error fired — the error path returns before updating
	// p.frontier/p.nextDepth, so retry sees the same depth's frontier).
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
	// Whole tree should be pinned by now.
	if p.PinnedTotal() != len(tree) {
		t.Fatalf("after retry pinned %d, want %d", p.PinnedTotal(), len(tree))
	}
}

// TestContractTrunkPreloadParallel_DbBranchesPerStep confirms that dbBranches
// can change between Run calls (caller may pass a freshly-prefetched overlay
// per block) and that the freshest values shadow file values per call.
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

	// On the first wave (depth 64) supply a fresh R value via dbBranches.
	freshRoot := branchVal(tree[root], valSz)
	freshRoot[4] = 0xAB
	dbWave0 := map[string][]byte{string(nibbles.HexToCompact([]byte(root))): freshRoot}

	c := NewBranchCache(64)
	p, err := NewContractTrunkPreloadParallel(hash)
	if err != nil {
		t.Fatal(err)
	}
	// Wave 0: pin the root using dbBranches (one entry budget).
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

	// Wave 1: depth 65. Pass an empty dbBranches (file-only); resolver
	// supplies stale-bitmap values.
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

// TestContractTrunkPreloadParallel_PinnedPrefixesAccumulate confirms that
// PinnedPrefixes() accumulates across Run calls (needed for demote-time
// cache invalidation in the adaptive controller).
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
	// Two small steps then one big step.
	for i := 0; i < 2; i++ {
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
	// Every prefix must be in the cache.
	for _, pf := range prefixes {
		if _, _, ok := c.Get(pf); !ok {
			t.Fatalf("prefix %x in PinnedPrefixes but not in cache", pf)
		}
	}
	// All prefixes are unique.
	seen := map[string]bool{}
	for _, pf := range prefixes {
		if seen[string(pf)] {
			t.Fatalf("duplicate prefix %x in PinnedPrefixes", pf)
		}
		seen[string(pf)] = true
	}
}

// TestContractTrunkPreloadParallel_NilCacheError + NilResolverError confirm
// the input-validation guards.
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

func TestContractTrunkPreloadParallel_BadHashLengthError(t *testing.T) {
	if _, err := NewContractTrunkPreloadParallel(make([]byte, 31)); err == nil {
		t.Fatal("expected error for 31-byte hash")
	}
	if _, err := NewContractTrunkPreloadParallel(make([]byte, 33)); err == nil {
		t.Fatal("expected error for 33-byte hash")
	}
}
