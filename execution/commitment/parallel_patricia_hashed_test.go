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

package commitment

import (
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

func TestParallelPatriciaHashedSkeletonConstruction(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)
	require.NotNil(t, p)
	require.NotNil(t, p.template, "template HexPatriciaHashed allocated")
	assert.Equal(t, int16(length.Addr), p.accountKeyLen)
	assert.Equal(t, runtime.NumCPU(), p.numWorkers)
	assert.Equal(t, MinSplitKeys, p.minSplitKeys)
	assert.Nil(t, p.rootHash.Load())
}

func TestParallelPatriciaHashedSkeletonRootTrie(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)
	require.Same(t, p.template, p.RootTrie())
}

func TestParallelPatriciaHashedSkeletonVariant(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)
	assert.Equal(t, VariantParallelHexPatricia, p.Variant())
	assert.Equal(t, TrieVariant("hex-parallel-patricia-hashed"), p.Variant())
}

func TestParallelPatriciaHashedSkeletonParseTrieVariant(t *testing.T) {
	assert.Equal(t, VariantParallelHexPatricia, ParseTrieVariant("parallel"))
	// Existing variants still parse to the expected values.
	assert.Equal(t, VariantHexPatriciaTrie, ParseTrieVariant("hex"))
	assert.Equal(t, VariantConcurrentHexPatricia, ParseTrieVariant("hex-parallel"))
	assert.Equal(t, VariantBinPatriciaTrie, ParseTrieVariant("bin"))
	// Unknown falls back to the default hex variant.
	assert.Equal(t, VariantHexPatriciaTrie, ParseTrieVariant("nonsense"))
}

func TestParallelPatriciaHashedSkeletonSetNumWorkers(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)

	p.SetNumWorkers(4)
	assert.Equal(t, 4, p.numWorkers)

	// Non-positive values fall back to runtime.NumCPU.
	p.SetNumWorkers(0)
	assert.Equal(t, runtime.NumCPU(), p.numWorkers)
	p.SetNumWorkers(-3)
	assert.Equal(t, runtime.NumCPU(), p.numWorkers)
}

func TestParallelPatriciaHashedSkeletonSetMinSplitKeys(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)

	p.SetMinSplitKeys(7)
	assert.Equal(t, uint32(7), p.minSplitKeys)

	// Zero falls back to the package default.
	p.SetMinSplitKeys(0)
	assert.Equal(t, MinSplitKeys, p.minSplitKeys)
}

func TestParallelPatriciaHashedSkeletonReset(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)
	stashed := []byte{0xde, 0xad}
	p.rootHash.Store(&stashed)
	require.NotNil(t, p.rootHash.Load())

	p.Reset()
	assert.Nil(t, p.rootHash.Load(), "Reset clears rootHash")
	require.NotNil(t, p.template, "Reset preserves the template")
}

func TestParallelPatriciaHashedSkeletonResetContextPropagates(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)
	ms := NewMockState(t)

	p.ResetContext(ms)
	assert.Same(t, ms, PatriciaContext(p.template.ctx), "context propagated to template")
}

func TestParallelPatriciaHashedSkeletonSetTrieContextFactory(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)
	assert.Nil(t, p.trieCtxFactory)

	ms := NewMockState(t)
	called := 0
	f := func() (PatriciaContext, func()) {
		called++
		return ms, func() {}
	}
	p.SetTrieContextFactory(f)
	require.NotNil(t, p.trieCtxFactory)

	got, cleanup := p.trieCtxFactory()
	assert.Same(t, ms, got)
	assert.NotNil(t, cleanup)
	assert.Equal(t, 1, called)
}

func TestParallelPatriciaHashedSkeletonSetTraceFlags(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)

	p.SetTrace(true)
	assert.True(t, p.template.trace)
	p.SetTrace(false)
	assert.False(t, p.template.trace)

	p.SetTraceDomain(true)
	assert.True(t, p.template.traceDomain)
	p.SetTraceDomain(false)
	assert.False(t, p.template.traceDomain)
}

func TestParallelPatriciaHashedSkeletonEnableWarmupCache(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)

	p.EnableWarmupCache(true)
	assert.True(t, p.template.enableWarmupCache)
	p.EnableWarmupCache(false)
	assert.False(t, p.template.enableWarmupCache)
}

func TestParallelPatriciaHashedSkeletonCaptureRoundTrip(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)

	capture := []string{"alpha", "beta"}
	p.SetCapture(capture)
	assert.Equal(t, capture, p.GetCapture(false), "GetCapture returns the set capture without truncation")
	assert.Equal(t, capture, p.GetCapture(true), "truncating GetCapture returns the previous capture")
	assert.Nil(t, p.GetCapture(false), "capture cleared after truncate")
}

func TestParallelPatriciaHashedSkeletonEnableCsvMetricsNoPanic(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)
	// Empty prefix is a valid no-op in HexPatriciaHashed.EnableCsvMetrics —
	// we only verify the delegation does not panic.
	require.NotPanics(t, func() { p.EnableCsvMetrics("") })
}

func TestParallelPatriciaHashedSkeletonReleaseNilSafe(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)
	require.NotNil(t, p.template)

	p.Release()
	assert.Nil(t, p.template, "Release drops the template")

	// Subsequent Release is a no-op.
	require.NotPanics(t, func() { p.Release() })

	// Plumbing methods stay safe after Release.
	require.NotPanics(t, func() {
		p.SetTrace(true)
		p.SetTraceDomain(true)
		p.EnableWarmupCache(true)
		p.SetCapture(nil)
		_ = p.GetCapture(false)
		p.EnableCsvMetrics("")
		p.ResetContext(nil)
	})
}

func TestParallelPatriciaHashedSkeletonRootHashStashed(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)
	defer p.Release()

	stored := []byte{0xde, 0xad, 0xbe, 0xef}
	p.rootHash.Store(&stored)

	got, err := p.RootHash()
	require.NoError(t, err)
	assert.Equal(t, stored, got)

	// Returned slice must be a copy so callers cannot mutate the published
	// value.
	got[0] = 0xff
	regot, err := p.RootHash()
	require.NoError(t, err)
	assert.Equal(t, byte(0xde), regot[0], "stashed root not mutated by caller")
}

func TestParallelPatriciaHashedSkeletonRootHashFallsBackToTemplate(t *testing.T) {
	ms := NewMockState(t)

	p := NewParallelPatriciaHashed(nil, length.Addr)
	defer p.Release()
	p.ResetContext(ms)

	got, err := p.RootHash()
	require.NoError(t, err)

	// Reference: a freshly constructed sequential trie returns the same hash
	// for the no-updates path.
	seq := NewHexPatriciaHashed(length.Addr, ms)
	defer seq.Release()
	expected, err := seq.RootHash()
	require.NoError(t, err)

	assert.Equal(t, expected, got, "RootHash falls back to template for the no-updates path")
}

func TestParallelPatriciaHashedSkeletonRootHashAfterRelease(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr)
	p.Release()

	got, err := p.RootHash()
	require.NoError(t, err)
	assert.Nil(t, got, "RootHash on released instance returns nil")
}

// assertEquivalentRoot drives the same update set through sequential
// HexPatriciaHashed (ModeDirect) and ParallelPatriciaHashed (ModeParallel)
// against independent in-memory MockState backends and asserts byte-equal
// root hashes. Returns the (shared) root hash so callers can do additional
// assertions.
//
// This is the helper that enforces the cardinal correctness rule for every
// end-to-end ModeParallel test. raiseMinSplitKeys, when > 0, raises the
// split-point threshold on the parallel side so tests in Task 6 scope can
// suppress split-point emission (the barrier protocol arrives in Task 7).
//
// Worker count defaults to 1. Use assertEquivalentRootWorkers when a test
// needs multiple workers running concurrently (Task 7 barrier tests use this
// for race detector coverage).
func assertEquivalentRoot(
	t *testing.T,
	plainKeys [][]byte,
	updates []Update,
	raiseMinSplitKeys uint32,
) []byte {
	return assertEquivalentRootWorkers(t, plainKeys, updates, raiseMinSplitKeys, 1)
}

// assertEquivalentRootWorkers is the multi-worker variant of
// assertEquivalentRoot. numWorkers <= 0 falls back to runtime.NumCPU on the
// ParallelPatriciaHashed side.
func assertEquivalentRootWorkers(
	t *testing.T,
	plainKeys [][]byte,
	updates []Update,
	raiseMinSplitKeys uint32,
	numWorkers int,
) []byte {
	t.Helper()
	ctx := context.Background()

	// Sequential side.
	seqMs := NewMockState(t)
	require.NoError(t, seqMs.applyPlainUpdates(plainKeys, updates))
	seqTrie := NewHexPatriciaHashed(length.Addr, seqMs)
	defer seqTrie.Release()
	seqUpds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer seqUpds.Close()
	seqRoot, err := seqTrie.Process(ctx, seqUpds, "", nil, WarmupConfig{})
	require.NoError(t, err)

	// Parallel side.
	parMs := NewMockState(t)
	require.NoError(t, parMs.applyPlainUpdates(plainKeys, updates))
	parTrie := NewParallelPatriciaHashed(mockTrieCtxFactory(parMs), length.Addr)
	defer parTrie.Release()
	parTrie.SetNumWorkers(numWorkers)
	if raiseMinSplitKeys > 0 {
		parTrie.SetMinSplitKeys(raiseMinSplitKeys)
	}
	parTrie.ResetContext(parMs)

	parUpds := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer parUpds.Close()
	for i, k := range plainKeys {
		i, k := i, k
		ks := string(k)
		parUpds.TouchPlainKey(ks, nil, func(c *KeyUpdate, _ []byte) {
			c.plainKey = ks
			c.hashedKey = KeyToHexNibbleHash(k)
			c.update = &updates[i]
		})
	}
	parRoot, err := parTrie.Process(ctx, parUpds, "", nil, WarmupConfig{})
	require.NoError(t, err)

	require.Equal(t, seqRoot, parRoot,
		"sequential and parallel root hashes must match (raiseMinSplitKeys=%d, numWorkers=%d)", raiseMinSplitKeys, numWorkers)
	return seqRoot
}

// TestParallelProcessSkeleton_EmptyUpdates: zero touched keys. Both modes
// must return the empty-trie root.
func TestParallelProcessSkeleton_EmptyUpdates(t *testing.T) {
	t.Parallel()
	root := assertEquivalentRoot(t, nil, nil, 0)
	require.NotEmpty(t, root)
}

// TestParallelProcessSkeleton_SingleAccount: one touched key. Prepare emits
// a single leafTask with no split-points. The single worker scans its nibble
// bucket, processes the key, folds to root, publishes the hash.
func TestParallelProcessSkeleton_SingleAccount(t *testing.T) {
	t.Parallel()
	plainKeys, updates := NewUpdateBuilder().
		Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 42).
		Build()
	root := assertEquivalentRoot(t, plainKeys, updates, 0)
	require.NotEmpty(t, root)
}

// TestParallelProcessSkeleton_SingleNibbleBucket: many accounts colliding
// into one root nibble. Prepare emits a single leafTask (no split-points).
// MinSplitKeys is left at default; the addresses are chosen so the only
// fork inside the bucket sits below the threshold.
func TestParallelProcessSkeleton_SingleNibbleBucket(t *testing.T) {
	t.Parallel()

	const targetNibble = 0x0
	const numAddrs = 8 // well below MinSplitKeys (32)

	ub := NewUpdateBuilder()
	for i := range numAddrs {
		addr := findAddressForNibble(targetNibble, i)
		ub.Balance(addrHex(addr), uint64(100+i))
	}
	plainKeys, updates := ub.Build()

	root := assertEquivalentRoot(t, plainKeys, updates, 0)
	require.NotEmpty(t, root)
}

// TestParallelProcessSkeleton_MultipleNibblesNoSplit: accounts span multiple
// root nibbles but with MinSplitKeys raised so high that no split-point is
// emitted. Each nibble produces its own leafTask, but the test sets
// NumWorkers=1 so workers run serially — and there is at most one leafTask
// per nibble, satisfying the Task 6 invariant.
//
// Note: this exercises the multi-bucket dispatch path (each nibble has its
// own goroutine) but each goroutine still owns its bucket entirely.
//
// SKIPPED until Task 7: the current Process implementation rejects multiple
// surviving workers because there is no barrier to merge their roots, which
// happens whenever more than one nibble bucket is touched.
func TestParallelProcessSkeleton_MultipleNibblesNoSplit(t *testing.T) {
	t.Skip("multi-bucket leafTasks require the Task 7 barrier protocol to merge worker roots")
}

// TestParallelProcessSkeleton_RaisedThresholdSuppressesSplit: even at the
// bloatnet-ish density where the trie would normally fork into split-points
// at depth 1, raising MinSplitKeys above the bucket size collapses the trie
// into a single leafTask for that bucket.
func TestParallelProcessSkeleton_RaisedThresholdSuppressesSplit(t *testing.T) {
	t.Parallel()

	const targetNibble = 0x5
	const numAddrs = 64 // well above default MinSplitKeys=32

	ub := NewUpdateBuilder()
	for i := range numAddrs {
		addr := findAddressForNibble(targetNibble, i)
		ub.Balance(addrHex(addr), uint64(7000+i))
	}
	plainKeys, updates := ub.Build()

	// Raise the threshold above the touched-key count to suppress every
	// possible split-point. The single leafTask covers everything under the
	// nibble bucket.
	root := assertEquivalentRoot(t, plainKeys, updates, uint32(numAddrs+1))
	require.NotEmpty(t, root)
}

// TestParallelProcessSkeleton_RejectsMissingFactory verifies that calling
// Process without a trieCtxFactory returns an explicit error rather than
// crashing inside the worker.
func TestParallelProcessSkeleton_RejectsMissingFactory(t *testing.T) {
	t.Parallel()

	ms := NewMockState(t)
	p := NewParallelPatriciaHashed(nil, length.Addr)
	defer p.Release()
	p.ResetContext(ms)

	plainKeys, updates := NewUpdateBuilder().
		Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 42).
		Build()
	require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))

	upds := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer upds.Close()
	for i, k := range plainKeys {
		i, k := i, k
		ks := string(k)
		upds.TouchPlainKey(ks, nil, func(c *KeyUpdate, _ []byte) {
			c.plainKey = ks
			c.hashedKey = KeyToHexNibbleHash(k)
			c.update = &updates[i]
		})
	}

	_, err := p.Process(context.Background(), upds, "", nil, WarmupConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TrieContextFactory")
}

// TestParallelProcessSkeleton_RejectsNonParallelMode verifies the mode guard
// trips when Updates is in ModeDirect.
func TestParallelProcessSkeleton_RejectsNonParallelMode(t *testing.T) {
	t.Parallel()

	ms := NewMockState(t)
	p := NewParallelPatriciaHashed(mockTrieCtxFactory(ms), length.Addr)
	defer p.Release()
	p.ResetContext(ms)

	upds := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
	defer upds.Close()

	_, err := p.Process(context.Background(), upds, "", nil, WarmupConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ModeParallel")
}

// TestParallelProcessSkeleton_DispatchLeafKeysSingleTask covers the helper
// directly with a single leafTask — every key in the collector is routed to
// the task with no filtering.
func TestParallelProcessSkeleton_DispatchLeafKeysSingleTask(t *testing.T) {
	t.Parallel()

	upds := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer upds.Close()
	ub := NewUpdateBuilder()
	for i := range 5 {
		addr := findAddressForNibble(0xA, i)
		ub.Balance(addrHex(addr), uint64(i+1))
	}
	plainKeys, updates := ub.Build()
	for i, k := range plainKeys {
		i, k := i, k
		ks := string(k)
		upds.TouchPlainKey(ks, nil, func(c *KeyUpdate, _ []byte) {
			c.plainKey = ks
			c.hashedKey = KeyToHexNibbleHash(k)
			c.update = &updates[i]
		})
	}

	tasks := []leafTask{{prefix: []byte{0x0A}, keyCount: uint32(len(plainKeys))}}
	var got int
	err := dispatchLeafKeys(context.Background(), upds.nibbles[0x0A], tasks, func(idx int, _, _ []byte) error {
		assert.Equal(t, 0, idx, "single-task dispatch always returns idx 0")
		got++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, len(plainKeys), got, "every collected key reached the callback")
}

// TestParallelProcessSkeleton_DispatchLeafKeysLongestPrefixWins covers the
// helper directly with two leafTasks whose prefixes differ in depth. The
// longer prefix wins for keys it matches; the shorter prefix catches the
// rest.
func TestParallelProcessSkeleton_DispatchLeafKeysLongestPrefixWins(t *testing.T) {
	t.Parallel()

	upds := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer upds.Close()

	// Construct two keys that share the first nibble but diverge at depth 1.
	// We hand-craft the touched keys via TouchHashedKey to keep the prefix
	// arrangement controlled, then exercise the helper directly.
	upds.TouchHashedKey([]byte{0x0A, 0x01, 0x02})
	upds.TouchHashedKey([]byte{0x0A, 0x02, 0x03})

	tasks := []leafTask{
		{prefix: []byte{0x0A}, keyCount: 1},       // shorter
		{prefix: []byte{0x0A, 0x02}, keyCount: 1}, // longer — wins for [0A 02 ...]
	}

	taskOfKey := map[string]int{}
	err := dispatchLeafKeys(context.Background(), upds.nibbles[0x0A], tasks, func(idx int, hk, _ []byte) error {
		taskOfKey[string(hk)] = idx
		return nil
	})
	require.NoError(t, err)

	assert.Equal(t, 0, taskOfKey[string([]byte{0x0A, 0x01, 0x02})], "[0A 01 02] should fall to the short prefix task")
	assert.Equal(t, 1, taskOfKey[string([]byte{0x0A, 0x02, 0x03})], "[0A 02 03] should match the longer prefix task")
}

// TestParallelProcessSkeleton_GroupLeafTasksByNibble: bookkeeping for the
// per-nibble dispatch loop.
func TestParallelProcessSkeleton_GroupLeafTasksByNibble(t *testing.T) {
	t.Parallel()

	queue := []leafTask{
		{prefix: []byte{0x01}},
		{prefix: []byte{0x01, 0x02}},
		{prefix: []byte{0x05, 0x06}},
		{prefix: nil}, // dropped: empty prefix
	}
	got := groupLeafTasksByNibble(queue)
	assert.Len(t, got[0x01], 2)
	assert.Len(t, got[0x05], 1)
	assert.Len(t, got[0x0A], 0)
}

// TestParallelProcessSkeleton_WarmupAncestorsNoOp ensures the helper accepts
// a nil warmuper and a parallelUpdate without split-points without panicking
// — Task 6 leafTask-only configurations exercise the no-op branch.
func TestParallelProcessSkeleton_WarmupAncestorsNoOp(t *testing.T) {
	t.Parallel()
	p := NewParallelPatriciaHashed(mockTrieCtxFactory(NewMockState(t)), length.Addr)
	defer p.Release()

	require.NotPanics(t, func() {
		p.warmupSplitAncestors(nil, nil)
		p.warmupSplitAncestors(newParallelUpdate(), nil)
	})
}

// --- Task 7: fold-time barrier protocol tests -------------------------------
//
// Every test below drives a multi-leafTask update set through both modes via
// assertEquivalentRoot. The split-point structure is inferred from the
// key-distribution shape: enough touches across distinct top-level nibbles to
// push subtreeCount past MinSplitKeys forces Prepare to emit a root
// split-point with the corresponding fanout, which is exactly what the
// barrier coordinates.

// twoLeafTaskAddrs constructs N/2 addresses hashing to firstNibble + N/2
// addresses hashing to secondNibble. Together they give the prefix trie's
// root a fanout of exactly 2 and a subtreeCount equal to N, so the root
// becomes a split-point with two children.
func twoLeafTaskAddrs(t *testing.T, firstNibble, secondNibble int, perSide int) [][]byte {
	t.Helper()
	out := make([][]byte, 0, perSide*2)
	for i := range perSide {
		out = append(out, findAddressForNibble(firstNibble, i))
	}
	for i := range perSide {
		out = append(out, findAddressForNibble(secondNibble, i))
	}
	return out
}

// TestParallelBarrier_TwoLeafTasksOneSplitPoint exercises the minimal barrier
// scenario: a single root split-point with exactly two children. One worker
// per nibble bucket: the first to arrive deposits and exits, the second
// becomes the last-finisher, loads the deposited sibling cell, and folds the
// merged grid all the way to root.
//
// Setup: 16 + 16 = 32 accounts (== MinSplitKeys); fanout=2 at root => one
// split-point at empty prefix.
func TestParallelBarrier_TwoLeafTasksOneSplitPoint(t *testing.T) {
	t.Parallel()

	const perSide = int(MinSplitKeys) / 2
	addrs := twoLeafTaskAddrs(t, 0x3, 0x5, perSide)

	ub := NewUpdateBuilder()
	for i, addr := range addrs {
		ub.Balance(addrHex(addr), uint64(1_000+i))
	}
	plainKeys, updates := ub.Build()

	root := assertEquivalentRootWorkers(t, plainKeys, updates, 0, 2)
	require.NotEmpty(t, root)
}

// TestParallelBarrier_FanoutFour: a single root split-point with four
// children. Stresses the bitmap arithmetic when sp.touchedBitmap has multiple
// bits set and only one of the four workers (the last to arrive) reloads the
// shared grid.
//
// Setup: 8 accounts in each of four top-level nibbles. Total 32 ==
// MinSplitKeys.
func TestParallelBarrier_FanoutFour(t *testing.T) {
	t.Parallel()

	const perBucket = int(MinSplitKeys) / 4
	buckets := []int{0x1, 0x4, 0x7, 0xC}

	ub := NewUpdateBuilder()
	for _, nib := range buckets {
		for i := range perBucket {
			addr := findAddressForNibble(nib, i)
			ub.Balance(addrHex(addr), uint64(2_000+nib*100+i))
		}
	}
	plainKeys, updates := ub.Build()

	root := assertEquivalentRootWorkers(t, plainKeys, updates, 0, 4)
	require.NotEmpty(t, root)
}

// TestParallelBarrier_AsymmetricWorkload: a single root split-point with
// three children of varying sizes. Verifies that the scheduling order
// (leafQueue is sorted descending by keyCount) does not change the resulting
// root hash and that the largest worker — which finishes last because it has
// the most work — can equally play either the depositor or the last-finisher
// role depending on Go's goroutine scheduling.
//
// Setup: 24 + 4 + 4 = 32 accounts; root fanout=3.
func TestParallelBarrier_AsymmetricWorkload(t *testing.T) {
	t.Parallel()

	ub := NewUpdateBuilder()
	for i := range 24 {
		addr := findAddressForNibble(0x2, i)
		ub.Balance(addrHex(addr), uint64(3_000+i))
	}
	for i := range 4 {
		addr := findAddressForNibble(0x6, i)
		ub.Balance(addrHex(addr), uint64(5_000+i))
	}
	for i := range 4 {
		addr := findAddressForNibble(0xB, i)
		ub.Balance(addrHex(addr), uint64(7_000+i))
	}
	plainKeys, updates := ub.Build()

	root := assertEquivalentRootWorkers(t, plainKeys, updates, 0, 3)
	require.NotEmpty(t, root)
}

// TestParallelBarrier_ChainedSplitPoints: two split-points along the same
// ancestor chain. With ≥32 accounts under one top-level nibble whose second
// nibbles are distributed across enough distinct values, Prepare emits both
// a root split-point AND a depth-1 split-point under that nibble. Workers
// converge at the inner split-point first; the last-finisher there crosses
// the inner barrier, folds upward, and deposits again at the root
// split-point.
//
// Setup: 32 accounts in nibble 0x0 (whose second nibbles fan out widely) +
// 16 accounts in nibble 0xF. Root: fanout=2, subtreeCount=48 (split-point).
// Depth-1 under 0x0: fanout >= 2, subtreeCount=32 (split-point).
func TestParallelBarrier_ChainedSplitPoints(t *testing.T) {
	t.Parallel()

	ub := NewUpdateBuilder()
	for i := range int(MinSplitKeys) {
		addr := findAddressForNibble(0x0, i)
		ub.Balance(addrHex(addr), uint64(9_000+i))
	}
	for i := range int(MinSplitKeys / 2) {
		addr := findAddressForNibble(0xF, i)
		ub.Balance(addrHex(addr), uint64(11_000+i))
	}
	plainKeys, updates := ub.Build()

	root := assertEquivalentRootWorkers(t, plainKeys, updates, 0, 8)
	require.NotEmpty(t, root)
}

// TestParallelBarrier_ProcessRejectsMultiBucketWithoutSplit: when Prepare
// emits multiple leafTasks but zero split-points, Process explicitly errors
// rather than folding workers to inconsistent roots. This is the
// counter-example to Task 7's correctness scope; the case is left for Task
// 10 (synthetic root barrier).
//
// Setup: 4 + 4 accounts in two top-level nibbles (below MinSplitKeys); root
// is not a split-point but produces two leafTasks. Process must reject.
func TestParallelBarrier_ProcessRejectsMultiBucketWithoutSplit(t *testing.T) {
	t.Parallel()

	ub := NewUpdateBuilder()
	for i := range 4 {
		addr := findAddressForNibble(0x4, i)
		ub.Balance(addrHex(addr), uint64(13_000+i))
	}
	for i := range 4 {
		addr := findAddressForNibble(0x9, i)
		ub.Balance(addrHex(addr), uint64(15_000+i))
	}
	plainKeys, updates := ub.Build()

	ms := NewMockState(t)
	require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))

	parTrie := NewParallelPatriciaHashed(mockTrieCtxFactory(ms), length.Addr)
	defer parTrie.Release()
	parTrie.SetNumWorkers(2)
	parTrie.ResetContext(ms)

	parUpds := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer parUpds.Close()
	for i, k := range plainKeys {
		i, k := i, k
		ks := string(k)
		parUpds.TouchPlainKey(ks, nil, func(c *KeyUpdate, _ []byte) {
			c.plainKey = ks
			c.hashedKey = KeyToHexNibbleHash(k)
			c.update = &updates[i]
		})
	}

	_, err := parTrie.Process(context.Background(), parUpds, "", nil, WarmupConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no split-points")
}

// TestParallelBarrier_ProduceCellEmpty: produceCellForBarrier guards against
// being called on an empty trie state.
func TestParallelBarrier_ProduceCellEmpty(t *testing.T) {
	t.Parallel()
	hph := NewHexPatriciaHashed(length.Addr, NewMockState(t))
	defer hph.Release()

	_, _, err := produceCellForBarrier(hph)
	require.Error(t, err, "produceCellForBarrier must reject activeRows==0")
}

// TestParallelBarrier_LoadSiblingsZeroes: loadSiblingsIntoGrid must zero
// stale cells in the target row before populating from sp.cells; otherwise a
// worker's earlier contribution at a different nibble would leak into the
// rebuilt branch.
func TestParallelBarrier_LoadSiblingsZeroes(t *testing.T) {
	t.Parallel()
	hph := NewHexPatriciaHashed(length.Addr, NewMockState(t))
	defer hph.Release()

	// Pre-populate the row with junk cells.
	row := 0
	for n := range 16 {
		hph.grid[row][n].hashLen = 32
		hph.grid[row][n].hash[0] = byte(0xDE)
	}

	sp := &splitPoint{
		prefix:        []byte{0x0A},
		touchedBitmap: uint16(1<<0x3 | 1<<0x5),
	}
	// Only nibble 3 and 5 carry payload; the other 14 must end up zeroed.
	sp.cells[0x3].hashLen = 32
	sp.cells[0x3].hash[0] = 0xAA
	sp.cells[0x5].hashLen = 32
	sp.cells[0x5].hash[0] = 0xBB

	loadSiblingsIntoGrid(hph, sp, row)

	for n := range 16 {
		c := &hph.grid[row][n]
		if n == 0x3 {
			assert.Equal(t, int16(32), c.hashLen, "nibble 0x3 preserved")
			assert.Equal(t, byte(0xAA), c.hash[0])
			continue
		}
		if n == 0x5 {
			assert.Equal(t, int16(32), c.hashLen, "nibble 0x5 preserved")
			assert.Equal(t, byte(0xBB), c.hash[0])
			continue
		}
		assert.Equal(t, int16(0), c.hashLen, "untouched nibble %x must be zeroed", n)
		assert.Equal(t, byte(0), c.hash[0], "untouched nibble %x hash byte must be zeroed", n)
	}
	assert.Equal(t, uint16(1<<0x3|1<<0x5), hph.touchMap[row])
	assert.Equal(t, uint16(1<<0x3|1<<0x5), hph.afterMap[row])
}

// TestParallelBarrier_LoadSiblingsRespectsDBSiblings populates a splitPoint
// with both touched (worker-deposited) cells and untouched DB cells, then
// verifies the reconstructed grid carries both sets — that's the
// untouched-nibble fix in action.
func TestParallelBarrier_LoadSiblingsRespectsDBSiblings(t *testing.T) {
	t.Parallel()
	hph := NewHexPatriciaHashed(length.Addr, NewMockState(t))
	defer hph.Release()

	sp := &splitPoint{
		prefix:        []byte{0x0A},
		touchedBitmap: uint16(1 << 0x3),
		dbBitmap:      uint16(1<<0x5 | 1<<0xA),
		branchBefore:  true,
	}
	sp.cells[0x3].hashLen = 32 // worker deposit
	sp.cells[0x3].hash[0] = 0xAA
	sp.cells[0x5].hashLen = 32 // DB sibling
	sp.cells[0x5].hash[0] = 0xCC
	sp.cells[0xA].hashLen = 32 // DB sibling
	sp.cells[0xA].hash[0] = 0xDD

	loadSiblingsIntoGrid(hph, sp, 0)

	for _, want := range []struct {
		nib  int
		byte byte
	}{{0x3, 0xAA}, {0x5, 0xCC}, {0xA, 0xDD}} {
		assert.Equal(t, int16(32), hph.grid[0][want.nib].hashLen, "nibble %x must be populated", want.nib)
		assert.Equal(t, want.byte, hph.grid[0][want.nib].hash[0])
	}
	assert.Equal(t, uint16(1<<0x3), hph.touchMap[0],
		"touchMap reflects only touched bitmap; DB-only siblings are not 'touched'")
	assert.Equal(t, uint16(1<<0x3|1<<0x5|1<<0xA), hph.afterMap[0],
		"afterMap covers every nibble we just installed")
	assert.True(t, hph.branchBefore[0], "branchBefore propagates from splitPoint")
}

// TestParallelBarrier_LoadSiblingsTouchedAndDeleted handles the
// delete-into-splitPoint case: a touched nibble whose deposited cell is
// empty (the worker deleted the only key in its subtree). The afterMap must
// reflect the absence so the branch encoder emits a deletion at that slot.
func TestParallelBarrier_LoadSiblingsTouchedAndDeleted(t *testing.T) {
	t.Parallel()
	hph := NewHexPatriciaHashed(length.Addr, NewMockState(t))
	defer hph.Release()

	sp := &splitPoint{
		prefix:        []byte{0x0A},
		touchedBitmap: uint16(1<<0x3 | 1<<0x5),
		dbBitmap:      0,
	}
	// Nibble 0x3 = live; nibble 0x5 = touched-and-deleted (zero cellEncodeData).
	sp.cells[0x3].hashLen = 32
	sp.cells[0x3].hash[0] = 0xAA

	loadSiblingsIntoGrid(hph, sp, 0)

	assert.Equal(t, int16(32), hph.grid[0][0x3].hashLen, "live touched nibble survives")
	assert.Equal(t, int16(0), hph.grid[0][0x5].hashLen, "deleted touched nibble must be zero")
	assert.Equal(t, uint16(1<<0x3|1<<0x5), hph.touchMap[0],
		"touchMap covers both touched nibbles even when one is deleted")
	assert.Equal(t, uint16(1<<0x3), hph.afterMap[0],
		"afterMap drops deleted touched nibbles")
}
