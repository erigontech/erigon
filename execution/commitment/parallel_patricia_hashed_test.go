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
func assertEquivalentRoot(
	t *testing.T,
	plainKeys [][]byte,
	updates []Update,
	raiseMinSplitKeys uint32,
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
	parTrie.SetNumWorkers(1)
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
		"sequential and parallel root hashes must match (raiseMinSplitKeys=%d)", raiseMinSplitKeys)
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
