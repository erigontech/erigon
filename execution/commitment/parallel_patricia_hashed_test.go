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
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

func TestParallelPatriciaHashedSkeletonConstruction(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())
	require.NotNil(t, p)
	require.NotNil(t, p.template, "template HexPatriciaHashed allocated")
	assert.Equal(t, int16(length.Addr), p.accountKeyLen)
	assert.Equal(t, runtime.NumCPU(), p.numWorkers)
	assert.Nil(t, p.rootHash.Load())
}

func TestParallelPatriciaHashedSkeletonRootTrie(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())
	require.Same(t, p.template, p.RootTrie())
}

func TestParallelPatriciaHashedSkeletonVariant(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())
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
	p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())

	p.SetNumWorkers(4)
	assert.Equal(t, 4, p.numWorkers)

	// Non-positive values fall back to runtime.NumCPU.
	p.SetNumWorkers(0)
	assert.Equal(t, runtime.NumCPU(), p.numWorkers)
	p.SetNumWorkers(-3)
	assert.Equal(t, runtime.NumCPU(), p.numWorkers)
}

func TestParallelPatriciaHashedSkeletonReset(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())
	stashed := []byte{0xde, 0xad}
	p.rootHash.Store(&stashed)
	require.NotNil(t, p.rootHash.Load())

	p.Reset()
	assert.Nil(t, p.rootHash.Load(), "Reset clears rootHash")
	require.NotNil(t, p.template, "Reset preserves the template")
}

func TestParallelPatriciaHashedSkeletonResetContextPropagates(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())
	ms := NewMockState(t)

	p.ResetContext(ms)
	assert.Same(t, ms, PatriciaContext(p.template.ctx), "context propagated to template")
}

func TestParallelPatriciaHashedSkeletonSetTrieContextFactory(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())
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
	p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())

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
	p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())

	p.EnableWarmupCache(true)
	assert.True(t, p.template.enableWarmupCache)
	p.EnableWarmupCache(false)
	assert.False(t, p.template.enableWarmupCache)
}

func TestParallelPatriciaHashedSkeletonCaptureRoundTrip(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())

	capture := []string{"alpha", "beta"}
	p.SetCapture(capture)
	assert.Equal(t, capture, p.GetCapture(false), "GetCapture returns the set capture without truncation")
	assert.Equal(t, capture, p.GetCapture(true), "truncating GetCapture returns the previous capture")
	assert.Nil(t, p.GetCapture(false), "capture cleared after truncate")
}

func TestParallelPatriciaHashedSkeletonEnableCsvMetricsNoPanic(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())
	// Empty prefix is a valid no-op in HexPatriciaHashed.EnableCsvMetrics —
	// we only verify the delegation does not panic.
	require.NotPanics(t, func() { p.EnableCsvMetrics("") })
}

func TestParallelPatriciaHashedSkeletonReleaseNilSafe(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())
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
	p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())
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

	p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())
	defer p.Release()
	p.ResetContext(ms)

	got, err := p.RootHash()
	require.NoError(t, err)

	// Reference: a freshly constructed sequential trie returns the same hash
	// for the no-updates path.
	seq := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer seq.Release()
	expected, err := seq.RootHash()
	require.NoError(t, err)

	assert.Equal(t, expected, got, "RootHash falls back to template for the no-updates path")
}

func TestParallelPatriciaHashedSkeletonRootHashAfterRelease(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())
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
// end-to-end ModeParallel test.
//
// Worker count defaults to 1. Use assertEquivalentRootWorkers when a test
// needs multiple workers running concurrently.
func assertEquivalentRoot(
	t *testing.T,
	plainKeys [][]byte,
	updates []Update,
) []byte {
	return assertEquivalentRootWorkers(t, plainKeys, updates, 1)
}

// assertEquivalentRootWorkers is the multi-worker variant of
// assertEquivalentRoot. numWorkers <= 0 falls back to runtime.NumCPU on the
// ParallelPatriciaHashed side.
func assertEquivalentRootWorkers(
	t *testing.T,
	plainKeys [][]byte,
	updates []Update,
	numWorkers int,
) []byte {
	t.Helper()
	ctx := context.Background()

	// Sequential side.
	seqMs := NewMockState(t)
	require.NoError(t, seqMs.applyPlainUpdates(plainKeys, updates))
	seqTrie := NewHexPatriciaHashed(length.Addr, seqMs, DefaultTrieConfig())
	defer seqTrie.Release()
	seqUpds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer seqUpds.Close()
	seqRoot, err := seqTrie.Process(ctx, seqUpds, "", nil, WarmupConfig{})
	require.NoError(t, err)

	// Parallel side.
	parMs := NewMockState(t)
	parMs.SetConcurrentCommitment(true)
	require.NoError(t, parMs.applyPlainUpdates(plainKeys, updates))
	parTrie := NewParallelPatriciaHashed(mockTrieCtxFactory(parMs), length.Addr, DefaultTrieConfig())
	defer parTrie.Release()
	parTrie.SetNumWorkers(numWorkers)
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
		"sequential and parallel root hashes must match (numWorkers=%d)", numWorkers)
	return seqRoot
}

// TestParallelProcessSkeleton_EmptyUpdates: zero touched keys. Both modes
// must return the empty-trie root.
func TestParallelProcessSkeleton_EmptyUpdates(t *testing.T) {
	t.Parallel()
	root := assertEquivalentRoot(t, nil, nil)
	require.NotEmpty(t, root)
}

// TestParallelProcessSkeleton_SingleAccount: one touched key. The mount worker
// scans its nibble bucket, processes the key, and folds to root.
func TestParallelProcessSkeleton_SingleAccount(t *testing.T) {
	t.Parallel()
	plainKeys, updates := NewUpdateBuilder().
		Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 42).
		Build()
	root := assertEquivalentRoot(t, plainKeys, updates)
	require.NotEmpty(t, root)
}

// TestParallelProcessSkeleton_SingleNibbleBucket: many accounts colliding
// into one root nibble, folded by a single mount worker.
func TestParallelProcessSkeleton_SingleNibbleBucket(t *testing.T) {
	t.Parallel()

	const targetNibble = 0x0
	const numAddrs = 8

	ub := NewUpdateBuilder()
	for i := range numAddrs {
		addr := findAddressForNibble(targetNibble, i)
		ub.Balance(addrHex(addr), uint64(100+i))
	}
	plainKeys, updates := ub.Build()

	root := assertEquivalentRoot(t, plainKeys, updates)
	require.NotEmpty(t, root)
}

// TestParallelProcessSkeleton_RejectsMissingFactory verifies that calling
// Process without a trieCtxFactory returns an explicit error rather than
// crashing inside the worker.
func TestParallelProcessSkeleton_RejectsMissingFactory(t *testing.T) {
	t.Parallel()

	ms := NewMockState(t)
	p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())
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
	p := NewParallelPatriciaHashed(mockTrieCtxFactory(ms), length.Addr, DefaultTrieConfig())
	defer p.Release()
	p.ResetContext(ms)

	upds := NewUpdates(ModeDirect, t.TempDir(), KeyToHexNibbleHash)
	defer upds.Close()

	_, err := p.Process(context.Background(), upds, "", nil, WarmupConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ModeParallel")
}

// TestDFSSubtree walks a built subtree and asserts keys emerge in sorted nibble
// order with their plainKeys, and that a terminator that is a prefix of others
// (account above storage) emits before its children.
func TestDFSSubtree(t *testing.T) {
	t.Parallel()

	pu := newParallelUpdate()
	pu.Insert(nibs(0x01, 0x02, 0x03), []byte("pk-A"), nil)
	pu.Insert(nibs(0x01, 0x02, 0x04), []byte("pk-B"), nil)
	pu.Insert(nibs(0x05, 0x06, 0x07), []byte("pk-C"), nil)
	pu.Insert(nibs(0x01, 0x02), []byte("pk-D"), nil) // terminator that is a prefix of A and B

	type kv struct{ hk, pk string }
	var got []kv
	err := dfsSubtree(pu.trie.root, nil, func(hk, pk []byte, _ *Update) error {
		got = append(got, kv{hk: fmt.Sprintf("%x", hk), pk: string(pk)})
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, []kv{
		{hk: "0102", pk: "pk-D"},
		{hk: "010203", pk: "pk-A"},
		{hk: "010204", pk: "pk-B"},
		{hk: "050607", pk: "pk-C"},
	}, got)
}

// TestDFSSubtree_NilPlainKeyLeafErrors: a hashed-only touch leaves a terminator
// without a plainKey, which the parallel fold cannot resolve — fail loudly.
func TestDFSSubtree_NilPlainKeyLeafErrors(t *testing.T) {
	t.Parallel()

	pu := newParallelUpdate()
	pu.Insert(nibs(0x01, 0x02, 0x03), nil, nil)
	err := dfsSubtree(pu.trie.root, nil, func(_, _ []byte, _ *Update) error { return nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plainKey")
}

// TestParallelPatriciaHashedTemplateMirrorsPublishedRoot verifies that after a
// successful Process the template's root cell mirrors the publishing worker's
// final root, so RootHash() returns the correct value via the template
// fallback. Without the mirror, the template stays at its initial empty state
// and downstream paths (zero-update fast-path, encode/restore for the parallel
// trie variant in commitmentdb) return the empty-trie hash instead of the
// computed root.
func TestParallelPatriciaHashedTemplateMirrorsPublishedRoot(t *testing.T) {
	t.Parallel()

	plainKeys, updates := NewUpdateBuilder().
		Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 42).
		Build()

	parMs := NewMockState(t)
	parMs.SetConcurrentCommitment(true)
	require.NoError(t, parMs.applyPlainUpdates(plainKeys, updates))

	p := NewParallelPatriciaHashed(mockTrieCtxFactory(parMs), length.Addr, DefaultTrieConfig())
	defer p.Release()
	p.SetNumWorkers(1)
	p.ResetContext(parMs)

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

	published, err := p.Process(context.Background(), parUpds, "", nil, WarmupConfig{})
	require.NoError(t, err)
	require.NotEmpty(t, published)

	// Drop the atomic publish so RootHash must compute from the template's
	// root cell — emulating a fresh instance (post-restart) or any caller
	// that hits the template fallback.
	p.rootHash.Store(nil)

	got, err := p.RootHash()
	require.NoError(t, err)
	require.Equal(t, published, got,
		"template.RootHash must return the published root once the worker has folded into it")
}

// TestParallelPatriciaHashedStateRoundTrip drives Process, encodes the
// resulting trie state via the template, restores it into a fresh template,
// and asserts the restored RootHash matches the originally published value.
// This is the persistence path the commitmentdb layer takes for the parallel
// trie variant.
func TestParallelPatriciaHashedStateRoundTrip(t *testing.T) {
	t.Parallel()

	plainKeys, updates := NewUpdateBuilder().
		Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 42).
		Build()

	parMs := NewMockState(t)
	parMs.SetConcurrentCommitment(true)
	require.NoError(t, parMs.applyPlainUpdates(plainKeys, updates))

	p := NewParallelPatriciaHashed(mockTrieCtxFactory(parMs), length.Addr, DefaultTrieConfig())
	defer p.Release()
	p.SetNumWorkers(1)
	p.ResetContext(parMs)

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

	published, err := p.Process(context.Background(), parUpds, "", nil, WarmupConfig{})
	require.NoError(t, err)
	require.NotEmpty(t, published)

	// The template's root flags must mirror the worker's terminal state. They
	// are serialized below and any drift surfaces as a restore-then-continue
	// bug on the next unfold/fold cycle.
	tmpl := p.RootTrie()
	require.True(t, tmpl.rootChecked, "template.rootChecked must be promoted from the worker")
	require.True(t, tmpl.rootTouched, "template.rootTouched must be promoted from the worker")
	require.True(t, tmpl.rootPresent, "template.rootPresent must be promoted from the worker")

	encoded, err := tmpl.EncodeCurrentState(nil)
	require.NoError(t, err)
	require.NotEmpty(t, encoded, "EncodeCurrentState must capture template state mirrored from the worker")

	// Restore on a brand-new instance, simulating a process restart.
	p2 := NewParallelPatriciaHashed(mockTrieCtxFactory(parMs), length.Addr, DefaultTrieConfig())
	defer p2.Release()
	p2.ResetContext(parMs)
	require.NoError(t, p2.RootTrie().SetState(encoded))

	require.True(t, p2.RootTrie().rootChecked, "rootChecked must round-trip through SetState")
	require.True(t, p2.RootTrie().rootTouched, "rootTouched must round-trip through SetState")
	require.True(t, p2.RootTrie().rootPresent, "rootPresent must round-trip through SetState")

	restored, err := p2.RootHash()
	require.NoError(t, err)
	require.Equal(t, published, restored,
		"RootHash after SetState must reproduce the published root")
}
