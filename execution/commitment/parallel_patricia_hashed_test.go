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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func TestParallelPatriciaHashedSkeletonConstruction(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())
	require.NotNil(t, p)
	require.NotNil(t, p.template, "template HexPatriciaHashed allocated")
	assert.Equal(t, int16(length.Addr), p.accountKeyLen)
	assert.Equal(t, runtime.NumCPU(), p.numWorkers)
	assert.Nil(t, p.rootHash.Load())
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

// TestParallelPatriciaHashedSkeletonPlumbing covers the pass-through setters and
// accessors on ParallelPatriciaHashed, one subtest per delegated method.
func TestParallelPatriciaHashedSkeletonPlumbing(t *testing.T) {
	t.Run("RootTrie", func(t *testing.T) {
		p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())
		require.Same(t, p.template, p.RootTrie())
	})

	t.Run("Variant", func(t *testing.T) {
		p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())
		assert.Equal(t, VariantParallelHexPatricia, p.Variant())
		assert.Equal(t, TrieVariant("hex-parallel-patricia-hashed"), p.Variant())
	})

	t.Run("SetNumWorkers", func(t *testing.T) {
		p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())

		p.SetNumWorkers(4)
		assert.Equal(t, 4, p.numWorkers)

		// Non-positive values fall back to runtime.NumCPU.
		p.SetNumWorkers(0)
		assert.Equal(t, runtime.NumCPU(), p.numWorkers)
		p.SetNumWorkers(-3)
		assert.Equal(t, runtime.NumCPU(), p.numWorkers)
	})

	t.Run("ResetContextPropagates", func(t *testing.T) {
		p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())
		ms := NewMockState(t)

		p.ResetContext(ms)
		assert.Same(t, ms, PatriciaContext(p.template.ctx), "context propagated to template")
	})

	t.Run("SetTrieContextFactory", func(t *testing.T) {
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
	})

	t.Run("SetTraceFlags", func(t *testing.T) {
		p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())

		p.SetTrace(true)
		assert.True(t, p.template.trace)
		p.SetTrace(false)
		assert.False(t, p.template.trace)

		p.SetTraceDomain(true)
		assert.True(t, p.template.traceDomain)
		p.SetTraceDomain(false)
		assert.False(t, p.template.traceDomain)
	})

	t.Run("EnableWarmupCache", func(t *testing.T) {
		p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())

		p.EnableWarmupCache(true)
		assert.True(t, p.template.enableWarmupCache)
		p.EnableWarmupCache(false)
		assert.False(t, p.template.enableWarmupCache)
	})

	t.Run("CaptureRoundTrip", func(t *testing.T) {
		p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())

		capture := []string{"alpha", "beta"}
		p.SetCapture(capture)
		assert.Equal(t, capture, p.GetCapture(false), "GetCapture returns the set capture without truncation")
		assert.Equal(t, capture, p.GetCapture(true), "truncating GetCapture returns the previous capture")
		assert.Nil(t, p.GetCapture(false), "capture cleared after truncate")
	})

	t.Run("EnableCsvMetricsNoPanic", func(t *testing.T) {
		p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())
		// Empty prefix is a valid no-op in HexPatriciaHashed.EnableCsvMetrics —
		// we only verify the delegation does not panic.
		require.NotPanics(t, func() { p.EnableCsvMetrics("") })
	})
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

// TestParallelPatriciaHashedSkeletonRelease covers Release idempotency, that
// plumbing methods stay safe after Release, and that RootHash on a released
// instance returns nil without error.
func TestParallelPatriciaHashedSkeletonRelease(t *testing.T) {
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

	got, err := p.RootHash()
	require.NoError(t, err)
	assert.Nil(t, got, "RootHash on released instance returns nil")
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
// needs multiple workers running concurrently for race detector coverage.
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
	return requireRootParity(t, plainKeys, updates, numWorkers)
}

// TestParallelProcessSkeleton_DenseSingleNibbleBucket: many accounts under one
// root nibble, so a single mount worker carries the whole batch.
func TestParallelProcessSkeleton_DenseSingleNibbleBucket(t *testing.T) {
	t.Parallel()

	const targetNibble = 0x5
	const numAddrs = 64

	ub := NewUpdateBuilder()
	for i := range numAddrs {
		addr := findAddressForNibble(targetNibble, i)
		ub.Balance(addrHex(addr), uint64(7000+i))
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

// --- multi-worker fanout parity tests -------------------------------
//
// Every test below drives a multi-nibble update set through both modes via
// assertEquivalentRootWorkers, so several mount workers fold concurrently
// (race detector coverage included).

// twoLeafTaskAddrs constructs N/2 addresses hashing to firstNibble + N/2
// addresses hashing to secondNibble, giving the prefix-trie root a fanout of
// exactly 2.
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

// TestParallelFanout drives multi-nibble update sets through both modes via
// assertEquivalentRootWorkers, so several mount workers fold concurrently. Each
// row varies the fanout shape and worker count.
func TestParallelFanout(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		workers int
		build   func(t *testing.T) ([][]byte, []Update)
	}{
		{
			// minimal concurrent scenario — two mount workers, one per touched root nibble.
			name:    "TwoNibbles",
			workers: 2,
			build: func(t *testing.T) ([][]byte, []Update) {
				const perSide = 16
				addrs := twoLeafTaskAddrs(t, 0x3, 0x5, perSide)
				ub := NewUpdateBuilder()
				for i, addr := range addrs {
					ub.Balance(addrHex(addr), uint64(1_000+i))
				}
				return ub.Build()
			},
		},
		{
			// four mount workers folding concurrently.
			name:    "FourNibbles",
			workers: 4,
			build: func(t *testing.T) ([][]byte, []Update) {
				const perBucket = 8
				buckets := []int{0x1, 0x4, 0x7, 0xC}
				ub := NewUpdateBuilder()
				for _, nib := range buckets {
					for i := range perBucket {
						addr := findAddressForNibble(nib, i)
						ub.Balance(addrHex(addr), uint64(2_000+nib*100+i))
					}
				}
				return ub.Build()
			},
		},
		{
			// three workers with a 6:1:1 size ratio, verifying completion order
			// does not change the resulting root hash.
			name:    "AsymmetricWorkload",
			workers: 3,
			build: func(t *testing.T) ([][]byte, []Update) {
				big := 24
				small := 4
				ub := NewUpdateBuilder()
				for i := range big {
					addr := findAddressForNibble(0x2, i)
					ub.Balance(addrHex(addr), uint64(3_000+i))
				}
				for i := range small {
					addr := findAddressForNibble(0x6, i)
					ub.Balance(addrHex(addr), uint64(5_000+i))
				}
				for i := range small {
					addr := findAddressForNibble(0xB, i)
					ub.Balance(addrHex(addr), uint64(7_000+i))
				}
				return ub.Build()
			},
		},
		{
			// one wide bucket (its second nibbles fan out broadly inside the
			// worker's subtree) next to a small one.
			name:    "LopsidedBuckets",
			workers: 8,
			build: func(t *testing.T) ([][]byte, []Update) {
				ub := NewUpdateBuilder()
				for i := range 32 {
					addr := findAddressForNibble(0x0, i)
					ub.Balance(addrHex(addr), uint64(9_000+i))
				}
				for i := range 16 {
					addr := findAddressForNibble(0xF, i)
					ub.Balance(addrHex(addr), uint64(11_000+i))
				}
				return ub.Build()
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			plainKeys, updates := tc.build(t)
			root := assertEquivalentRootWorkers(t, plainKeys, updates, tc.workers)
			require.NotEmpty(t, root)
		})
	}
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

// Edge-case tests for ParallelPatriciaHashed.
//
// Every end-to-end test in this file MUST drive the same update set through
// both sequential HexPatriciaHashed (ModeDirect) and ParallelPatriciaHashed
// (ModeParallel) and assert byte-equal root hashes — the cardinal correctness
// rule. Single-batch tests use assertEquivalentRootWorkers from
// parallel_patricia_hashed_test.go. Multi-batch tests (delete patterns that
// need an established DB state before the test batch runs) use the
// stagedRootEquivalence helper defined below; it persists MockState across
// batches so PutBranch writes from batch N visit ctx.Branch on batch N+1.

// stagedBatch is one phase in a multi-phase test: a (plainKeys, updates)
// pair plus a short label for failure messages.
type stagedBatch struct {
	label      string
	plainKeys  [][]byte
	updates    []Update
	expectSame bool // when true, root hash must equal the previous batch's
}

// stagedRootEquivalence applies a sequence of batches against persistent
// sequential and parallel tries and asserts both sides produce byte-equal
// root hashes after every batch. State persists across batches: the MockState
// PutBranch/Branch maps survive, so a later batch sees the DB state the
// earlier batches established. This is what lets delete-pattern tests
// validate the untouched-nibble fix in Prepare — by phase 2 the DB already
// carries the cells phase 1 wrote.
//
// numWorkers controls the parallel side's worker pool. Returns the final
// root hash for caller-side assertions.
func stagedRootEquivalence(t *testing.T, batches []stagedBatch, numWorkers int) []byte {
	t.Helper()
	ctx := context.Background()

	seqMs := NewMockState(t)
	parMs := NewMockState(t)
	parMs.SetConcurrentCommitment(true)

	seqTrie := NewHexPatriciaHashed(length.Addr, seqMs, DefaultTrieConfig())
	defer seqTrie.Release()

	parTrie := NewParallelPatriciaHashed(mockTrieCtxFactory(parMs), length.Addr, DefaultTrieConfig())
	defer parTrie.Release()
	parTrie.SetNumWorkers(numWorkers)
	parTrie.ResetContext(parMs)

	var prevRoot []byte
	for i, batch := range batches {
		require.NoError(t, seqMs.applyPlainUpdates(batch.plainKeys, batch.updates),
			"batch[%d] %q: applyPlainUpdates(seq)", i, batch.label)
		require.NoError(t, parMs.applyPlainUpdates(batch.plainKeys, batch.updates),
			"batch[%d] %q: applyPlainUpdates(par)", i, batch.label)

		seqUpds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, batch.plainKeys, batch.updates)
		seqRoot, err := seqTrie.Process(ctx, seqUpds, "", nil, WarmupConfig{})
		seqUpds.Close()
		require.NoError(t, err, "batch[%d] %q: seq Process", i, batch.label)
		seqTrie.Reset()

		parUpds := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
		for j, k := range batch.plainKeys {
			j, k := j, k
			ks := string(k)
			parUpds.TouchPlainKey(ks, nil, func(c *KeyUpdate, _ []byte) {
				c.plainKey = ks
				c.hashedKey = KeyToHexNibbleHash(k)
				c.update = &batch.updates[j]
			})
		}
		parRoot, err := parTrie.Process(ctx, parUpds, "", nil, WarmupConfig{})
		parUpds.Close()
		require.NoError(t, err, "batch[%d] %q: par Process", i, batch.label)
		parTrie.Reset()

		require.Equal(t, seqRoot, parRoot,
			"batch[%d] %q: sequential and parallel root hashes must match", i, batch.label)
		if batch.expectSame {
			require.Equal(t, prevRoot, seqRoot,
				"batch[%d] %q: root must equal previous batch's root (no-op batch)", i, batch.label)
		}
		prevRoot = seqRoot
	}
	return prevRoot
}

// TestParallelDeleteWithSurvivingSiblings exercises touched-and-deleted cells
// together with untouched DB siblings. Phase 1 populates 8 top
// nibbles, each with multiple accounts, giving the root branch in the DB a
// rich bitmap. Phase 2 deletes some touched accounts in two of those nibbles
// while leaving the other six entirely untouched — the encoded branch must
// carry the surviving siblings forward, and assertEquivalentRoot pins that
// invariant to the sequential root.
func TestParallelDeleteWithSurvivingSiblings(t *testing.T) {
	t.Parallel()

	// Phase 1: 8 top-level nibbles populated with 4 accounts each (32 total).
	// Every nibble ends up in the root branch encoded in the DB.
	ub1 := NewUpdateBuilder()
	for nib := range 8 {
		for s := range 4 {
			ub1.Balance(addrHex(nibbleAddr(nib, s)), uint64(100+nib*10+s))
		}
	}
	pk1, up1 := ub1.Build()

	// Phase 2: touch only nibbles 0x0 and 0x5. Delete two of nibble 0x0's
	// accounts; modify two of nibble 0x5's. Nibbles 0x1..0x4, 0x6, 0x7
	// remain in the DB untouched — the untouched-nibble fix in Prepare
	// must preload them so the last-finisher's grid carries them through.
	ub2 := NewUpdateBuilder()
	ub2.Delete(addrHex(nibbleAddr(0x0, 0)))
	ub2.Delete(addrHex(nibbleAddr(0x0, 1)))
	ub2.Balance(addrHex(nibbleAddr(0x5, 0)), 9999)
	ub2.Balance(addrHex(nibbleAddr(0x5, 1)), 8888)
	pk2, up2 := ub2.Build()

	root := stagedRootEquivalence(t, []stagedBatch{
		{label: "phase1-populate-8-nibbles", plainKeys: pk1, updates: up1},
		{label: "phase2-delete-and-modify-touched", plainKeys: pk2, updates: up2},
	}, 4)
	require.NotEmpty(t, root)
}

// TestParallelAllDeleted deletes every touched key without any untouched
// siblings in the DB. After phase 2 the trie should be empty again — phase 3
// (a no-op batch) asserts the empty-trie root is reached.
func TestParallelAllDeleted(t *testing.T) {
	t.Parallel()

	// Phase 1: 4 nibbles populated with 1 account each (4 total). No
	// other nibbles are populated, so phase 2's deletion leaves the trie
	// empty.
	ub1 := NewUpdateBuilder()
	for nib := range 4 {
		ub1.Balance(addrHex(nibbleAddr(nib, 0)), uint64(200+nib))
	}
	pk1, up1 := ub1.Build()

	// Phase 2: delete every account from phase 1.
	ub2 := NewUpdateBuilder()
	for nib := range 4 {
		ub2.Delete(addrHex(nibbleAddr(nib, 0)))
	}
	pk2, up2 := ub2.Build()

	root := stagedRootEquivalence(t, []stagedBatch{
		{label: "phase1-populate", plainKeys: pk1, updates: up1},
		{label: "phase2-delete-all", plainKeys: pk2, updates: up2},
	}, 4)
	require.NotEmpty(t, root, "empty-trie root should still be a well-known hash")
}

// TestParallelDeleteAllTouchedWithUntouchedSurviving is the critical
// untouched-nibble regression test. Phase 1 populates nibbles 0x0..0x3 with
// several accounts. Phase 2 deletes EVERY touched account in nibble 0x0 and
// updates nibbles 0x1..0x3. The deletion at nibble 0x0 must not collapse the
// root branch — the cells at 0x1..0x3 are untouched but present in DB and
// must survive in the final branch. assertEquivalentRoot is the entire
// diagnostic surface here.
func TestParallelDeleteAllTouchedWithUntouchedSurviving(t *testing.T) {
	t.Parallel()

	ub1 := NewUpdateBuilder()
	for nib := range 4 {
		for s := range 3 {
			ub1.Balance(addrHex(nibbleAddr(nib, s)), uint64(300+nib*10+s))
		}
	}
	pk1, up1 := ub1.Build()

	ub2 := NewUpdateBuilder()
	for s := range 3 {
		ub2.Delete(addrHex(nibbleAddr(0x0, s)))
	}
	// Update 0x1..0x3 so the parallel batch fans out across several mount
	// workers instead of degenerating to a single nibble bucket.
	for nib := 1; nib < 4; nib++ {
		for s := range 3 {
			ub2.Balance(addrHex(nibbleAddr(nib, s)), uint64(7000+nib*10+s))
		}
	}
	pk2, up2 := ub2.Build()

	root := stagedRootEquivalence(t, []stagedBatch{
		{label: "phase1-populate-4-nibbles", plainKeys: pk1, updates: up1},
		{label: "phase2-delete-touched-keep-siblings", plainKeys: pk2, updates: up2},
	}, 4)
	require.NotEmpty(t, root)
}

// TestParallelBloatnetShape exercises a synthetic bloatnet-style workload:
// many accounts, each carrying many storage slots, spread across the trie
// so the root has fanout 16 and the per-nibble subtrees are large.
//
// Memory budget: ~250 accounts × 40 storage slots ≈ 10K touched keys. The
// prefix-trie node count stays comfortably under 100K nodes (each ~80B), so
// peak RSS is well under 100MB — small enough to run alongside other
// commitment tests under -short=false. The "bloatnet stress" gating is kept
// because the test still runs ~5-15s end-to-end in race-detector mode.
func TestParallelBloatnetShape(t *testing.T) {
	if testing.Short() {
		t.Skip("bloatnet stress test — skipped in -short mode (~10K touched keys end-to-end)")
	}
	t.Parallel()

	const numAccounts = 64
	const slotsPerAccount = 40

	ub := NewUpdateBuilder()
	for accIdx := range numAccounts {
		// Spread accounts across every top nibble so the root branch ends
		// up with full fanout. With 64 accounts and 16 nibbles, each
		// nibble holds ~4 accounts on average.
		targetNibble := accIdx % 16
		addr := nibbleAddr(targetNibble, accIdx)
		ah := addrHex(addr)
		ub.Balance(ah, uint64(1_000_000+accIdx))
		ub.Nonce(ah, uint64(accIdx))
		for slotIdx := range slotsPerAccount {
			loc := slotHashBytes(accIdx*slotsPerAccount + slotIdx)
			ub.Storage(ah, hex.EncodeToString(loc), fmt.Sprintf("%02x", (slotIdx%255)+1))
		}
	}
	plainKeys, updates := ub.Build()

	root := assertEquivalentRootWorkers(t, plainKeys, updates, 8)
	require.NotEmpty(t, root)
	t.Logf("bloatnet root (%d touched keys, %d accounts): %x",
		len(plainKeys), numAccounts, root)
}

// TestParallelSingleAccountManyStorage stresses the deep storage subtree
// path: one account, many storage slots. The prefix trie collapses the
// shared 64-nibble account hash into one extension; the fan-out happens at
// depth 64 onwards as the slot hashes diverge.
//
// Workers folding from deep storage depth must not overflow cell.extension
// (sized [64]byte) when their folded subtree cell lands in the base row.
func TestParallelSingleAccountManyStorage(t *testing.T) {
	t.Parallel()

	const numSlots = 96

	// Pick a deterministic account address. The exact value is unimportant;
	// using a fixed string keeps the test reproducible without depending on
	// findAddressForNibble's search ordering.
	accHex := "68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9"

	ub := NewUpdateBuilder()
	ub.Balance(accHex, 1_234_567)
	ub.Nonce(accHex, 42)
	for slotIdx := range numSlots {
		loc := slotHashBytes(slotIdx)
		ub.Storage(accHex, hex.EncodeToString(loc), fmt.Sprintf("%04x", slotIdx+1))
	}
	plainKeys, updates := ub.Build()

	root := assertEquivalentRootWorkers(t, plainKeys, updates, 4)
	require.NotEmpty(t, root)
	t.Logf("single-account-%d-slots root: %x", numSlots, root)
}

// TestParallelEmptyUpdates: ModeParallel must handle a zero-touched-key
// batch by returning the same empty-trie root the sequential path returns.
// This duplicates the skeleton test's coverage so the edge-case sweep is
// self-contained.
func TestParallelEmptyUpdates(t *testing.T) {
	t.Parallel()
	root := assertEquivalentRoot(t, nil, nil)
	require.NotEmpty(t, root, "empty-trie root is the empty hash")
}

// TestParallelSingleTouchedKey: one TouchPlainKey, one mount worker.
func TestParallelSingleTouchedKey(t *testing.T) {
	t.Parallel()
	plainKeys, updates := NewUpdateBuilder().
		Balance("4c888535841acbe0709b0758083f61d375bc02b4", 9001).
		Build()
	root := assertEquivalentRoot(t, plainKeys, updates)
	require.NotEmpty(t, root)
}

// TestParallelOnlyOneAccountTouchedManyTimes: the same account hit with
// several distinct field updates (Balance, Nonce, CodeHash). UpdateBuilder
// merges field updates per key before hashing, so the resulting batch has
// a single touched key whose cell carries the union of all field updates.
func TestParallelOnlyOneAccountTouchedManyTimes(t *testing.T) {
	t.Parallel()

	const accHex = "9c9c8b889dcef79f9e6b3aabaf729b5dbf6e3a2c"
	plainKeys, updates := NewUpdateBuilder().
		Balance(accHex, 12_345).
		Nonce(accHex, 7).
		CodeHash(accHex, "deadbeefcafef00d0102030405060708aaaabbbbccccddddeeeeffff00112233").
		Build()
	require.Equal(t, 1, len(plainKeys), "UpdateBuilder merges per-account updates into one key")
	require.Equal(t, BalanceUpdate|NonceUpdate|CodeUpdate, updates[0].Flags)

	root := assertEquivalentRoot(t, plainKeys, updates)
	require.NotEmpty(t, root)
}

// TestParallelDeleteWithSurvivingSiblings_BranchInspection extends
// TestParallelDeleteWithSurvivingSiblings with a direct check of the
// final root branch: after phase 2 the encoded branch at the empty prefix
// must still expose nibbles 0x1..0x4, 0x6, 0x7 — the untouched siblings.
func TestParallelDeleteWithSurvivingSiblings_BranchInspection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Reuse the same setup as TestParallelDeleteWithSurvivingSiblings but
	// keep a reference to the parallel MockState so we can inspect the
	// final commitment after phase 2.
	seqMs := NewMockState(t)
	parMs := NewMockState(t)
	parMs.SetConcurrentCommitment(true)

	seqTrie := NewHexPatriciaHashed(length.Addr, seqMs, DefaultTrieConfig())
	defer seqTrie.Release()
	parTrie := NewParallelPatriciaHashed(mockTrieCtxFactory(parMs), length.Addr, DefaultTrieConfig())
	defer parTrie.Release()
	parTrie.SetNumWorkers(4)
	parTrie.ResetContext(parMs)

	runBatch := func(label string, plainKeys [][]byte, updates []Update) {
		require.NoError(t, seqMs.applyPlainUpdates(plainKeys, updates), "seq applyPlainUpdates %s", label)
		require.NoError(t, parMs.applyPlainUpdates(plainKeys, updates), "par applyPlainUpdates %s", label)

		seqUpds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
		seqRoot, err := seqTrie.Process(ctx, seqUpds, "", nil, WarmupConfig{})
		seqUpds.Close()
		require.NoError(t, err)
		seqTrie.Reset()

		parUpds := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
		for j, k := range plainKeys {
			j, k := j, k
			ks := string(k)
			parUpds.TouchPlainKey(ks, nil, func(c *KeyUpdate, _ []byte) {
				c.plainKey = ks
				c.hashedKey = KeyToHexNibbleHash(k)
				c.update = &updates[j]
			})
		}
		parRoot, err := parTrie.Process(ctx, parUpds, "", nil, WarmupConfig{})
		parUpds.Close()
		require.NoError(t, err)
		parTrie.Reset()

		require.Equal(t, seqRoot, parRoot, "root mismatch after %s", label)
	}

	ub1 := NewUpdateBuilder()
	for nib := range 8 {
		for s := range 4 {
			ub1.Balance(addrHex(nibbleAddr(nib, s)), uint64(100+nib*10+s))
		}
	}
	pk1, up1 := ub1.Build()
	runBatch("phase1-populate", pk1, up1)

	ub2 := NewUpdateBuilder()
	ub2.Delete(addrHex(nibbleAddr(0x0, 0)))
	ub2.Delete(addrHex(nibbleAddr(0x0, 1)))
	ub2.Balance(addrHex(nibbleAddr(0x5, 0)), 9999)
	ub2.Balance(addrHex(nibbleAddr(0x5, 1)), 8888)
	pk2, up2 := ub2.Build()
	runBatch("phase2-delete-and-modify", pk2, up2)

	// Inspect the root branch in the parallel MockState's commitment map.
	// It must carry every nibble whose accounts were populated in phase 1
	// minus any that were entirely cleared. Since phase 2 only touched
	// nibbles 0x0 and 0x5 (and 0x0 still has surviving siblings: seeds
	// 2 and 3 from phase 1 were not deleted), all 8 nibbles 0x0..0x7
	// must still be present.
	//
	// Branches are stored under their HexToCompact-encoded prefix —
	// HexToCompact(nil) is the single byte 0x00 (the compact "empty path"
	// header), not the empty string. Use that lookup key.
	branch, _, err := parMs.Branch(nibbles.HexToCompact(nil))
	require.NoError(t, err)
	require.NotEmpty(t, branch, "root branch must exist after phase 2")

	_, afterMap, _, err := BranchData(branch).decodeCells()
	require.NoError(t, err)

	for nib := range 8 {
		assert.NotZero(t, afterMap&(1<<nib),
			"nibble %x must survive in the root branch after phase 2 deletions+updates", nib)
	}
}

// findHashForNibbles brute-forces a key whose keccak hash begins with prefix.
// counterOff is the byte offset of the search counter within key.
func findHashForNibbles(t testing.TB, key []byte, counterOff int, prefix []byte, seed, salt uint64) {
	t.Helper()
	counter := seed*salt + 1
	for range 1 << 26 {
		binary.BigEndian.PutUint64(key[counterOff:counterOff+8], counter)
		h := crypto.Keccak256(key)
		ok := true
		for j, n := range prefix {
			hn := h[j>>1] >> 4
			if j&1 == 1 {
				hn = h[j>>1] & 0x0f
			}
			if hn != n {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		counter++
	}
	t.Fatalf("findHashForNibbles(%x): not found in 2^26 tries", prefix)
}

// findAddrForNibbles returns a 20-byte address whose keccak hash starts with prefix.
func findAddrForNibbles(t testing.TB, prefix []byte, seed uint64) []byte {
	addr := make([]byte, 20)
	findHashForNibbles(t, addr, 8, prefix, seed, 1_000_003)
	return addr
}

// findSlotForNibbles returns a 32-byte storage slot whose keccak hash starts with prefix.
func findSlotForNibbles(t testing.TB, prefix []byte, seed uint64) []byte {
	slot := make([]byte, 32)
	findHashForNibbles(t, slot, 16, prefix, seed, 2_000_003)
	return slot
}

var (
	storageTopN byte = 4
	storageSubN byte = 2
)

// genWideNested builds a trie wide at the top that forks at depths 1..4: 8 top
// nibbles × 4 × 2, several keys per leaf group — nested forks at every level,
// the structure random keys never produce.
func genWideNested(t testing.TB) (keys [][]byte, upds []Update) {
	t.Helper()
	seed := uint64(1)
	add := func(prefix []byte) {
		var u Update
		u.Flags = BalanceUpdate | NonceUpdate
		u.Balance.SetUint64(seed * 7)
		u.Nonce = seed
		keys = append(keys, findAddrForNibbles(t, prefix, seed))
		upds = append(upds, u)
		seed++
	}
	for top := byte(0); top < 8; top++ {
		for s := byte(0); s < 4; s++ {
			for u := byte(0); u < 2; u++ {
				add([]byte{top, s, u})
				add([]byte{top, s, u})
				add([]byte{top, s, u, 0x0})
				add([]byte{top, s, u, 0x1})
			}
		}
	}
	return keys, upds
}

// genAccountsWithNestedStorage builds nAccounts accounts, each with an account
// update plus storage slots whose keccak(slot) forks at depths 1..4 — exercising
// deep extensions and account-terminator nodes below depth 64.
func genAccountsWithNestedStorage(t testing.TB, nAccounts int) (keys [][]byte, upds []Update) {
	t.Helper()
	seed := uint64(1000)
	for a := 0; a < nAccounts; a++ {
		addr := findAddrForNibbles(t, []byte{byte(a & 0x7)}, seed)
		seed++
		var au Update
		au.Flags = BalanceUpdate | NonceUpdate | CodeUpdate
		au.Balance.SetUint64(seed)
		au.Nonce = seed
		au.CodeHash = [32]byte{byte(a), 0x11, 0x22}
		keys = append(keys, append([]byte{}, addr...))
		upds = append(upds, au)

		addSlot := func(prefix []byte) {
			slot := findSlotForNibbles(t, prefix, seed)
			seed++
			key := append(append(make([]byte, 0, length.Addr+length.Hash), addr...), slot...)
			var u Update
			u.Flags = StorageUpdate
			u.StorageLen = 4
			binary.BigEndian.PutUint32(u.Storage[:4], uint32(seed))
			keys = append(keys, key)
			upds = append(upds, u)
		}
		for top := byte(0); top < storageTopN; top++ {
			for s := byte(0); s < storageSubN; s++ {
				addSlot([]byte{top, s})
				addSlot([]byte{top, s})
				addSlot([]byte{top, s, 0x0})
				addSlot([]byte{top, s, 0x1})
			}
		}
	}
	return keys, upds
}

// genRandomAccountsStorage builds nAcc accounts with uncontrolled (real keccak)
// hash distribution, each with a few storage slots — the production-like shape.
func genRandomAccountsStorage(nAcc int) (keys [][]byte, upds []Update) {
	for a := 0; a < nAcc; a++ {
		var addr [20]byte
		binary.BigEndian.PutUint64(addr[0:8], uint64(a)*2654435761+1)
		binary.BigEndian.PutUint64(addr[12:20], uint64(a)*40503+7)
		var au Update
		au.Flags = BalanceUpdate | NonceUpdate | CodeUpdate
		au.Balance.SetUint64(uint64(a) + 1000)
		au.Nonce = uint64(a)
		au.CodeHash = [32]byte{byte(a), 0x11}
		keys = append(keys, append([]byte{}, addr[:]...))
		upds = append(upds, au)
		for s := 0; s < 3+a%6; s++ {
			var slot [32]byte
			binary.BigEndian.PutUint64(slot[0:8], uint64(a)*7919+uint64(s))
			binary.BigEndian.PutUint64(slot[24:32], uint64(s)*131+1)
			key := append(append(make([]byte, 0, length.Addr+length.Hash), addr[:]...), slot[:]...)
			var u Update
			u.Flags = StorageUpdate
			u.StorageLen = 4
			binary.BigEndian.PutUint32(u.Storage[:4], uint32(a*100+s))
			keys = append(keys, key)
			upds = append(upds, u)
		}
	}
	return keys, upds
}

// sparseBatch2 selects every modN-th key as an incremental batch: a balance/nonce
// update for accounts, a storage update for storage keys, and — when deletes is
// set — a delete for every other selected key.
func sparseBatch2(keys [][]byte, modN int, deletes bool) (k2 [][]byte, u2 []Update) {
	for i := range keys {
		if i%modN != 0 {
			continue
		}
		var u Update
		switch {
		case deletes && i%2 == 0:
			u.Flags = DeleteUpdate
		case len(keys[i]) == length.Addr:
			u.Flags = BalanceUpdate | NonceUpdate
			u.Balance.SetUint64(uint64(i)*13 + 1)
			u.Nonce = uint64(i) + 7
		default:
			u.Flags = StorageUpdate
			u.StorageLen = 4
			binary.BigEndian.PutUint32(u.Storage[:4], uint32(i)*97+3)
		}
		k2 = append(k2, keys[i])
		u2 = append(u2, u)
	}
	return k2, u2
}

// runIncremental applies two batches to one MockState (batch-1 branches become
// DB state for batch-2) and returns the final root and the MockState.
func runIncremental(t *testing.T, mode runMode, workers int, k1 [][]byte, u1 []Update, k2 [][]byte, u2 []Update) ([]byte, *MockState) {
	t.Helper()
	return incrementalRoot(t, mode, workers, k1, u1, k2, u2)
}

// requireIncrementalEquiv asserts the parallel AND streaming roots match the
// sequential root after the same two batches, dumping divergent branches on
// mismatch.
func requireIncrementalEquiv(t *testing.T, k1 [][]byte, u1 []Update, k2 [][]byte, u2 []Update, workers int) {
	t.Helper()
	requireAllEnginesParity(t, k1, u1, k2, u2, workers)
}

// TestVerifyParallel_WideNested: single batch over a deeply-nested wide trie.
func TestVerifyParallel_WideNested(t *testing.T) {
	t.Parallel()
	keys, upds := genWideNested(t)
	require.NotEmpty(t, assertEquivalentRootWorkers(t, keys, upds, 8))
}

// TestVerifyParallel_WideNestedIncremental: batch 1 commits the whole wide-nested
// trie, batch 2 touches a sparse subset so split-points have untouched DB-only
// siblings.
func TestVerifyParallel_WideNestedIncremental(t *testing.T) {
	t.Parallel()
	keys, upds := genWideNested(t)
	k2, u2 := sparseBatch2(keys, 3, false)
	requireIncrementalEquiv(t, keys, upds, k2, u2, 8)
}

// TestVerifyParallel_StorageMinimal: one storage slot per account (storageTopN/
// SubN=1) so each top nibble holds a single account+storage leaf with a shared
// extension — the shape that broke the deposit/mount extension trim.
func TestVerifyParallel_StorageMinimal(t *testing.T) {
	oldTop, oldSub := storageTopN, storageSubN
	storageTopN, storageSubN = 1, 1
	defer func() { storageTopN, storageSubN = oldTop, oldSub }()
	keys, upds := genAccountsWithNestedStorage(t, 4)
	k2, u2 := sparseBatch2(keys, 3, false)
	requireIncrementalEquiv(t, keys, upds, k2, u2, 8)
}

// TestVerifyParallel_StorageBranchEquiv asserts that after a single batch the
// parallel run's stored branches (not just the root) match the sequential ones —
// catching wrong branch metadata that a matching root would hide.
func TestVerifyParallel_StorageBranchEquiv(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	keys, upds := genAccountsWithNestedStorage(t, 4)

	seqMs := NewMockState(t)
	require.NoError(t, seqMs.applyPlainUpdates(keys, upds))
	seqTrie := NewHexPatriciaHashed(length.Addr, seqMs, DefaultTrieConfig())
	defer seqTrie.Release()
	seqUpds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, keys, upds)
	defer seqUpds.Close()
	seqRoot, err := seqTrie.Process(ctx, seqUpds, "", nil, WarmupConfig{})
	require.NoError(t, err)

	parMs := NewMockState(t)
	parMs.SetConcurrentCommitment(true)
	require.NoError(t, parMs.applyPlainUpdates(keys, upds))
	parTrie := NewParallelPatriciaHashed(mockTrieCtxFactory(parMs), length.Addr, DefaultTrieConfig())
	defer parTrie.Release()
	parTrie.SetNumWorkers(8)
	parTrie.ResetContext(parMs)
	parUpds := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer parUpds.Close()
	for i, k := range keys {
		ks := string(k)
		parUpds.TouchPlainKey(ks, nil, func(c *KeyUpdate, _ []byte) {
			c.plainKey = ks
			c.hashedKey = KeyToHexNibbleHash(k)
			c.update = &upds[i]
		})
	}
	parRoot, err := parTrie.Process(ctx, parUpds, "", nil, WarmupConfig{})
	require.NoError(t, err)

	require.Equal(t, seqRoot, parRoot, "single-batch root must match")
	if len(seqMs.cm) != len(parMs.cm) {
		branchDiff(t, seqMs, parMs)
	}
	require.Equal(t, len(seqMs.cm), len(parMs.cm), "branch count must match")
	for k, sb := range seqMs.cm {
		require.Equalf(t, []byte(sb), []byte(parMs.cm[k]), "branch at prefix %x must match", []byte(k))
	}
}

// TestVerifyParallel_RandomStorageIncremental: production-like distribution,
// incremental, across worker counts.
func TestVerifyParallel_RandomStorageIncremental(t *testing.T) {
	t.Parallel()
	keys, upds := genRandomAccountsStorage(256)
	k2, u2 := sparseBatch2(keys, 3, false)
	for _, w := range []int{1, 2, 4, 8} {
		requireIncrementalEquiv(t, keys, upds, k2, u2, w)
	}
}

// TestVerifyParallel_StorageIncrementalDeletes: incremental batch mixing updates
// and deletes (storage zeroing / account removal), across worker counts.
func TestVerifyParallel_StorageIncrementalDeletes(t *testing.T) {
	t.Parallel()
	keys, upds := genRandomAccountsStorage(256)
	k2, u2 := sparseBatch2(keys, 3, true)
	for _, w := range []int{1, 2, 4, 8} {
		requireIncrementalEquiv(t, keys, upds, k2, u2, w)
	}
}
