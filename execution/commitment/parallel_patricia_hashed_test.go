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
	"bytes"
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
	assert.Equal(t, VariantHexPatriciaTrie, ParseTrieVariant("hex"))
	assert.Equal(t, VariantHexPatriciaTrie, ParseTrieVariant("nonsense"))
}

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

	t.Run("SetTraceWriter", func(t *testing.T) {
		p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())

		var buf bytes.Buffer
		p.SetTraceWriter(&buf)
		assert.NotNil(t, p.template.traceW, "trace writer fanned out to template")
		p.SetTraceWriter(nil)
		assert.Nil(t, p.template.traceW, "nil writer disables tracing on template")
	})

	t.Run("EnableCsvMetricsNoPanic", func(t *testing.T) {
		p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())
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

func TestParallelPatriciaHashedSkeletonRelease(t *testing.T) {
	p := NewParallelPatriciaHashed(nil, length.Addr, DefaultTrieConfig())
	require.NotNil(t, p.template)

	p.Release()
	assert.Nil(t, p.template, "Release drops the template")

	require.NotPanics(t, func() { p.Release() })

	require.NotPanics(t, func() {
		var buf bytes.Buffer
		p.SetTraceWriter(&buf)
		p.SetTraceWriter(nil)
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

	seq := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer seq.Release()
	expected, err := seq.RootHash()
	require.NoError(t, err)

	assert.Equal(t, expected, got, "RootHash falls back to template for the no-updates path")
}

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

	root := requireRootParity(t, plainKeys, updates, 1)
	require.NotEmpty(t, root)
}

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

	upds := WrapKeyUpdates(t, ModeParallel, KeyToHexNibbleHash, plainKeys, updates)
	defer upds.Close()

	_, err := p.Process(context.Background(), upds, "", nil, WarmupConfig{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "TrieContextFactory")
}

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

func TestDFSSubtree(t *testing.T) {
	t.Parallel()

	pu := newParallelUpdate()
	pu.Insert(nibs(0x01, 0x02, 0x03), []byte("pk-A"), nil)
	pu.Insert(nibs(0x01, 0x02, 0x04), []byte("pk-B"), nil)
	pu.Insert(nibs(0x05, 0x06, 0x07), []byte("pk-C"), nil)
	pu.Insert(nibs(0x01, 0x02), []byte("pk-D"), nil) // prefix of A and B

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

func TestDFSSubtree_NilPlainKeyLeafErrors(t *testing.T) {
	t.Parallel()

	pu := newParallelUpdate()
	pu.Insert(nibs(0x01, 0x02, 0x03), nil, nil)
	err := dfsSubtree(pu.trie.root, nil, func(_, _ []byte, _ *Update) error { return nil })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plainKey")
}

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

func TestParallelFanout(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		workers int
		build   func(t *testing.T) ([][]byte, []Update)
	}{
		{
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
			root := requireRootParity(t, plainKeys, updates, tc.workers)
			require.NotEmpty(t, root)
		})
	}
}

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

	parUpds := WrapKeyUpdates(t, ModeParallel, KeyToHexNibbleHash, plainKeys, updates)
	defer parUpds.Close()

	published, err := p.Process(context.Background(), parUpds, "", nil, WarmupConfig{})
	require.NoError(t, err)
	require.NotEmpty(t, published)

	// drop the atomic publish so RootHash falls back to the template's root cell
	p.rootHash.Store(nil)

	got, err := p.RootHash()
	require.NoError(t, err)
	require.Equal(t, published, got,
		"template.RootHash must return the published root once the worker has folded into it")
}

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

	parUpds := WrapKeyUpdates(t, ModeParallel, KeyToHexNibbleHash, plainKeys, updates)
	defer parUpds.Close()

	published, err := p.Process(context.Background(), parUpds, "", nil, WarmupConfig{})
	require.NoError(t, err)
	require.NotEmpty(t, published)

	tmpl := p.RootTrie()
	require.True(t, tmpl.rootChecked, "template.rootChecked must be promoted from the worker")
	require.True(t, tmpl.rootTouched, "template.rootTouched must be promoted from the worker")
	require.True(t, tmpl.rootPresent, "template.rootPresent must be promoted from the worker")

	encoded, err := tmpl.EncodeCurrentState(nil)
	require.NoError(t, err)
	require.NotEmpty(t, encoded, "EncodeCurrentState must capture template state mirrored from the worker")

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

type stagedBatch struct {
	label      string
	plainKeys  [][]byte
	updates    []Update
	expectSame bool // root must equal the previous batch's root
}

// one MockState per mode persists across batches, so a later batch sees the DB state earlier batches established
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

		parUpds := WrapKeyUpdates(t, ModeParallel, KeyToHexNibbleHash, batch.plainKeys, batch.updates)
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

func TestParallelAllDeleted(t *testing.T) {
	t.Parallel()

	ub1 := NewUpdateBuilder()
	for nib := range 4 {
		ub1.Balance(addrHex(nibbleAddr(nib, 0)), uint64(200+nib))
	}
	pk1, up1 := ub1.Build()

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

func TestParallelBloatnetShape(t *testing.T) {
	if testing.Short() {
		t.Skip("bloatnet stress test — skipped in -short mode (~10K touched keys end-to-end)")
	}
	t.Parallel()

	const numAccounts = 64
	const slotsPerAccount = 40

	ub := NewUpdateBuilder()
	for accIdx := range numAccounts {
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

	root := requireRootParity(t, plainKeys, updates, 8)
	require.NotEmpty(t, root)
	t.Logf("bloatnet root (%d touched keys, %d accounts): %x",
		len(plainKeys), numAccounts, root)
}

// guards against workers folding from deep storage depth overflowing the fixed-size cell.extension
func TestParallelSingleAccountManyStorage(t *testing.T) {
	t.Parallel()

	const numSlots = 96

	accHex := "68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9"

	ub := NewUpdateBuilder()
	ub.Balance(accHex, 1_234_567)
	ub.Nonce(accHex, 42)
	for slotIdx := range numSlots {
		loc := slotHashBytes(slotIdx)
		ub.Storage(accHex, hex.EncodeToString(loc), fmt.Sprintf("%04x", slotIdx+1))
	}
	plainKeys, updates := ub.Build()

	root := requireRootParity(t, plainKeys, updates, 4)
	require.NotEmpty(t, root)
	t.Logf("single-account-%d-slots root: %x", numSlots, root)
}

func TestParallelEmptyUpdates(t *testing.T) {
	t.Parallel()
	root := requireRootParity(t, nil, nil, 1)
	require.NotEmpty(t, root, "empty-trie root is the empty hash")
}

func TestParallelSingleTouchedKey(t *testing.T) {
	t.Parallel()
	plainKeys, updates := NewUpdateBuilder().
		Balance("4c888535841acbe0709b0758083f61d375bc02b4", 9001).
		Build()
	root := requireRootParity(t, plainKeys, updates, 1)
	require.NotEmpty(t, root)
}

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

	root := requireRootParity(t, plainKeys, updates, 1)
	require.NotEmpty(t, root)
}

func TestParallelDeleteWithSurvivingSiblings_BranchInspection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

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

		parUpds := WrapKeyUpdates(t, ModeParallel, KeyToHexNibbleHash, plainKeys, updates)
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

	// root branch key is HexToCompact(nil) (byte 0x00), not the empty string
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

// counterOff is the byte offset of the search counter within key
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

func findAddrForNibbles(t testing.TB, prefix []byte, seed uint64) []byte {
	addr := make([]byte, 20)
	findHashForNibbles(t, addr, 8, prefix, seed, 1_000_003)
	return addr
}

func findSlotForNibbles(t testing.TB, prefix []byte, seed uint64) []byte {
	slot := make([]byte, 32)
	findHashForNibbles(t, slot, 16, prefix, seed, 2_000_003)
	return slot
}

var (
	storageTopN byte = 4
	storageSubN byte = 2
)

// deterministic nested forks at depths 1..4 that random keccak keys never produce
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
	for top := range byte(8) {
		for s := range byte(4) {
			for u := range byte(2) {
				add([]byte{top, s, u})
				add([]byte{top, s, u})
				add([]byte{top, s, u, 0x0})
				add([]byte{top, s, u, 0x1})
			}
		}
	}
	return keys, upds
}

// storage slots fork at depths 1..4 to exercise deep extensions and account-terminator nodes below depth 64
func genAccountsWithNestedStorage(t testing.TB, nAccounts int) (keys [][]byte, upds []Update) {
	t.Helper()
	seed := uint64(1000)
	for a := range nAccounts {
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

// uncontrolled keccak distribution, the production-like counterpart to the forked generators
func genRandomAccountsStorage(nAcc int) (keys [][]byte, upds []Update) {
	for a := range nAcc {
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

// one MockState across both batches: batch-1 branches become DB state for batch-2
func runIncremental(t *testing.T, mode runMode, workers int, k1 [][]byte, u1 []Update, k2 [][]byte, u2 []Update) ([]byte, *MockState) {
	t.Helper()
	return incrementalRoot(t, mode, workers, k1, u1, k2, u2)
}

func requireIncrementalEquiv(t *testing.T, k1 [][]byte, u1 []Update, k2 [][]byte, u2 []Update, workers int) {
	t.Helper()
	requireAllEnginesParity(t, k1, u1, k2, u2, workers)
}

func TestVerifyParallel_WideNested(t *testing.T) {
	t.Parallel()
	keys, upds := genWideNested(t)
	require.NotEmpty(t, requireRootParity(t, keys, upds, 8))
}

func TestVerifyParallel_WideNestedIncremental(t *testing.T) {
	t.Parallel()
	keys, upds := genWideNested(t)
	k2, u2 := sparseBatch2(keys, 3, false)
	requireIncrementalEquiv(t, keys, upds, k2, u2, 8)
}

// one storage slot per account, so each top nibble holds a single account+storage leaf sharing one extension
func TestVerifyParallel_StorageMinimal(t *testing.T) {
	oldTop, oldSub := storageTopN, storageSubN
	storageTopN, storageSubN = 1, 1
	defer func() { storageTopN, storageSubN = oldTop, oldSub }()
	keys, upds := genAccountsWithNestedStorage(t, 4)
	k2, u2 := sparseBatch2(keys, 3, false)
	requireIncrementalEquiv(t, keys, upds, k2, u2, 8)
}

// compares stored branches, not just the root, catching wrong branch metadata a matching root would hide
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
	parUpds := WrapKeyUpdates(t, ModeParallel, KeyToHexNibbleHash, keys, upds)
	defer parUpds.Close()
	parRoot, err := parTrie.Process(ctx, parUpds, "", nil, WarmupConfig{})
	require.NoError(t, err)

	require.Equal(t, seqRoot, parRoot, "single-batch root must match")
	requireBranchParity(t, seqMs, parMs)
}

func TestVerifyParallel_RandomStorageIncremental(t *testing.T) {
	t.Parallel()
	keys, upds := genRandomAccountsStorage(256)
	k2, u2 := sparseBatch2(keys, 3, false)
	for _, w := range []int{1, 2, 4, 8} {
		requireIncrementalEquiv(t, keys, upds, k2, u2, w)
	}
}

func TestVerifyParallel_StorageIncrementalDeletes(t *testing.T) {
	t.Parallel()
	keys, upds := genRandomAccountsStorage(256)
	k2, u2 := sparseBatch2(keys, 3, true)
	for _, w := range []int{1, 2, 4, 8} {
		requireIncrementalEquiv(t, keys, upds, k2, u2, w)
	}
}
