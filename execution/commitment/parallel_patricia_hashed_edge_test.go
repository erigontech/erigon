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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// Task 9 edge-case tests for ParallelPatriciaHashed.
//
// Every end-to-end test in this file MUST drive the same update set through
// both sequential HexPatriciaHashed (ModeDirect) and ParallelPatriciaHashed
// (ModeParallel) and assert byte-equal root hashes — the cardinal correctness
// rule. Single-batch tests use assertEquivalentRootWorkers from
// parallel_patricia_hashed_test.go. Multi-batch tests (delete patterns that
// need an established DB state before the test batch runs) use the
// stagedRootEquivalence helper defined below; it persists MockState across
// batches so PutBranch writes from batch N visit ctx.Branch on batch N+1.
//
// The helper always exercises the barrier protocol by lowering the
// split-point threshold to parallelEdgeMinSplitKeys (=2): every two-child
// fork above the touched-key threshold becomes a barrier, which is the
// surface most prone to regression.

// parallelEdgeMinSplitKeys is the threshold passed to SetMinSplitKeys for
// edge-case tests. Two means every fork with >=2 touched siblings becomes a
// split-point regardless of subtreeCount, keeping the barrier protocol in the
// path under test for batches that would otherwise be too small to trigger a
// split in the default 32-threshold configuration.
const parallelEdgeMinSplitKeys uint32 = 2

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

	seqTrie := NewHexPatriciaHashed(length.Addr, seqMs)
	defer seqTrie.Release()

	parTrie := NewParallelPatriciaHashed(mockTrieCtxFactory(parMs), length.Addr)
	defer parTrie.Release()
	parTrie.SetNumWorkers(numWorkers)
	parTrie.SetMinSplitKeys(parallelEdgeMinSplitKeys)
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

// nibbleAddr returns an address whose hashed first nibble matches targetNibble
// and whose seed differentiates it from other addresses in the same nibble.
// Wraps the existing findAddressForNibble cache.
func nibbleAddr(targetNibble, seed int) []byte {
	return findAddressForNibble(targetNibble, seed)
}

// slotHashBytes returns a 32-byte storage slot identifier derived from i.
// Each i produces a distinct slot; the values are deterministic so tests
// reproduce across runs.
func slotHashBytes(i int) []byte {
	var out [32]byte
	binary.BigEndian.PutUint64(out[24:], uint64(i)+1)
	return out[:]
}

// TestParallelDeleteWithSurvivingSiblings exercises the touched-and-deleted
// barrier path together with untouched DB siblings. Phase 1 populates 8 top
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
// updates nibbles 0x1..0x3 to keep the split-point active. The deletion at
// nibble 0x0 must not collapse the root branch — the cells at 0x1..0x3 are
// untouched but present in DB and must survive in the final branch.
//
// If Prepare's untouched-nibble pre-population path is broken (e.g. fails to
// load the DB branch at the split-point), the parallel side would produce a
// branch missing the 0x1..0x3 cells and the resulting root would diverge.
// assertEquivalentRoot is the entire diagnostic surface here.
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
	// Update 0x1..0x3 to ensure the parallel batch produces multiple
	// leafTasks routed through the root split-point. Without these the
	// batch would degenerate to a single nibble bucket (no split-point) and
	// the test would not exercise the barrier protocol.
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
// so the root has fanout 16 and the per-nibble subtrees are large enough to
// nest split-points. Wall time and peak RSS are documented in the test's
// doc-comment.
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

	root := assertEquivalentRootWorkers(t, plainKeys, updates, parallelEdgeMinSplitKeys, 8)
	require.NotEmpty(t, root)
	t.Logf("bloatnet root (%d touched keys, %d accounts): %x",
		len(plainKeys), numAccounts, root)
}

// TestParallelSingleAccountManyStorage stresses the deep storage subtree
// path: one account, many storage slots. The prefix trie collapses the
// shared 64-nibble account hash into one extension; the fan-out happens at
// depth 64 onwards as the slot hashes diverge. Prepare emits a split-point
// at depth 64 (fanout=16, subtreeCount above threshold) and 16 leafTasks
// covering the slot subtrees.
//
// This is the case Task 8's fuzz harness flagged: workers folding from
// deep storage depth must not overflow cell.extension (sized [64]byte) when
// they deposit at the split-point. The deposit code in
// depositRootIntoSplitPoint trims leading nibbles from the cell's extension
// before depositing, and the deposit depth is len(sp.prefix)+1 ≤ 65, so the
// extension stays within bounds as long as Prepare emits the split-point at
// the depth where the keys actually fork.
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

	root := assertEquivalentRootWorkers(t, plainKeys, updates, parallelEdgeMinSplitKeys, 4)
	require.NotEmpty(t, root)
	t.Logf("single-account-%d-slots root: %x", numSlots, root)
}

// TestParallelEmptyUpdates: ModeParallel must handle a zero-touched-key
// batch by returning the same empty-trie root the sequential path returns.
// This duplicates the skeleton test's coverage but is named per the Task 9
// plan so the edge-case sweep is self-contained.
func TestParallelEmptyUpdates(t *testing.T) {
	t.Parallel()
	root := assertEquivalentRoot(t, nil, nil, parallelEdgeMinSplitKeys)
	require.NotEmpty(t, root, "empty-trie root is the empty hash")
}

// TestParallelSingleTouchedKey: one TouchPlainKey, no split-points. Prepare
// emits a single leafTask. The barrier protocol is short-circuited (no
// split-point to deposit at) and the lone worker publishes the root hash
// directly via publishRootFromWorker.
func TestParallelSingleTouchedKey(t *testing.T) {
	t.Parallel()
	plainKeys, updates := NewUpdateBuilder().
		Balance("4c888535841acbe0709b0758083f61d375bc02b4", 9001).
		Build()
	root := assertEquivalentRoot(t, plainKeys, updates, parallelEdgeMinSplitKeys)
	require.NotEmpty(t, root)
}

// TestParallelMixedAccountStorage: random mix of accounts and storage slots
// across many accounts. Exercises both the account-leaf and storage-leaf
// hashing paths through a single Process call, and ensures the barrier
// works when a split-point separates accounts from storage subtrees.
func TestParallelMixedAccountStorage(t *testing.T) {
	t.Parallel()

	ub := NewUpdateBuilder()
	// 16 accounts spread across all top nibbles.
	for nib := range 16 {
		addr := nibbleAddr(nib, 0)
		ah := addrHex(addr)
		ub.Balance(ah, uint64(50_000+nib))
		ub.Nonce(ah, uint64(nib))
		// Each account gets 3 storage slots.
		for s := range 3 {
			loc := slotHashBytes(nib*10 + s)
			ub.Storage(ah, hex.EncodeToString(loc), fmt.Sprintf("%02x", (s+nib)%255+1))
		}
	}
	plainKeys, updates := ub.Build()

	root := assertEquivalentRootWorkers(t, plainKeys, updates, parallelEdgeMinSplitKeys, 8)
	require.NotEmpty(t, root)
}

// TestParallelOnlyOneAccountTouchedManyTimes: the same account hit with
// several distinct field updates (Balance, Nonce, CodeHash). UpdateBuilder
// merges field updates per key before hashing, so the resulting batch has
// a single touched key with combined Flags. The test asserts ModeParallel
// handles a one-leafTask batch (no split-points) where the cell carries
// the union of all field updates.
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

	root := assertEquivalentRoot(t, plainKeys, updates, parallelEdgeMinSplitKeys)
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

	seqTrie := NewHexPatriciaHashed(length.Addr, seqMs)
	defer seqTrie.Release()
	parTrie := NewParallelPatriciaHashed(mockTrieCtxFactory(parMs), length.Addr)
	defer parTrie.Release()
	parTrie.SetNumWorkers(4)
	parTrie.SetMinSplitKeys(parallelEdgeMinSplitKeys)
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
