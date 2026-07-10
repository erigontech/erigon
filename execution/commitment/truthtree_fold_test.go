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
	"encoding/hex"
	"fmt"
	"math/bits"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// whaleStorageNode builds the touched prefix trie for the whale corpus and returns the depth-64
// account node (whose children are the storage subtree) and the 64-nibble account prefix. All
// keys share the account hash, so the account node is a single walk down.
func whaleStorageNode(pk [][]byte, upds []Update, accHash []byte) (*prefixNode, []byte) {
	tr := newPrefixTrie()
	for i, k := range pk {
		tr.Insert(KeyToHexNibbleHash(k), k, &upds[i])
	}
	node := tr.root
	depth := 0
	for {
		depth += len(node.ext)
		if depth >= 64 {
			break
		}
		nib := accHash[depth]
		idx, ok := childIndex(node, nib)
		if !ok {
			panic(fmt.Sprintf("whaleStorageNode: account path missing at depth %d", depth))
		}
		depth++ // consume the branch nibble
		node = node.children[idx]
	}
	if depth != 64 {
		panic(fmt.Sprintf("whaleStorageNode: account node at depth %d, want 64", depth))
	}
	return node, append([]byte(nil), accHash[:64]...)
}

func makeFoldPool(ms *MockState, workers int) *foldPool {
	return &foldPool{
		numWorkers: workers,
		ctxFactory: mockTrieCtxFactory(ms),
		workerPool: &sync.Pool{New: func() any { return NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig()) }},
	}
}

// oracleRoot computes the reference account state root via the sequential ModeDirect engine.
func oracleRoot(tb testing.TB, pk [][]byte, upds []Update) []byte {
	ms := NewMockState(tb)
	require.NoError(tb, ms.applyPlainUpdates(pk, upds))
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	u := WrapKeyUpdates(tb, ModeDirect, KeyToHexNibbleHash, pk, upds)
	root, err := hph.Process(context.Background(), u, "", nil, WarmupConfig{})
	require.NoError(tb, err)
	u.Close()
	hph.Release()
	return root
}

// wrapAccountRoot builds the single-account state root from a storage-subtree root hash by hashing
// the account leaf at depth 0 (its full 64-nibble key is re-derived from the address).
func wrapAccountRoot(ms *MockState, addr []byte, accUpd Update, sr common.Hash) ([]byte, error) {
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer hph.Release()
	var ac cell
	ac.accountAddrLen = int16(len(addr))
	copy(ac.accountAddr[:], addr)
	ac.CodeHash = empty.CodeHash
	ac.setFromUpdate(&accUpd)
	ac.hash = sr
	ac.hashLen = 32
	h, err := hph.computeCellHash(&ac, 0, nil)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), h[1:]...), nil
}

// TestTruthtreeFold_FreshStoragePlane pins the direct fold's storage root byte-for-byte against
// both the current fresh-whale fold path and the sequential oracle, on fresh storage subtrees.
func TestTruthtreeFold_FreshStoragePlane(t *testing.T) {
	for _, slots := range []int{5_000, 50_000} {
		t.Run(fmt.Sprintf("slots=%d", slots), func(t *testing.T) {
			addr, accHash, _, accUpd, pk, upds, _ := whaleByNibble(slots)
			ctx := context.Background()

			oracle := oracleRoot(t, pk, upds)

			ms := NewMockState(t)
			require.NoError(t, ms.applyPlainUpdates(pk, upds))
			node, accPrefix := whaleStorageNode(pk, upds, accHash)

			fp := makeFoldPool(ms, runtime.NumCPU())
			srCur, _, err := fp.foldFreshStorage(ctx, node, accPrefix)
			require.NoError(t, err)

			sr, err := foldFreshStorageRoot(node)
			require.NoError(t, err)
			require.Equal(t, srCur, sr, "direct fold storage root != current fresh-whale fold")

			root, err := wrapAccountRoot(ms, addr, accUpd, sr)
			require.NoError(t, err)
			require.Equal(t, oracle, root, "direct fold account root != sequential oracle")
		})
	}
}

// seqFreshBranchOracle processes the fresh whale through the sequential engine and returns the
// resulting branch store; for a single-account whale every stored branch is a storage branch.
func seqFreshBranchOracle(t *testing.T, pk [][]byte, upds []Update) *MockState {
	t.Helper()
	ms := NewMockState(t)
	require.NoError(t, ms.applyPlainUpdates(pk, upds))
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer hph.Release()
	u := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk, upds)
	defer u.Close()
	_, err := hph.Process(context.Background(), u, "", nil, WarmupConfig{})
	require.NoError(t, err)
	return ms
}

// TestTruthtreeFold_FreshDeferredEmission pins that the direct fresh fold emits a branch record per
// storage branch prefix byte-for-byte identical to the sequential engine: applying the fold's
// deferred updates to an empty branch store must reproduce the oracle store prefix-for-prefix.
func TestTruthtreeFold_FreshDeferredEmission(t *testing.T) {
	for _, slots := range []int{5_000, 50_000} {
		t.Run(fmt.Sprintf("slots=%d", slots), func(t *testing.T) {
			_, accHash, _, _, pk, upds, _ := whaleByNibble(slots)

			oracleMs := seqFreshBranchOracle(t, pk, upds)

			node, accPrefix := whaleStorageNode(pk, upds, accHash)
			sr, deferred, err := foldFreshStorageRootDeferred(node, accPrefix)
			require.NoError(t, err)
			require.NotEmpty(t, deferred, "fresh fold must emit branch records")

			got := NewMockState(t)
			_, err = ApplyDeferredBranchUpdates(deferred, 1, got.PutBranch)
			require.NoError(t, err)
			requireBranchParity(t, oracleMs, got)

			pureSR, err := foldFreshStorageRoot(node)
			require.NoError(t, err)
			require.Equal(t, pureSR, sr, "deferred fold storage root != pure fold storage root")
		})
	}
}

// TestTruthtreeFold_FreshDeferredFailClosed pins fail-closed emission: a subtree whose first
// storage nibble folds and emits a branch record, followed by a malformed sibling that errors
// mid-fold, must return no deferred updates at all — a partial write is never surfaced.
func TestTruthtreeFold_FreshDeferredFailClosed(t *testing.T) {
	_, accHash, _, _, _, _, groups := whaleByNibble(200)
	x, z := -1, -1
	for nib := range 16 {
		if x == -1 {
			if len(groups[nib]) >= 2 {
				x = nib
			}
			continue
		}
		if len(groups[nib]) >= 1 {
			z = nib
			break
		}
	}
	require.GreaterOrEqual(t, x, 0, "corpus needs a multi-slot first-nibble group")
	require.Greater(t, z, x, "corpus needs a second populated first-nibble group above it")

	var pk2 [][]byte
	var u2 []Update
	for _, kv := range groups[x] {
		pk2 = append(pk2, kv.pk)
		u2 = append(u2, kv.upd)
	}
	for _, kv := range groups[z] {
		pk2 = append(pk2, kv.pk)
		u2 = append(u2, kv.upd)
	}
	node, accPrefix := whaleStorageNode(pk2, u2, accHash)
	require.Len(t, node.children, 2)
	node.children[1] = &prefixNode{} // malformed leaf: bitmap 0, nil plainKey

	_, deferred, err := foldFreshStorageRootDeferred(node, accPrefix)
	require.Error(t, err)
	require.Nil(t, deferred, "error must drop every collected deferred update")
}

// seqStorageDisk seeds one storage block on disk (branch records into ms) via the sequential
// engine and returns the resulting root.
func seqStorageDisk(t *testing.T, ms *MockState, pk [][]byte, upds []Update) []byte {
	t.Helper()
	require.NoError(t, ms.applyPlainUpdates(pk, upds))
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer hph.Release()
	u := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk, upds)
	defer u.Close()
	root, err := hph.Process(context.Background(), u, "", nil, WarmupConfig{})
	require.NoError(t, err)
	return common.Copy(root)
}

// seqTwoBlockOracle folds block 1 then block 2 through the sequential engine on one state, so
// block 1's branches are the on-disk state block 2 reconciles against, and returns block 2's root
// plus the resulting branch store. For a single-account whale every stored branch is a storage
// branch, so the store doubles as the branch-parity oracle.
func seqTwoBlockOracle(t *testing.T, pk [][]byte, upds []Update, k2 [][]byte, u2 []Update) ([]byte, *MockState) {
	t.Helper()
	ctx := context.Background()
	ms := NewMockState(t)
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer hph.Release()

	require.NoError(t, ms.applyPlainUpdates(pk, upds))
	u1 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, pk, upds)
	_, err := hph.Process(ctx, u1, "", nil, WarmupConfig{})
	require.NoError(t, err)
	u1.Close()

	require.NoError(t, ms.applyPlainUpdates(k2, u2))
	u2w := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, k2, u2)
	root, err := hph.Process(ctx, u2w, "", nil, WarmupConfig{})
	require.NoError(t, err)
	u2w.Close()
	return common.Copy(root), ms
}

// requireReconciledFoldParity seeds block 1 on disk, then folds block 2's touched storage subtree
// against that disk via foldReconciledStorageRoot and asserts (1) the wrapped account root equals
// the sequential oracle and (2) the branch records the fold emits — the deletes it writes in place
// plus the deferred branch updates it returns — reproduce the oracle's branch store byte-for-byte.
// block 2 leaves the account untouched, so accUpd holds the on-disk account fields and the account
// leaf rehashes over the reconciled storage root.
func requireReconciledFoldParity(t *testing.T, addr, accHash []byte, accUpd Update, pk [][]byte, upds []Update, k2 [][]byte, u2 []Update) {
	t.Helper()
	ctx := context.Background()

	oracle, oracleMs := seqTwoBlockOracle(t, pk, upds, k2, u2)

	msFold := NewMockState(t)
	seqStorageDisk(t, msFold, pk, upds)
	require.NoError(t, msFold.applyPlainUpdates(k2, u2))

	node2, accPrefix := whaleStorageNode(k2, u2, accHash)
	fp := makeFoldPool(msFold, runtime.NumCPU())

	sr, deferred, err := foldReconciledStorageRoot(fp, ctx, node2, accPrefix)
	require.NoError(t, err)
	root, err := wrapAccountRoot(msFold, addr, accUpd, sr)
	require.NoError(t, err)
	require.Equal(t, oracle, root, "reconciled fold root != sequential oracle")

	_, err = ApplyDeferredBranchUpdates(deferred, 1, msFold.PutBranch)
	require.NoError(t, err)
	requireBranchParity(t, oracleMs, msFold)
}

// TestTruthtreeFold_OnDiskSiblingReconciliation is the net-new crux: block 2 touches a scattered
// subset of block 1's on-disk storage, so most of the subtree is untouched on-disk siblings the
// direct fold never sees in the touched trie. The hand-rolled fresh fold drops them and diverges
// (the RED baseline); the reconciling fold reads the on-disk branches and reproduces the root.
func TestTruthtreeFold_OnDiskSiblingReconciliation(t *testing.T) {
	addr, accHash, _, accUpd, pk, upds, groups := whaleByNibble(20_000)

	var k2 [][]byte
	var u2 []Update
	for x := range 16 {
		for i, kv := range groups[x] {
			if i%7 != 0 {
				continue
			}
			nu := kv.upd
			nu.Storage[len(nu.Storage)-1] ^= 0x5A
			k2 = append(k2, kv.pk)
			u2 = append(u2, nu)
		}
	}

	oracle, _ := seqTwoBlockOracle(t, pk, upds, k2, u2)

	// RED baseline: without reconciliation the fresh fold sees only the touched subset and drops
	// every untouched on-disk sibling, so its root cannot match the oracle.
	node2, _ := whaleStorageNode(k2, u2, accHash)
	freshSR, err := foldFreshStorageRoot(node2)
	require.NoError(t, err)
	freshRoot, err := wrapAccountRoot(NewMockState(t), addr, accUpd, freshSR)
	require.NoError(t, err)
	require.NotEqual(t, oracle, freshRoot, "precondition: fresh fold must drop on-disk siblings and diverge")

	requireReconciledFoldParity(t, addr, accHash, accUpd, pk, upds, k2, u2)
}

// TestTruthtreeFold_SingleSurvivorCollapse deletes every storage slot but one whole first-nibble
// group, so the reconciled storage subtree collapses to a single surviving first-nibble child — the
// depth-64 seam must emit the extension-node root over the survivor, not a branch hash.
func TestTruthtreeFold_SingleSurvivorCollapse(t *testing.T) {
	addr, accHash, _, accUpd, pk, upds, groups := whaleByNibble(20_000)
	surv := -1
	for x := range 16 {
		if len(groups[x]) >= 2 {
			surv = x
			break
		}
	}
	require.GreaterOrEqual(t, surv, 0, "corpus must have a multi-slot first-nibble group")

	var k2 [][]byte
	var u2 []Update
	for x := range 16 {
		if x == surv {
			continue
		}
		for _, kv := range groups[x] {
			k2 = append(k2, kv.pk)
			u2 = append(u2, Update{Flags: DeleteUpdate})
		}
	}
	requireReconciledFoldParity(t, addr, accHash, accUpd, pk, upds, k2, u2)
}

// TestTruthtreeFold_DeleteToEmpty deletes every storage slot, so the reconciled fold must collect
// the delete of the storage-root branch and return the empty-storage root.
func TestTruthtreeFold_DeleteToEmpty(t *testing.T) {
	addr, accHash, _, accUpd, pk, upds, groups := whaleByNibble(2_000)
	var k2 [][]byte
	var u2 []Update
	for x := range 16 {
		for _, kv := range groups[x] {
			k2 = append(k2, kv.pk)
			u2 = append(u2, Update{Flags: DeleteUpdate})
		}
	}
	requireReconciledFoldParity(t, addr, accHash, accUpd, pk, upds, k2, u2)
}

// truthtreeFoldAllocCeiling caps the direct fold's per-op allocation on the 750k fresh-whale
// storage subtree. Buffer reuse keeps it near the proto's ~44 MB serial figure; the naive
// per-node-cell fold the proto rejected sits at ~575 MB (~331 MB for the current copy-replay fold).
// The ceiling sits well above the former and far below the latter, so it catches a buffer-reuse
// regression without pinning an exact byte count.
const truthtreeFoldAllocCeiling = 96 << 20

func freshWhaleFoldNode(tb testing.TB, slots int) *prefixNode {
	tb.Helper()
	_, accHash, _, _, pk, upds, _ := whaleByNibble(slots)
	node, _ := whaleStorageNode(pk, upds, accHash)
	return node
}

func foldFreshWhale(b *testing.B, node *prefixNode) {
	b.ReportAllocs()
	var sink common.Hash
	for b.Loop() {
		h, err := foldFreshStorageRoot(node)
		if err != nil {
			b.Fatal(err)
		}
		sink = h
	}
	runtime.KeepAlive(sink)
}

func Benchmark_TruthtreeFold_FreshWhaleAlloc(b *testing.B) {
	foldFreshWhale(b, freshWhaleFoldNode(b, 750_000))
}

// TestTruthtreeFold_AllocCeiling is the buffer-reuse definition-of-done gate: the direct fold of the
// 750k fresh-whale storage subtree must stay near the proto's ~44 MB figure and never regress toward
// the ~575 MB naive per-node-cell fold.
func TestTruthtreeFold_AllocCeiling(t *testing.T) {
	node := freshWhaleFoldNode(t, 750_000)
	res := testing.Benchmark(func(b *testing.B) { foldFreshWhale(b, node) })
	require.NotZero(t, res.N, "alloc-ceiling bench did not run")
	got := res.AllocedBytesPerOp()
	t.Logf("truthtree fold 750k fresh-whale: %.1f MB/op, %d allocs/op", float64(got)/(1<<20), res.AllocsPerOp())
	require.Lessf(t, got, int64(truthtreeFoldAllocCeiling),
		"fold alloc %.1f MB/op exceeds %.0f MB ceiling — buffer-reuse regression toward the ~575 MB naive fold",
		float64(got)/(1<<20), float64(truthtreeFoldAllocCeiling)/(1<<20))
}

// freshAccountTrie builds the touched prefix trie for a fresh account corpus and returns its root.
func freshAccountTrie(pk [][]byte, upds []Update) *prefixNode {
	tr := newPrefixTrie()
	for i, k := range pk {
		tr.Insert(KeyToHexNibbleHash(k), k, &upds[i])
	}
	return tr.root
}

// branchDepths counts the stored branch records by their nibble-path depth, so a test can assert a
// corpus actually exercises account branches (depth < 64), the storage-root seam (depth 64), and
// interior storage branches (depth > 64).
func branchDepths(ms *MockState) map[int]int {
	out := make(map[int]int)
	for k := range ms.cm {
		out[len(nibbles.CompactToHex([]byte(k)))]++
	}
	return out
}

func requireSpansAllPlanes(t *testing.T, ms *MockState) {
	t.Helper()
	depths := branchDepths(ms)
	require.Positive(t, depths[2], "corpus must have account branches at depth 2")
	require.Positive(t, depths[64], "corpus must have storage-root branches at depth 64 (multi-nibble storage)")
	require.Positive(t, depths[65], "corpus must have interior storage branches at depth 65")
}

// TestTruthtreeFold_AccountPlaneFresh folds a fresh mixed account/storage corpus directly through
// the account-plane recursion — recursing account branches at mixed depths and crossing every
// depth-64 storage seam inline — and pins its state root and every emitted branch record
// byte-for-byte against the sequential oracle at depths {2, 64, 65}.
func TestTruthtreeFold_AccountPlaneFresh(t *testing.T) {
	for _, n := range []int{2_000, 20_000} {
		t.Run(fmt.Sprintf("keys=%d", n), func(t *testing.T) {
			pk, upds := buildMixedCorpus(int64(n), n)
			root := freshAccountTrie(pk, upds)
			require.Empty(t, root.ext, "a large mixed corpus root branches at depth 0")

			oracle := oracleRoot(t, pk, upds)
			oracleMs := seqFreshBranchOracle(t, pk, upds)
			requireSpansAllPlanes(t, oracleMs)

			sr, deferred, err := foldFreshAccountRootDeferred(root)
			require.NoError(t, err)
			require.Equal(t, oracle, sr[:], "direct account fold state root != sequential oracle")

			got := NewMockState(t)
			_, err = ApplyDeferredBranchUpdates(deferred, 1, got.PutBranch)
			require.NoError(t, err)
			requireBranchParity(t, oracleMs, got)

			pure, err := foldFreshAccountRoot(root)
			require.NoError(t, err)
			require.Equal(t, sr, pure, "deferred account fold root != pure account fold root")
		})
	}
}

// TestTruthtreeFold_AccountPlaneMountWall folds each top-nibble account subtree into its mount-wall
// cell via foldFreshAccountSubtreeCellDeferred (invariant M: stripCellToMountWall over the subtree
// prefix), stitches those cells into the finale root wall, and folds to the state root — asserting
// the leaf-task cells are parent-stitchable by reproducing the oracle root and branch store.
func TestTruthtreeFold_AccountPlaneMountWall(t *testing.T) {
	ctx := context.Background()
	const n = 20_000
	pk, upds := buildMixedCorpus(7_007, n)
	root := freshAccountTrie(pk, upds)
	require.Empty(t, root.ext, "a large mixed corpus root branches at depth 0")

	oracle := oracleRoot(t, pk, upds)
	oracleMs := seqFreshBranchOracle(t, pk, upds)
	requireSpansAllPlanes(t, oracleMs)

	ms := NewMockState(t)
	require.NoError(t, ms.applyPlainUpdates(pk, upds))
	fp := makeFoldPool(ms, 1)
	base, release := newDeferredStorageWorker(fp.workerPool, fp.ctxFactory, nil)
	defer release()
	require.NoError(t, unfoldRootWall(ctx, base))
	seedRootBase(base)

	var (
		cells    [16]cell
		present  [16]bool
		deferred []*DeferredBranchUpdate
	)
	childIdx := 0
	for bm := root.bitmap; bm != 0; {
		nib := bits.TrailingZeros16(bm)
		child := root.children[childIdx]
		require.NotZero(t, child.bitmap, "top-nibble subtree must be a branch for a 20k corpus")
		c, d, err := foldFreshAccountSubtreeCellDeferred(child, root.ext, nib)
		require.NoError(t, err)
		cells[nib] = c
		present[nib] = true
		deferred = append(deferred, d...)
		childIdx++
		bm &^= uint16(1) << nib
	}
	stitchSplitCells(base, &cells, &present)
	if base.activeRows == 0 {
		base.activeRows = 1
	}
	for base.activeRows > 0 {
		require.NoError(t, base.fold())
	}
	deferred = append(deferred, base.TakeDeferredUpdates()...)

	gotRoot, err := base.RootHash()
	require.NoError(t, err)
	require.Equal(t, oracle, gotRoot, "mount-wall stitched root != sequential oracle")

	got := NewMockState(t)
	_, err = ApplyDeferredBranchUpdates(deferred, 1, got.PutBranch)
	require.NoError(t, err)
	requireBranchParity(t, oracleMs, got)
}

func TestTruthtreeFold_ErrorPaths(t *testing.T) {
	t.Run("nil subtree", func(t *testing.T) {
		h, err := foldFreshStorageRoot(nil)
		require.NoError(t, err)
		require.Equal(t, empty.RootHash, h)
	})
	t.Run("childless subtree", func(t *testing.T) {
		h, err := foldFreshStorageRoot(&prefixNode{})
		require.NoError(t, err)
		require.Equal(t, empty.RootHash, h)
	})
	t.Run("malformed leaf without plainKey", func(t *testing.T) {
		leaf := &prefixNode{} // bitmap 0, plainKey nil -> a leaf that terminates no key
		parent := &prefixNode{bitmap: uint16(1) << 3, children: []*prefixNode{leaf}}
		_, err := foldFreshStorageRoot(parent)
		require.Error(t, err)
	})
}

func truthtreeFoldCfg() TrieConfig {
	cfg := DefaultTrieConfig()
	cfg.TruthtreeFold = true
	return cfg
}

// processParallelBatchCfg folds one batch through the parallel engine built with cfg, carrying the
// EncodeCurrentState→SetState blob across batches like processModeBatchState, and returns the root
// plus the new state blob. It is the flag-parameterized sibling of processModeBatchState's parallel
// case, letting one corpus fold flag-on and flag-off through the same restart lifecycle.
func processParallelBatchCfg(t *testing.T, ms *MockState, cfg TrieConfig, workers int, keys [][]byte, upds []Update, blob []byte) ([]byte, []byte) {
	t.Helper()
	require.NoError(t, ms.applyPlainUpdates(keys, upds))
	tr := NewParallelPatriciaHashed(mockTrieCtxFactory(ms), length.Addr, cfg)
	tr.SetNumWorkers(workers)
	tr.ResetContext(ms)
	defer tr.Release()
	require.NoError(t, tr.RootTrie().SetState(blob))
	ut := NewUpdates(ModeParallel, t.TempDir(), KeyToHexNibbleHash)
	defer ut.Close()
	for i, k := range keys {
		ks := string(k)
		ut.TouchPlainKey(ks, nil, func(c *KeyUpdate, _ []byte) {
			c.plainKey = ks
			c.hashedKey = KeyToHexNibbleHash(k)
			c.update = &upds[i]
		})
	}
	root := processRoot(t, tr, ut)
	out, err := tr.RootTrie().EncodeCurrentState(nil)
	require.NoError(t, err)
	return root, out
}

// requireFlagLeafParity folds one batch stream through the sequential oracle, the flag-off parallel
// engine, and the flag-on parallel engine in lockstep, asserting root AND stored-branch byte parity
// after every batch. It returns the number of leaf tasks the flag-on run folded through the direct
// recursion, so the caller can prove the truthtree path actually executed. Each engine carries its
// own state blob across the per-batch encode/restore restart.
func requireFlagLeafParity(t *testing.T, workers int, batches []engineBatch) int64 {
	t.Helper()
	require.GreaterOrEqualf(t, len(batches), 3, "flag parity harness expects N>=3 batches, got %d", len(batches))

	seqMs := NewMockState(t)
	offMs := NewMockState(t)
	offMs.SetConcurrentCommitment(true)
	onMs := NewMockState(t)
	onMs.SetConcurrentCommitment(true)
	onCfg := truthtreeFoldCfg()

	before := directLeafFolds.Load()
	var seqBlob, offBlob, onBlob []byte
	for i, b := range batches {
		var seqRoot, offRoot, onRoot []byte
		seqRoot, seqBlob = processModeBatchState(t, seqMs, modeSeq, 0, b.keys, b.upds, seqBlob)
		offRoot, offBlob = processParallelBatchCfg(t, offMs, DefaultTrieConfig(), workers, b.keys, b.upds, offBlob)
		onRoot, onBlob = processParallelBatchCfg(t, onMs, onCfg, workers, b.keys, b.upds, onBlob)

		require.Equalf(t, seqRoot, offRoot, "flag-off parallel batch %d root != sequential", i+1)
		if !bytes.Equal(seqRoot, onRoot) {
			branchDiff(t, seqMs, onMs)
		}
		require.Equalf(t, seqRoot, onRoot, "flag-on parallel batch %d root != sequential", i+1)
		requireBranchParity(t, seqMs, offMs)
		requireBranchParity(t, seqMs, onMs)
	}
	return directLeafFolds.Load() - before
}

// TestTruthtreeFold_LeafFlagParity is the Task 6 gate: flag-on == flag-off == sequential, root and
// stored-branch byte-for-byte, over an N>=3 batch chain on the account-plane (balanced), whale, and
// incremental (re-touch) corpora, through the encode/restore restart. The balanced corpus must route
// at least one leaf through the direct recursion, or the flag-on arm never exercised foldNode.
func TestTruthtreeFold_LeafFlagParity(t *testing.T) {
	direct := requireFlagLeafParity(t, 4, balancedBatches())
	require.Greaterf(t, direct, int64(0),
		"flag-on run folded no leaf through the direct recursion — the balanced corpus no longer covers foldNode")

	requireFlagLeafParity(t, 4, megaWhaleBatches(20_000))
	requireFlagLeafParity(t, 8, megaWhaleBatches(20_000))
}

// freshDeleteBatches builds, in one fresh batch, two multi-account branch subtrees: nibble 7 holds
// only clean accounts (folded via the direct recursion), while nibble 3 holds clean accounts plus one
// whose only storage slot is set then deleted in the same batch. That slot resolves to a delete the
// fresh direct fold would wrongly hash, so nibble 3 must fall back to replay. Two re-touch batches
// follow for the N>=3 chain.
func freshDeleteBatches() []engineBatch {
	slot := hex.EncodeToString(slotHashBytes(1))
	clean := make([]string, 0, 7)

	ub := NewUpdateBuilder()
	addClean := func(nib, s int) {
		a := addrHex(findAddressForNibble(nib, 700+nib*8+s))
		clean = append(clean, a)
		ub.Balance(a, uint64(1000+nib*4+s))
		ub.Storage(a, hex.EncodeToString(slotHashBytes(100+nib*4+s)), slotValHex(100+nib*4+s))
	}
	for s := range 3 {
		addClean(7, s) // nibble 7: all clean -> direct fold
		addClean(3, s) // nibble 3: clean siblings of the deleter -> branch leaf
	}
	deleter := addrHex(findAddressForNibble(3, 42))
	ub.Balance(deleter, 5000)
	ub.Storage(deleter, slot, slotValHex(7))
	ub.DeleteStorage(deleter, slot)
	k1, u1 := ub.Build()

	retouch := func(bal uint64) engineBatch {
		rb := NewUpdateBuilder()
		for i, a := range clean {
			rb.Balance(a, bal+uint64(i))
		}
		rb.Balance(deleter, bal+100)
		k, u := rb.Build()
		return engineBatch{k, u}
	}

	return []engineBatch{{k1, u1}, retouch(20000), retouch(30000)}
}

// TestTruthtreeFold_FreshDeleteFallback pins that a fresh account whose only slot is set-then-deleted
// in the same batch stays byte-parity-clean flag-on: the direct fold detects the resolved delete and
// falls back to replay, which drops the empty leaf. Both the direct fold (nibble 3) and the fallback
// (nibble 5) must fire, or the corpus stopped covering the guard.
func TestTruthtreeFold_FreshDeleteFallback(t *testing.T) {
	folds := directLeafFolds.Load()
	fallbacks := directLeafFallbacks.Load()
	requireFlagLeafParity(t, 4, freshDeleteBatches())
	require.Greater(t, directLeafFolds.Load(), folds, "direct fold never ran — corpus no longer covers foldNode")
	require.Greater(t, directLeafFallbacks.Load(), fallbacks, "delete fallback never ran — corpus no longer covers the guard")
}
