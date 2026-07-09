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
	"encoding/hex"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// These tests pin the generalized fold primitives — seedBaseAtPrefix, foldMountedLeaf,
// mergeChildrenAtPrefix (with stripCellToMountWall), and the depth-64 storage seam — before the
// frontier pool (Task 4) wires them into Process. A serial recursive folder drives them over a
// deriveFoldDAG tree and the produced root + stored branches are compared byte-for-byte against
// the sequential trie, so mount-boundary invariant M (a task at prefix P under nibble n returns
// exactly the cell the sequential trie leaves in slot (P,n)) is enforced at depths {2, 64, 65}.

// primFolder folds a deriveFoldDAG tree serially through the real primitives, accumulating the
// deferred branch updates each task produces so the caller can apply them after the whole fold.
type primFolder struct {
	t       *testing.T
	factory TrieContextFactory
}

func (pf *primFolder) newWorker() *HexPatriciaHashed {
	w := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())
	wctx, _ := pf.factory()
	w.ResetContext(wctx)
	w.SetTraceWriter(nil)
	w.branchEncoder.setDeferUpdates(true)
	w.SetLeaveDeferredForCaller(true)
	return w
}

// newBase builds the root base the top-nibble tasks stitch into: unfolded one level at the
// on-disk root branch, then walled at the top nibble — exactly StreamingCommitter.buildBase.
func (pf *primFolder) newBase(ctx context.Context) *HexPatriciaHashed {
	base := pf.newWorker()
	require.NoError(pf.t, unfoldRootWall(ctx, base))
	seedRootBase(base)
	return base
}

func (pf *primFolder) take(w *HexPatriciaHashed, into *[]*DeferredBranchUpdate) {
	if d := w.TakeDeferredUpdates(); len(d) > 0 {
		*into = append(*into, d...)
	}
}

// foldChild folds a non-root task against its parent's seeded base and returns the mount-wall-
// relative cell the parent stitches at task.nib.
func (pf *primFolder) foldChild(ctx context.Context, parentBase *HexPatriciaHashed, task *foldTask, deferred *[]*DeferredBranchUpdate) cell {
	switch task.kind {
	case foldLeaf:
		w := pf.newWorker()
		w.mountTo(parentBase, task.nib)
		if task.storage != nil {
			sr := pf.foldStorageSubtree(ctx, task.storage, deferred)
			if task.node.plainKey != nil {
				require.NoError(pf.t, w.followAndUpdate(task.prefix, task.node.plainKey, task.node.update))
			}
			setAccountStorageRoot(w, task.prefix, sr)
		} else {
			for _, kk := range collectSubtreeKeys(task.node, append([]byte(nil), task.prefix...)) {
				require.NoError(pf.t, w.followAndUpdate(kk.hk, kk.pk, kk.upd))
			}
		}
		c, err := w.foldMounted(ctx, task.nib)
		require.NoError(pf.t, err)
		pf.take(w, deferred)
		return c
	case foldMerge:
		base := pf.newWorker()
		require.NoError(pf.t, seedBaseAtPrefix(base, task.prefix))
		var children [16]cell
		for _, child := range task.children {
			children[child.nib] = pf.foldChild(ctx, base, child, deferred)
		}
		mountWall := int16(len(task.parent.prefix)) + 1
		c, err := mergeChildrenAtPrefix(base, &children, task.node.bitmap, mountWall)
		require.NoError(pf.t, err)
		pf.take(base, deferred)
		return c
	}
	pf.t.Fatalf("foldChild: unknown task kind %v", task.kind)
	return cell{}
}

// foldStorageSubtree folds an account's storage-root subtask and returns the injected root hash;
// the depth-64 seam collapses the storage row to a bare hash via aggregateMountedStorageRoot,
// while its interior storage merges (depth >= 65) go through the general mergeChildrenAtPrefix.
func (pf *primFolder) foldStorageSubtree(ctx context.Context, task *foldTask, deferred *[]*DeferredBranchUpdate) common.Hash {
	base := pf.newWorker()
	require.NoError(pf.t, seedBaseAtPrefix(base, task.prefix))
	var children [16]cell
	for _, child := range task.children {
		children[child.nib] = pf.foldChild(ctx, base, child, deferred)
	}
	sr, err := aggregateMountedStorageRoot(base, &children, task.node.bitmap)
	require.NoError(pf.t, err)
	pf.take(base, deferred)
	if sr.IsEmpty() {
		return empty.RootHash
	}
	return sr.hash
}

// foldTrie folds a whole DAG whose root is a top-nibble branch, stitching each top-nibble task's
// cell into the base row and folding to the root, then returns the root hash and every deferred
// branch update produced.
func (pf *primFolder) foldTrie(ctx context.Context, base *HexPatriciaHashed, dag *foldTask) ([]byte, []*DeferredBranchUpdate) {
	require.Equal(pf.t, foldMerge, dag.kind, "foldTrie expects a top-nibble root branch")
	var (
		cells    [16]cell
		present  [16]bool
		deferred []*DeferredBranchUpdate
	)
	for _, child := range dag.children {
		cells[child.nib] = pf.foldChild(ctx, base, child, &deferred)
		present[child.nib] = true
	}
	stitchSplitCells(base, &cells, &present)
	if base.activeRows == 0 {
		base.activeRows = 1
	}
	for base.activeRows > 0 {
		require.NoError(pf.t, base.fold())
	}
	pf.take(base, &deferred)
	root, err := base.RootHash()
	require.NoError(pf.t, err)
	return root, deferred
}

// seedableOf returns a seedable prober reading ms's current branch store — the exact evidence
// deriveFoldDAG uses to decide seed-vs-demote at each merge candidate.
func seedableOf(t *testing.T, ms *MockState) func(prefix []byte) bool {
	return func(prefix []byte) bool {
		b, _, err := ms.Branch(nibbles.HexToCompact(prefix))
		require.NoError(t, err)
		return len(b) > 0
	}
}

// foldBatchViaPrimitives builds the prefix trie for one batch already applied to ms, derives the
// fold DAG at k, folds it through the primitives, applies the deferred branch updates, and returns
// the root. inspect (optional) asserts the derived DAG shape before folding.
func foldBatchViaPrimitives(t *testing.T, ms *MockState, keys [][]byte, upds []Update, k uint32, inspect func(*testing.T, *foldTask)) []byte {
	t.Helper()
	ctx := context.Background()

	tr := newPrefixTrie()
	for i, key := range keys {
		tr.Insert(KeyToHexNibbleHash(key), key, &upds[i])
	}
	dag := deriveFoldDAG(tr.root, k, seedableOf(t, ms))
	require.NotNil(t, dag)
	if inspect != nil {
		inspect(t, dag)
	}

	pf := &primFolder{t: t, factory: mockTrieCtxFactory(ms)}
	base := pf.newBase(ctx)
	root, deferred := pf.foldTrie(ctx, base, dag)
	require.NoError(t, applyDeferredGuarded(ms, deferred, 1))
	return root
}

// mountBoundaryParity processes batch1 sequentially into both an oracle and a candidate store
// (seeding identical on-disk branches), then folds batch2 sequentially (oracle) versus through
// the primitives (candidate) and asserts root + stored-branch byte parity.
func mountBoundaryParity(t *testing.T, batch1, batch2 engineBatch, k uint32, inspect func(*testing.T, *foldTask)) {
	t.Helper()

	seqMs := NewMockState(t)
	seqTrie := NewHexPatriciaHashed(length.Addr, seqMs, DefaultTrieConfig())
	defer seqTrie.Release()
	processBatch(t, seqMs, seqTrie, batch1.keys, batch1.upds)
	seqRoot := processBatch(t, seqMs, seqTrie, batch2.keys, batch2.upds)

	candMs := NewMockState(t)
	candTrie := NewHexPatriciaHashed(length.Addr, candMs, DefaultTrieConfig())
	processBatch(t, candMs, candTrie, batch1.keys, batch1.upds)
	candTrie.Release()
	require.NoError(t, candMs.applyPlainUpdates(batch2.keys, batch2.upds))

	candRoot := foldBatchViaPrimitives(t, candMs, batch2.keys, batch2.upds, k, inspect)

	require.Equal(t, seqRoot, candRoot, "primitive-folded root diverged from sequential")
	require.Empty(t, branchStoreMismatches(seqMs, candMs), "primitive-folded branch store diverged from sequential")
}

// depth2Corpus seeds NN accounts sharing hashed prefix [5,7] (distinct third nibbles, a real
// depth-2 branch) plus one account under each spread top nibble so the trie root is a top-nibble
// branch. Returns batch1 and the [5,7] accounts / spread accounts for batch-2 construction.
func depth2Corpus(nn int) (batch1 engineBatch, forked []string, spread []string, forkedPrefix []byte) {
	forkedPrefix = nibs(5, 7)
	ub := NewUpdateBuilder()
	for i := range nn {
		a := addrHex(findAddressForHexPrefix(nibs(5, 7, byte(i)), 700+i))
		forked = append(forked, a)
		ub.Balance(a, uint64(1000+i))
	}
	for _, nib := range []int{1, 9, 0xc} {
		a := addrHex(findAddressForNibble(nib, 3300+nib))
		spread = append(spread, a)
		ub.Balance(a, uint64(4000+nib))
	}
	k1, u1 := ub.Build()
	return engineBatch{k1, u1}, forked, spread, forkedPrefix
}

// TestFoldPrimitives_MountBoundaryDepth2 folds an account-plane merge at depth 2 (the [5,7]
// branch) whose children are single-account leaves, exercising mergeChildrenAtPrefix +
// stripCellToMountWall in the account plane.
func TestFoldPrimitives_MountBoundaryDepth2(t *testing.T) {
	t.Parallel()
	batch1, forked, spread, forkedPrefix := depth2Corpus(6)

	ub := NewUpdateBuilder()
	for i, a := range forked {
		ub.Balance(a, uint64(50000+i))
	}
	for i, a := range spread {
		ub.Balance(a, uint64(60000+i))
	}
	k2, u2 := ub.Build()

	mountBoundaryParity(t, batch1, engineBatch{k2, u2}, 4, func(t *testing.T, dag *foldTask) {
		m := findByPrefix(dag, forkedPrefix)
		require.NotNil(t, m, "a merge task must exist at the depth-2 prefix [5,7]")
		require.Equal(t, foldMerge, m.kind, "the depth-2 [5,7] subtree must fold as a merge")
		require.Equal(t, planeAccount, m.plane)
		require.Len(t, m.prefix, 2)
	})
}

// TestFoldPrimitives_SingleSurvivorCollapseDepth2 deletes all but one account under the depth-2
// [5,7] branch so the merge folds through the single-survivor propagate path (branch record
// deleted, survivor fused upward) rather than the branch path.
func TestFoldPrimitives_SingleSurvivorCollapseDepth2(t *testing.T) {
	t.Parallel()
	batch1, forked, spread, forkedPrefix := depth2Corpus(6)

	ub := NewUpdateBuilder()
	for _, a := range forked[:len(forked)-1] {
		ub.Delete(a)
	}
	ub.Balance(forked[len(forked)-1], 999999) // lone survivor
	for i, a := range spread {
		ub.Balance(a, uint64(70000+i))
	}
	k2, u2 := ub.Build()

	mountBoundaryParity(t, batch1, engineBatch{k2, u2}, 4, func(t *testing.T, dag *foldTask) {
		m := findByPrefix(dag, forkedPrefix)
		require.NotNil(t, m)
		require.Equal(t, foldMerge, m.kind, "the collapsing [5,7] subtree still derives as a merge (subtreeCount > k)")
	})
}

// genStorageSlotFirstNibble brute-forces a storage slot whose first storage nibble (nibble 64 of
// the hashed key) equals want, so a whale's storage can be steered under chosen first-nibbles.
func genStorageSlotFirstNibble(rnd *rand.Rand, addr []byte, want byte) (locHex, valHex string) {
	for {
		loc := make([]byte, length.Hash)
		rnd.Read(loc)
		pk := make([]byte, 0, length.Addr+length.Hash)
		pk = append(pk, addr...)
		pk = append(pk, loc...)
		if KeyToHexNibbleHash(pk)[64] == want {
			val := make([]byte, 32)
			rnd.Read(val)
			return hex.EncodeToString(loc), hex.EncodeToString(val)
		}
	}
}

// whaleStorageCorpus builds a whale under top nibble 0xd whose storage spans two first-storage-
// nibbles {3,7} — so an on-disk branch exists at the depth-64 account prefix, and nibble 3 carries
// enough slots to branch again at depth 65. Spread accounts keep the trie root a real branch.
// Returns batch1, the whale address, its 64-nibble account prefix, and the nibble-3 slot locs.
func whaleStorageCorpus(seed int64, nib3Slots, nib7Slots int) (batch1 engineBatch, whale string, accPrefix []byte, nib3Locs []string) {
	rnd := rand.New(rand.NewSource(seed))
	waddr := findAddressForNibble(0xd, int(seed))
	whale = addrHex(waddr)
	accPrefix = KeyToHexNibbleHash(waddr)

	ub := NewUpdateBuilder()
	ub.Balance(whale, 12345)
	for range nib3Slots {
		l, v := genStorageSlotFirstNibble(rnd, waddr, 3)
		nib3Locs = append(nib3Locs, l)
		ub.Storage(whale, l, v)
	}
	for range nib7Slots {
		l, v := genStorageSlotFirstNibble(rnd, waddr, 7)
		ub.Storage(whale, l, v)
	}
	for _, nib := range []int{2, 6, 0xa} {
		ub.Balance(addrHex(findAddressForNibble(nib, 5100+nib)), uint64(8000+nib))
	}
	k1, u1 := ub.Build()
	return engineBatch{k1, u1}, whale, accPrefix, nib3Locs
}

// TestFoldPrimitives_MountBoundaryDepth64 folds a whale account at the depth-64 account/storage
// seam: the account leaf depends on a storage-root subtask whose collapsed root hash is injected
// via setAccountStorageRoot (aggregateMountedStorageRoot), exercised alongside the account fold.
func TestFoldPrimitives_MountBoundaryDepth64(t *testing.T) {
	t.Parallel()
	batch1, whale, accPrefix, nib3Locs := whaleStorageCorpus(64064, 40, 8)

	rnd := rand.New(rand.NewSource(90001))
	ub := NewUpdateBuilder()
	ub.Balance(whale, 55555)
	for _, l := range nib3Locs {
		val := make([]byte, 32)
		rnd.Read(val)
		ub.Storage(whale, l, hex.EncodeToString(val))
	}
	for _, nib := range []int{2, 6, 0xa} {
		ub.Balance(addrHex(findAddressForNibble(nib, 5100+nib)), uint64(90000+nib))
	}
	k2, u2 := ub.Build()

	mountBoundaryParity(t, batch1, engineBatch{k2, u2}, 8, func(t *testing.T, dag *foldTask) {
		acct := findByPrefix(dag, accPrefix)
		require.NotNil(t, acct, "the whale account leaf must exist at the depth-64 prefix")
		require.Equal(t, foldLeaf, acct.kind)
		require.Equal(t, planeAccount, acct.plane)
		require.Len(t, acct.prefix, 64)
		require.NotNil(t, acct.storage, "the depth-64 account leaf must carry a storage-root subtask")
		require.Equal(t, foldMerge, acct.storage.kind)
		require.Equal(t, planeStorage, acct.storage.plane)
		require.Len(t, acct.storage.prefix, 64)
	})
}

// TestFoldPrimitives_MountBoundaryDepth65 folds a whale whose nibble-3 storage branches again at
// depth 65, so the storage subtask contains an interior storage merge at the 65-nibble prefix —
// mergeChildrenAtPrefix must return a mount-wall-relative cell in the storage plane, not a
// storage-root hash (which is the depth-64 seam's job only).
func TestFoldPrimitives_MountBoundaryDepth65(t *testing.T) {
	t.Parallel()
	batch1, whale, accPrefix, nib3Locs := whaleStorageCorpus(65065, 40, 8)
	require.Greater(t, len(nib3Locs), 8, "nibble-3 storage must be a real depth-65 branch")

	rnd := rand.New(rand.NewSource(91001))
	ub := NewUpdateBuilder()
	ub.Balance(whale, 66666)
	for _, l := range nib3Locs {
		val := make([]byte, 32)
		rnd.Read(val)
		ub.Storage(whale, l, hex.EncodeToString(val))
	}
	for _, nib := range []int{2, 6, 0xa} {
		ub.Balance(addrHex(findAddressForNibble(nib, 5100+nib)), uint64(95000+nib))
	}
	k2, u2 := ub.Build()

	merge65 := append(append([]byte(nil), accPrefix...), 3)
	mountBoundaryParity(t, batch1, engineBatch{k2, u2}, 8, func(t *testing.T, dag *foldTask) {
		acct := findByPrefix(dag, accPrefix)
		require.NotNil(t, acct)
		require.NotNil(t, acct.storage)
		m := findByPrefix(acct.storage, merge65)
		require.NotNil(t, m, "an interior storage merge must exist at the depth-65 prefix")
		require.Equal(t, foldMerge, m.kind)
		require.Equal(t, planeStorage, m.plane)
		require.Len(t, m.prefix, 65)
	})
}

// TestFoldPrimitives_SeedOrDemote drives the seed-or-demote decision over both evidence paths on a
// real branch store: an on-disk branch prefix seeds (a merge), an absent one demotes (a leaf), and
// seedBaseAtPrefix returns the hard errStorageBaseNotBranch that a mis-derived merge would fail on.
func TestFoldPrimitives_SeedOrDemote(t *testing.T) {
	t.Parallel()
	batch1, _, _, forkedPrefix := depth2Corpus(6)

	ms := NewMockState(t)
	trie := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer trie.Release()
	processBatch(t, ms, trie, batch1.keys, batch1.upds)

	newProbe := func() *HexPatriciaHashed {
		w := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
		return w
	}

	// Present branch: seed succeeds -> the prefix hosts a merge.
	seeded, err := seedOrDemote(newProbe(), forkedPrefix)
	require.NoError(t, err)
	require.True(t, seeded, "a present on-disk branch must seed (merge), never demote and drop siblings")

	// Absent branch: false-empty must demote, not seed an empty (sibling-dropping) wall.
	absent := nibs(0xf, 0xf, 0xf)
	seeded, err = seedOrDemote(newProbe(), absent)
	require.NoError(t, err)
	require.False(t, seeded, "an absent branch must demote to the ancestor's serial leaf")

	// The primitive itself returns the hard error a merge fold fails closed on rather than guessing.
	err = seedBaseAtPrefix(newProbe(), absent)
	require.ErrorIs(t, err, errStorageBaseNotBranch)
}
