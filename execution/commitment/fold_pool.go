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
	"errors"
	"fmt"
	"io"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

// maxFoldConcurrency caps the fresh-whale storage fan-out at the CPUs the process may run on, so
// the account-leaf worker that spawns it plus every other concurrently-folding storage subtree
// stays within the core count.
func maxFoldConcurrency() int { return max(1, runtime.GOMAXPROCS(0)) }

// foldPool folds a fold DAG with one shared worker pool. Leaf tasks mount on their parent
// merge's seeded base and fold their key group; merge tasks stitch their already-folded child
// cells and fold to the mount wall; an account-leaf task waits on its storage-root subtask.
// A worker never blocks holding a slot while waiting on a child — completion decrements the
// parent's pending counter and the pool schedules the parent — so the single pool is the whole
// concurrency budget and the dispatch is deadlock-free by construction.
type foldPool struct {
	numWorkers int
	ctxFactory TrieContextFactory
	workerPool *sync.Pool
	traceW     io.Writer
	// foldSem bounds the nested fresh-whale storage fan-out at GOMAXPROCS across all account
	// leaves folding at once, so the leaf workers spawning it never oversubscribe the cores.
	foldSem *semaphore.Weighted
	// truthtreeFold routes provably-fresh leaf subtrees through the direct foldNode recursion
	// instead of mount+replay. Set from TrieConfig only on the parallel regime's pool; the
	// streaming pool leaves it false so streaming keeps the current fold.
	truthtreeFold bool
}

// deriveFoldFrontier builds the fold DAG for one Process: the root task is always the serial
// finale over the caller's pre-built root wall (never a demotable leaf), with one task per
// touched top nibble derived by the shared K policy. It mirrors deriveFoldDAG's per-child
// derivation but keeps the root a merge so the finale stitch path is uniform for small and
// large tries. Returns nil for an empty trie; the caller rejects a non-empty root extension.
func deriveFoldFrontier(root *prefixNode, k uint32, seedable func([]byte) bool) *foldTask {
	if root == nil || root.subtreeCount == 0 {
		return nil
	}
	b := &foldDAGBuilder{k: k, seedable: seedable}
	prefix := append([]byte(nil), root.ext...)
	rootTask := b.newTask(prefix, root, foldMerge, planeFor(len(prefix)))
	b.addChildren(rootTask, root, prefix)
	return rootTask
}

// collectFoldTasks appends every descendant of root (children and storage subtrees), i.e. every
// task the pool folds — root itself, the finale, is excluded.
func collectFoldTasks(root *foldTask, out []*foldTask) []*foldTask {
	for _, c := range root.children {
		out = append(out, c)
		out = collectFoldTasks(c, out)
	}
	if root.storage != nil {
		out = append(out, root.storage)
		out = collectFoldTasks(root.storage, out)
	}
	return out
}

// run seeds every merge base, folds the whole DAG below rootTask with the shared worker pool,
// leaving each top-level task's mount-wall cell in task.cell, and returns the deferred branch
// updates every task produced. rootTask itself (the finale over rootTask.base) is not folded.
// Any error recycles every collected deferred update and returns nothing — fail-closed, so a
// partial fold never leaks a plausible-but-wrong branch to the caller.
func (fp *foldPool) run(ctx context.Context, rootTask *foldTask) ([]*DeferredBranchUpdate, error) {
	subTasks := collectFoldTasks(rootTask, nil)
	if len(subTasks) == 0 {
		return nil, nil
	}

	// A merge base holds an mmap-pinned context from seed to fold; clean up any base a folding
	// merge did not release (the error path leaves unfolded merges with their base still set).
	defer func() {
		for _, t := range subTasks {
			if t.baseCleanup != nil {
				t.baseCleanup()
				t.baseCleanup = nil
			}
		}
	}()

	for _, t := range subTasks {
		if t.kind != foldMerge {
			continue
		}
		if err := fp.seedMerge(t); err != nil {
			recycleTaskDeferred(subTasks)
			return nil, err
		}
	}

	// Merging each task's deferred right after its fold overlaps the raw+prev merge
	// with the remaining folds, so the caller's flush is a pure write pass.
	foldAndMerge := func(ctx context.Context, t *foldTask) error {
		if err := fp.foldOne(ctx, t); err != nil {
			return err
		}
		return MergeDeferredBranchUpdates(t.deferred, 1)
	}
	if err := dispatchFoldTasks(ctx, fp.numWorkers, rootTask, subTasks, foldAndMerge); err != nil {
		recycleTaskDeferred(subTasks)
		return nil, err
	}

	var deferred []*DeferredBranchUpdate
	for _, t := range subTasks {
		if len(t.deferred) > 0 {
			deferred = append(deferred, t.deferred...)
			t.deferred = nil
		}
	}
	return deferred, nil
}

// forkWholeFresh gates the whole-fresh account-plane fork-join route. It is a dev-only package
// toggle — set it false to measure the frontier-serial baseline in a bench — and is never wired to
// a CLI flag. Default on.
var forkWholeFresh atomic.Bool

// wholeFreshFolds counts Process calls routed through the whole-fresh account-plane fork
// (dispatchWholeFresh). Read by tests to confirm the whole-fresh path fired on a fresh build and
// stayed dormant on an incremental one.
var wholeFreshFolds atomic.Int64

// wholeFreshForkJoins counts whole-fresh Process calls that took the account-plane fork-join rather
// than falling back to the frontier fold (a non-pure-branch top nibble routes to the fallback). Read
// by tests to confirm the fork actually ran on an all-branch fresh corpus.
var wholeFreshForkJoins atomic.Int64

// wholeFreshCellFolds counts top-nibble subtrees folded through the account-plane fork-join.
var wholeFreshCellFolds atomic.Int64

// onNibFold, when non-nil, receives the wall time of each top-nibble subtree fold in the serial
// dispatch loop — a measurement hook for the fresh-fork critical-path study. nil in production.
var onNibFold func(nib int, subtreeCount uint32, d time.Duration)

func init() { forkWholeFresh.Store(true) }

// wholeFreshBuild reports whether a Process runs against provably empty on-disk state: no seedable
// branch probed anywhere during derivation (a propagate-folded root's deeper branches are seedable,
// so sawSeedable excludes it) AND an empty unfolded root wall. Probe absence alone cannot prove
// emptiness — a root collapsed to a single leaf or extension has no branch record at the root prefix
// either, yet its cell is carried on base's wall and must be folded, not overwritten. Only a real
// multi-way root branch qualifies (no root extension, ≥2 children), the shape the account-plane fork
// reproduces. Gated on the dev-only forkWholeFresh toggle.
func wholeFreshBuild(base *HexPatriciaHashed, root *prefixNode, sawSeedable bool) bool {
	if !forkWholeFresh.Load() {
		return false
	}
	if sawSeedable || !rootWallEmpty(base) {
		return false
	}
	return len(root.ext) == 0 && bits.OnesCount16(root.bitmap) >= 2
}

// rootWallEmpty reports whether base's unfolded row-0 wall carries no on-disk state: an empty root
// cell, no child cells, and no root branch record.
func rootWallEmpty(base *HexPatriciaHashed) bool {
	return base != nil && base.activeRows > 0 && base.root.IsEmpty() &&
		base.afterMap[0] == 0 && !base.branchBefore[0]
}

// dispatchFrontier derives the fold DAG over root, folds it with fp's shared worker pool, stitches
// the resulting top-nibble cells into base, and folds base down to its root, returning the deferred
// branch updates the whole fold produced. reuse, when non-nil, is the streaming scheduler's
// cached-cell consumer: it prunes clean top nibbles from the DAG (their pre-folded cells are
// stitched directly) and returns the deferred those reused folds produced. A whole-fresh build
// (empty on-disk state) routes to the account-plane fork instead. A seed-probe read error or any
// fold error fails closed — every collected deferred update is recycled and nothing is returned, so
// a partial fold never leaks a plausible-but-wrong branch to the caller.
func (fp *foldPool) dispatchFrontier(ctx context.Context, base *HexPatriciaHashed, root *prefixNode,
	reuse func(*foldTask) ([16]cell, [16]bool, []*DeferredBranchUpdate),
) ([]*DeferredBranchUpdate, error) {
	dctx, dcleanup := fp.ctxFactory()
	if dcleanup != nil {
		defer dcleanup()
	}
	// seed-or-demote prober: a merge candidate is confirmed only if an on-disk branch exists
	// exactly at its prefix; a read error fails the whole fold closed. sawSeedable records whether
	// any on-disk branch was found, feeding the whole-fresh detection below.
	var seedErr error
	sawSeedable := false
	seedable := func(prefix []byte) bool {
		if seedErr != nil {
			return false
		}
		b, _, berr := dctx.Branch(nibbles.HexToCompact(prefix))
		if berr != nil {
			seedErr = berr
			return false
		}
		if len(b) == 0 {
			return false
		}
		sawSeedable = true
		return true
	}

	fp.foldSem = semaphore.NewWeighted(int64(maxFoldConcurrency()))
	k := foldK(root.subtreeCount, fp.numWorkers)
	seedable(append([]byte(nil), root.ext...)) // explicit root probe; derivation only probes merge candidates
	rootTask := deriveFoldFrontier(root, k, seedable)
	if seedErr != nil {
		return nil, seedErr
	}
	rootTask.base = base

	if wholeFreshBuild(base, root, sawSeedable) {
		return fp.dispatchWholeFresh(ctx, base, rootTask, reuse)
	}
	return fp.foldFrontierBody(ctx, base, rootTask, reuse)
}

// dispatchWholeFresh folds a whole-fresh build (empty on-disk state) through the account-plane
// fork-join: each top-nibble subtree folds against an empty wall via the recursive per-split-point
// fork-join (recursing across every depth-64 seam into each account's fresh storage), and the
// resulting mount-wall cells stitch into base and fold to the root — byte-identical to the serial
// account fold the frontier path would produce, but fanned out per prefix. A scheduler-pre-folded
// top nibble is reused rather than re-folded. A top nibble that is not a pure branch (a lone account
// or a whale seam at the mount boundary) routes the whole commit to the frontier fold, which handles
// those shapes. Fail-closed: any fold error recycles every collected update.
func (fp *foldPool) dispatchWholeFresh(ctx context.Context, base *HexPatriciaHashed, rootTask *foldTask,
	reuse func(*foldTask) ([16]cell, [16]bool, []*DeferredBranchUpdate),
) ([]*DeferredBranchUpdate, error) {
	wholeFreshFolds.Add(1)
	if !allTopNibblesPureBranch(rootTask.node) {
		return fp.foldFrontierBody(ctx, base, rootTask, reuse)
	}

	// A ModeParallel touch stages a nil update the fold must read from state; materialize the whole
	// plane up front (a no-op for carried updates) so the fork-join reads immutable node.update
	// fields concurrently. A resolved delete is a leaf the fresh fold would wrongly hash, so any
	// empty terminator routes the commit to the frontier fold, which drops it.
	w, wrelease := newDeferredStorageWorker(fp.workerPool, fp.ctxFactory, fp.traceW)
	forkable := accountPlaneForkable(rootTask.node, len(rootTask.prefix), int(w.accountKeyLen))
	hasEmpty, err := resolveSubtreeUpdates(rootTask.node, w.accountKeyLen, w.accountFromCacheOrDB, w.storageFromCacheOrDB)
	wrelease()
	if err != nil {
		return nil, err
	}
	if !forkable || hasEmpty {
		return fp.foldFrontierBody(ctx, base, rootTask, reuse)
	}
	wholeFreshForkJoins.Add(1)

	var (
		reusedCells    [16]cell
		reusedPresent  [16]bool
		reusedDeferred []*DeferredBranchUpdate
	)
	if reuse != nil {
		reusedCells, reusedPresent, reusedDeferred = reuse(rootTask)
	}

	sem := fp.foldSem
	if sem == nil {
		sem = semaphore.NewWeighted(int64(maxFoldConcurrency()))
	}
	ff := &forkFolder{sem: sem, k: foldK(rootTask.node.subtreeCount, fp.numWorkers), numWorkers: fp.numWorkers}

	var (
		cells    [16]cell
		present  [16]bool
		deferred []*DeferredBranchUpdate
	)
	for _, top := range rootTask.children {
		var t0 time.Time
		if onNibFold != nil {
			t0 = time.Now()
		}
		c, d, err := foldFreshAccountSubtreeCellForkJoin(ctx, ff, top.node, rootTask.prefix, top.nib)
		if err != nil {
			putDeferredUpdates(deferred)
			putDeferredUpdates(reusedDeferred)
			return nil, err
		}
		if onNibFold != nil {
			onNibFold(top.nib, top.node.subtreeCount, time.Since(t0))
		}
		cells[top.nib] = c
		present[top.nib] = true
		deferred = append(deferred, d...)
	}
	deferred = append(deferred, reusedDeferred...)
	overlayReusedCells(&cells, &present, &reusedCells, &reusedPresent)

	return fp.stitchAndFoldRoot(ctx, base, &cells, &present, deferred)
}

// overlayReusedCells stitches the scheduler's pre-folded top-nibble cells over the route's own.
func overlayReusedCells(cells *[16]cell, present *[16]bool, reusedCells *[16]cell, reusedPresent *[16]bool) {
	for nib := range 16 {
		if reusedPresent[nib] {
			cells[nib] = reusedCells[nib]
			present[nib] = true
		}
	}
}

// allTopNibblesPureBranch reports whether every child of the whole-fresh root branch is a pure branch
// (bitmap set, no plain key) — the shape the account-plane fork-join wraps as a mount-wall cell. A
// lone account or a whale seam directly at a top nibble is not, and routes the commit to the frontier.
func allTopNibblesPureBranch(root *prefixNode) bool {
	for _, child := range root.children {
		if child.bitmap == 0 || child.plainKey != nil {
			return false
		}
	}
	return true
}

// accountPlaneForkable reports whether the whole-fresh account plane (above the depth-64 seam) holds
// only account leaves. An orphan storage slot — a storage write with no account, which never occurs
// in real state — lands as a long-keyed leaf in the account plane where the direct fold would hash it
// as an account and corrupt the root; such a corpus routes to the frontier fold instead. A leaf is
// classified by its parent branch's depth (foldNode's storagePlane), so its own extension is ignored;
// storage subtrees at or below a seam are folded, not classified here, so the walk stops at the seam.
func accountPlaneForkable(node *prefixNode, depth, accountKeyLen int) bool {
	if node == nil || node.bitmap == 0 || node.plainKey != nil || depth >= 64 {
		return true
	}
	childIdx := 0
	for bm := node.bitmap; bm != 0; {
		nib := bits.TrailingZeros16(bm)
		child := node.children[childIdx]
		if child.bitmap == 0 {
			if len(child.plainKey) != accountKeyLen {
				return false
			}
		} else if !accountPlaneForkable(child, depth+1+len(child.ext), accountKeyLen) {
			return false
		}
		childIdx++
		bm &^= uint16(1) << nib
	}
	return true
}

// foldFreshAccountSubtreeCellForkJoin folds one fresh account-plane top-nibble subtree into its
// parent-stitchable mount-wall cell plus the branch records it emits, through the recursive
// fork-join. Fail-closed on any error.
func foldFreshAccountSubtreeCellForkJoin(ctx context.Context, ff *forkFolder, node *prefixNode, parentPrefix []byte, nib int) (cell, []*DeferredBranchUpdate, error) {
	wholeFreshCellFolds.Add(1)
	fc := newFoldCtx(true)
	defer fc.hph.Release()
	c, err := fc.foldSubtreeCellForkJoin(ctx, ff, node, parentPrefix, nib)
	if err != nil {
		putDeferredUpdates(fc.deferred)
		return cell{}, nil, err
	}
	return c, fc.deferred, nil
}

// foldFrontierBody folds the derived DAG below rootTask with the shared worker pool, stitches the
// top-nibble cells (plus any scheduler-reused cells) into base, and folds base down to its root,
// returning the deferred branch updates. Shared by the frontier dispatch and the whole-fresh route,
// which reach it with the same rootTask/base contract. Fail-closed: any error recycles the collected
// deferred and returns nothing.
func (fp *foldPool) foldFrontierBody(ctx context.Context, base *HexPatriciaHashed, rootTask *foldTask,
	reuse func(*foldTask) ([16]cell, [16]bool, []*DeferredBranchUpdate),
) ([]*DeferredBranchUpdate, error) {
	var (
		reusedCells    [16]cell
		reusedPresent  [16]bool
		reusedDeferred []*DeferredBranchUpdate
	)
	if reuse != nil {
		reusedCells, reusedPresent, reusedDeferred = reuse(rootTask)
	}

	deferred, err := fp.run(ctx, rootTask)
	if err != nil {
		putDeferredUpdates(reusedDeferred)
		return nil, err
	}
	deferred = append(deferred, reusedDeferred...)

	var (
		cells   [16]cell
		present [16]bool
	)
	for _, top := range rootTask.children {
		cells[top.nib] = top.cell
		present[top.nib] = true
	}
	overlayReusedCells(&cells, &present, &reusedCells, &reusedPresent)

	return fp.stitchAndFoldRoot(ctx, base, &cells, &present, deferred)
}

// stitchAndFoldRoot stitches the top-nibble cells into base's row-0 wall and folds base down to its
// root, merging any branch updates the root fold emits into deferred. Shared by the frontier dispatch
// and the whole-fresh route, which reach it with the same base/cells contract. Fail-closed: any error
// recycles deferred and returns nothing.
func (fp *foldPool) stitchAndFoldRoot(ctx context.Context, base *HexPatriciaHashed, cells *[16]cell, present *[16]bool, deferred []*DeferredBranchUpdate) ([]*DeferredBranchUpdate, error) {
	stitchSplitCells(base, cells, present)

	if base.activeRows == 0 {
		base.activeRows = 1
	}
	for base.activeRows > 0 {
		if err := ctx.Err(); err != nil {
			putDeferredUpdates(deferred)
			return nil, err
		}
		if err := base.fold(); err != nil {
			putDeferredUpdates(deferred)
			return nil, fmt.Errorf("fold frontier: root fold: %w", err)
		}
	}
	if d := base.TakeDeferredUpdates(); len(d) > 0 {
		deferred = mergeDeferredByPrefix(deferred, d)
	}
	return deferred, nil
}

// seedMerge seeds a merge task's base from Branch(prefix) so its children fold against the real
// on-disk state and every untouched sibling survives. deriveFoldFrontier only classifies a node
// as a merge when its prefix is seedable, so a missing branch here is a store change mid-Process
// and returns the hard error rather than a sibling-dropping empty wall.
func (fp *foldPool) seedMerge(t *foldTask) error {
	base, cleanup := newDeferredStorageWorker(fp.workerPool, fp.ctxFactory, fp.traceW)
	if err := seedBaseAtPrefix(base, t.prefix); err != nil {
		cleanup()
		return err
	}
	t.base = base
	t.baseCleanup = cleanup
	return nil
}

// foldOne folds one ready task into its mount-wall cell (or, for a storage-root subtask, injects
// its collapsed root hash into the account-leaf parent).
func (fp *foldPool) foldOne(ctx context.Context, t *foldTask) error {
	switch {
	case t.kind == foldLeaf:
		return fp.foldLeafTask(ctx, t)
	case isStorageRootSubtask(t):
		return fp.foldStorageSeam(ctx, t)
	default:
		return fp.foldMergeTask(ctx, t)
	}
}

// isStorageRootSubtask reports the depth-64 account/storage seam merge — the storage subtask an
// account leaf depends on, which collapses to a bare storage-root hash rather than a mount-wall
// cell.
func isStorageRootSubtask(t *foldTask) bool {
	return t.parent != nil && t.parent.storage == t
}

// foldLeafTask mounts a pooled worker on the parent merge's base, replays the leaf's key group
// (or, for an account leaf with a storage subtask, applies the account and injects the storage
// root its subtask already folded), and folds to the mount wall.
func (fp *foldPool) foldLeafTask(ctx context.Context, t *foldTask) error {
	w, release := newDeferredStorageWorker(fp.workerPool, fp.ctxFactory, fp.traceW)
	defer release()

	if fp.directLeafEligible(t) {
		hasEmpty, err := resolveSubtreeUpdates(t.node, w.accountKeyLen, w.accountFromCacheOrDB, w.storageFromCacheOrDB)
		if err != nil {
			return err
		}
		// An empty (deleted) leaf must be dropped, which the fresh direct fold does not do; fall
		// back to replay, which folds the now-populated delete correctly.
		if !hasEmpty {
			directLeafFolds.Add(1)
			c, deferred, ferr := foldFreshAccountSubtreeCellDeferred(t.node, t.parent.prefix, t.nib)
			if ferr != nil {
				return ferr
			}
			t.cell = c
			t.deferred = append(t.deferred, deferred...)
			return nil
		}
		directLeafFallbacks.Add(1)
	}

	w.mountTo(t.parent.base, t.nib)

	switch {
	case t.storage != nil:
		if t.node.plainKey != nil {
			if err := w.followAndUpdate(t.prefix, t.node.plainKey, t.node.update); err != nil {
				return err
			}
		}
		setAccountStorageRoot(w, t.prefix, t.storageRoot)
	case t.freshWhaleCandidate:
		if err := fp.foldWhaleLeaf(ctx, w, t); err != nil {
			return err
		}
	default:
		for _, kk := range collectSubtreeKeys(t.node, append([]byte(nil), t.prefix...)) {
			if err := w.followAndUpdate(kk.hk, kk.pk, kk.upd); err != nil {
				return err
			}
		}
	}

	c, err := w.foldMounted(ctx, t.nib)
	if err != nil {
		return err
	}
	t.cell = c
	t.deferred = append(t.deferred, w.TakeDeferredUpdates()...)
	return nil
}

// directLeafFolds counts leaf tasks folded through the direct foldNode recursion (the truthtree
// path) instead of mount+replay. Read by tests to confirm the flag-on path actually executed.
var directLeafFolds atomic.Int64

// directLeafFallbacks counts eligible leaves that resolved an empty (deleted) terminator and fell
// back to replay rather than the fresh direct fold. Read by tests to confirm that guard executed.
var directLeafFallbacks atomic.Int64

// directLeafEligible reports whether a leaf task can be folded by the direct foldNode recursion
// instead of mount+replay: the flag is on and the leaf is a fresh, pure account-plane branch
// subtree. Freshness is proven by an empty mounted slot on the parent's seeded base — the on-disk
// Branch(parentPrefix) had no child there, so nothing on disk lives under this subtree and the
// fresh fold has no siblings to drop. The fresh-whale storage leaf routes through foldNode in
// foldWhaleLeaf; seedable merge/seam leaves read on-disk siblings the fresh fold cannot reproduce,
// so they stay on replay.
func (fp *foldPool) directLeafEligible(t *foldTask) bool {
	if !fp.truthtreeFold {
		return false
	}
	if t.plane != planeAccount || len(t.prefix) >= 64 {
		return false
	}
	if t.node.bitmap == 0 || t.node.plainKey != nil {
		return false
	}
	return mountSlotEmpty(t.parent.base, t.nib)
}

// mountSlotEmpty reports whether the parent base's mount-wall slot for nib carries no on-disk
// state — an empty cell means Branch(parentPrefix) had no child there, so the subtree mounted at
// that slot is fresh.
func mountSlotEmpty(base *HexPatriciaHashed, nib int) bool {
	if base == nil {
		return false
	}
	if base.activeRows == 0 {
		return base.root.IsEmpty()
	}
	return base.grid[base.activeRows-1][nib].IsEmpty()
}

// foldWhaleLeaf applies a demoted big-storage account, then folds its storage: in parallel when
// the account proves fresh (empty on disk, so its storage is provably storage-less and needs no
// seed), else serial. It leaves the account cell, with its storage root injected, for foldMounted.
func (fp *foldPool) foldWhaleLeaf(ctx context.Context, w *HexPatriciaHashed, t *foldTask) error {
	if err := w.followAndUpdate(t.prefix, t.node.plainKey, t.node.update); err != nil {
		return err
	}
	if !w.lastUpdateCellWasEmpty {
		for _, kk := range collectStorageKeys(t.node, t.prefix) {
			if err := w.followAndUpdate(kk.hk, kk.pk, kk.upd); err != nil {
				return err
			}
		}
		return nil
	}
	sr, deferred, err := fp.foldFreshWhaleStorage(ctx, w, t.node, t.prefix)
	if err != nil {
		putDeferredUpdates(deferred)
		return err
	}
	t.deferred = append(t.deferred, deferred...)
	setAccountStorageRoot(w, t.prefix, sr)
	return nil
}

// directWhaleStorageFolds counts fresh-whale storage subtrees folded through the direct foldNode
// recursion (foldFreshStorageRootDeferred) instead of foldFreshStorage's mount+replay fan-out. Read
// by tests to confirm the flag-on whale path executed.
var directWhaleStorageFolds atomic.Int64

// directWhaleStorageFallbacks counts eligible fresh-whale storage subtrees that resolved a deleted
// slot and fell back to foldFreshStorage rather than the direct fold. Read by tests to confirm the
// guard executed.
var directWhaleStorageFallbacks atomic.Int64

// foldFreshWhaleStorage folds a provably-fresh whale's storage subtree into its root and branch
// records. With the truthtree flag on and a real (>=2 first-nibble) storage-root branch, it routes
// through the direct foldNode recursion — the case foldFreshStorageRootDeferred reproduces
// foldFreshStorage byte-for-byte; a single-first-nibble top collapses to an extension root the direct
// depth-64 keccak does not build, so it stays on foldFreshStorage. The direct fold reads each leaf's
// update straight, so the storage children's ModeParallel nil updates are materialized from the
// worker's ctx first; a materialized delete is a slot the direct fold would wrongly hash, so it falls
// back to replay (which drops it). Flag-off or ineligible ⇒ the mount+replay path.
func (fp *foldPool) foldFreshWhaleStorage(ctx context.Context, w *HexPatriciaHashed, node *prefixNode, accPrefix []byte) (common.Hash, []*DeferredBranchUpdate, error) {
	if fp.truthtreeFold && bits.OnesCount16(node.bitmap) >= 2 {
		hasEmpty, err := resolveSubtreeUpdates(node, w.accountKeyLen, w.accountFromCacheOrDB, w.storageFromCacheOrDB)
		if err != nil {
			return common.Hash{}, nil, err
		}
		if !hasEmpty {
			directWhaleStorageFolds.Add(1)
			return foldFreshStorageRootDeferred(node, accPrefix)
		}
		directWhaleStorageFallbacks.Add(1)
	}
	return fp.foldFreshStorage(ctx, node, accPrefix)
}

// freshWhaleParallelFolds counts fresh-whale parallel storage folds — the only runtime signal
// that foldFreshStorage's concurrent path (not the serial fallback in foldWhaleLeaf) executed.
// Read by tests to confirm coverage of a rarely-taken path.
var freshWhaleParallelFolds atomic.Int64

// foldFreshStorage folds a fresh account's storage subtree in parallel by first nibble against an
// empty wall — the account is provably storage-less on disk, so seedBaseAtPrefix's reset wall is
// the correct seed. Returns the collapsed storage root and the deferred branch updates produced.
func (fp *foldPool) foldFreshStorage(ctx context.Context, node *prefixNode, accPrefix []byte) (common.Hash, []*DeferredBranchUpdate, error) {
	freshWhaleParallelFolds.Add(1)
	base, releaseBase := newDeferredStorageWorker(fp.workerPool, fp.ctxFactory, fp.traceW)
	defer releaseBase()
	if err := seedBaseAtPrefix(base, accPrefix); err != nil && !errors.Is(err, errStorageBaseNotBranch) {
		return common.Hash{}, nil, fmt.Errorf("fresh storage seed: %w", err)
	}

	sem := fp.foldSem
	if sem == nil {
		sem = semaphore.NewWeighted(int64(maxFoldConcurrency()))
	}
	var (
		children [16]cell
		mu       sync.Mutex
		deferred []*DeferredBranchUpdate
	)
	g, gctx := errgroup.WithContext(ctx)
	childIdx := 0
	for bm := node.bitmap; bm != 0; {
		nib := int(bits.TrailingZeros16(bm))
		child := node.children[childIdx]
		childPrefix := make([]byte, len(accPrefix), len(accPrefix)+1+len(child.ext))
		copy(childPrefix, accPrefix)
		childPrefix = append(childPrefix, byte(nib))
		childPrefix = append(childPrefix, child.ext...)
		ni := nib
		g.Go(func() error {
			if err := sem.Acquire(gctx, 1); err != nil {
				return err
			}
			defer sem.Release(1)
			cw, crelease := newDeferredStorageWorker(fp.workerPool, fp.ctxFactory, fp.traceW)
			defer crelease()
			cw.mountTo(base, ni)
			// Each goroutine walks its own disjoint first-nibble subtree read-only and streams
			// keys straight into followAndUpdate, so no []touchedKey is materialized per group.
			if err := dfsSubtree(child, childPrefix, func(hk, pk []byte, upd *Update) error {
				return cw.followAndUpdate(hk, pk, upd)
			}); err != nil {
				return err
			}
			c, err := cw.foldMounted(gctx, ni)
			if err != nil {
				return fmt.Errorf("fresh storage nibble[%x]: %w", ni, err)
			}
			if d := cw.TakeDeferredUpdates(); len(d) > 0 {
				mu.Lock()
				deferred = append(deferred, d...)
				mu.Unlock()
			}
			children[ni] = c
			return nil
		})
		childIdx++
		bm &^= uint16(1) << nib
	}
	if err := g.Wait(); err != nil {
		return common.Hash{}, deferred, err
	}

	sr, err := aggregateMountedStorageRoot(base, &children, node.bitmap)
	if err != nil {
		return common.Hash{}, deferred, fmt.Errorf("fresh storage aggregate: %w", err)
	}
	if d := base.TakeDeferredUpdates(); len(d) > 0 {
		deferred = append(deferred, d...)
	}
	if sr.IsEmpty() {
		return empty.RootHash, deferred, nil
	}
	return sr.hash, deferred, nil
}

// foldFreshForkJoin folds a provably-fresh whale's storage subtree into its root and branch records
// with a recursive per-split-point fork-join over foldNode: at any storage branch larger than
// K = foldK(node.subtreeCount, numWorkers) — the same threshold the fold DAG uses — each child
// branch forks onto foldSem with its own foldCtx and recurses, while smaller subtrees fold serially
// with buffer reuse. The fork unit is the split point, not the top nibble, so nested splits fan out
// at every depth. Byte-identical to the serial foldFreshStorageRootDeferred; fail-closed drops every
// collected update on any error.
func (fp *foldPool) foldFreshForkJoin(ctx context.Context, node *prefixNode, accPrefix []byte) (common.Hash, []*DeferredBranchUpdate, error) {
	if node == nil || node.bitmap == 0 {
		return empty.RootHash, nil, nil
	}
	sem := fp.foldSem
	if sem == nil {
		sem = semaphore.NewWeighted(int64(maxFoldConcurrency()))
	}
	ff := &forkFolder{sem: sem, k: foldK(node.subtreeCount, fp.numWorkers), numWorkers: fp.numWorkers}
	fc := newFoldCtx(true)
	defer fc.hph.Release()
	h, err := ff.fold(ctx, fc, node, accPrefix, 64)
	if err != nil {
		putDeferredUpdates(fc.deferred)
		return common.Hash{}, nil, err
	}
	return h, fc.deferred, nil
}

// collectStorageKeys collects a depth-64 account node's storage keys (its child subtrees),
// excluding the account terminator the caller applies separately.
func collectStorageKeys(node *prefixNode, accPrefix []byte) []touchedKey {
	var out []touchedKey
	childIdx := 0
	for bm := node.bitmap; bm != 0; {
		nib := bits.TrailingZeros16(bm)
		child := node.children[childIdx]
		childPrefix := make([]byte, len(accPrefix), len(accPrefix)+1+len(child.ext))
		copy(childPrefix, accPrefix)
		childPrefix = append(childPrefix, byte(nib))
		childPrefix = append(childPrefix, child.ext...)
		out = append(out, collectSubtreeKeys(child, childPrefix)...)
		childIdx++
		bm &^= uint16(1) << nib
	}
	return out
}

// foldMergeTask stitches the task's already-folded child cells into its seeded base and folds to
// the mount wall the parent stitches at.
func (fp *foldPool) foldMergeTask(_ context.Context, t *foldTask) error {
	children := childCells(t)
	mountWall := int16(len(t.parent.prefix)) + 1
	c, err := mergeChildrenAtPrefix(t.base, &children, t.node.bitmap, mountWall)
	if err != nil {
		return err
	}
	t.cell = c
	t.deferred = t.base.TakeDeferredUpdates()
	fp.releaseBase(t)
	return nil
}

// foldStorageSeam folds the depth-64 storage subtask into a bare storage-root hash and injects it
// into the account-leaf parent, which folds once this subtask releases its pending edge.
func (fp *foldPool) foldStorageSeam(_ context.Context, t *foldTask) error {
	children := childCells(t)
	sr, err := aggregateMountedStorageRoot(t.base, &children, t.node.bitmap)
	if err != nil {
		return err
	}
	if sr.IsEmpty() {
		t.parent.storageRoot = empty.RootHash
	} else {
		t.parent.storageRoot = sr.hash
	}
	t.deferred = t.base.TakeDeferredUpdates()
	fp.releaseBase(t)
	return nil
}

// childCells gathers a merge task's already-folded child cells into a row indexed by nibble.
func childCells(t *foldTask) [16]cell {
	var out [16]cell
	for _, c := range t.children {
		out[c.nib] = c.cell
	}
	return out
}

func (fp *foldPool) releaseBase(t *foldTask) {
	if t.baseCleanup != nil {
		t.baseCleanup()
		t.baseCleanup = nil
	}
	t.base = nil
}

func recycleTaskDeferred(tasks []*foldTask) {
	for _, t := range tasks {
		for _, upd := range t.deferred {
			putDeferredUpdate(upd)
		}
		t.deferred = nil
	}
}

// dispatchFoldTasks runs subTasks through fold on a shared pool of numWorkers goroutines. A task
// is ready when its pending counter reaches zero; folding it decrements its parent's counter and
// enqueues the parent at zero. rootTask (the finale) is never folded — its children's completion
// just leaves their cells ready. The ready channel is sized to the task count so no send blocks,
// and the pool closes it once every task has folded; an error cancels the shared context and
// every worker drains out. fold is a parameter so the scheduling can be tested without real tries.
func dispatchFoldTasks(ctx context.Context, numWorkers int, rootTask *foldTask, subTasks []*foldTask, fold func(context.Context, *foldTask) error) error {
	if len(subTasks) == 0 {
		return nil
	}
	ready := make(chan *foldTask, len(subTasks))
	for _, t := range subTasks {
		if t.pending.Load() == 0 {
			ready <- t
		}
	}

	total := int64(len(subTasks))
	var done atomic.Int64
	nw := max(numWorkers, 1)

	g, gctx := errgroup.WithContext(ctx)
	for range nw {
		g.Go(func() error {
			for {
				select {
				case <-gctx.Done():
					return gctx.Err()
				case t, ok := <-ready:
					if !ok {
						return nil
					}
					if err := fold(gctx, t); err != nil {
						return err
					}
					if p := t.parent; p != nil && p != rootTask {
						if p.pending.Add(-1) == 0 {
							ready <- p
						}
					}
					if done.Add(1) == total {
						close(ready)
					}
				}
			}
		})
	}
	return g.Wait()
}
