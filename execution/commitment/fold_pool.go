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

	if err := dispatchFoldTasks(ctx, fp.numWorkers, rootTask, subTasks, fp.foldOne); err != nil {
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

// dispatchFrontier derives the fold DAG over root, folds it with fp's shared worker pool, stitches
// the resulting top-nibble cells into base, and folds base down to its root, returning the deferred
// branch updates the whole fold produced. reuse, when non-nil, is the streaming scheduler's
// cached-cell consumer: it prunes clean top nibbles from the DAG (their pre-folded cells are
// stitched directly) and returns the deferred those reused folds produced. A seed-probe read error
// or any fold error fails closed — every collected deferred update is recycled and nothing is
// returned, so a partial fold never leaks a plausible-but-wrong branch to the caller.
func (fp *foldPool) dispatchFrontier(ctx context.Context, base *HexPatriciaHashed, root *prefixNode,
	reuse func(*foldTask) ([16]cell, [16]bool, []*DeferredBranchUpdate),
) ([]*DeferredBranchUpdate, error) {
	dctx, dcleanup := fp.ctxFactory()
	if dcleanup != nil {
		defer dcleanup()
	}
	// seed-or-demote prober: a merge candidate is confirmed only if an on-disk branch exists
	// exactly at its prefix; a read error fails the whole fold closed.
	var seedErr error
	seedable := func(prefix []byte) bool {
		if seedErr != nil {
			return false
		}
		b, _, berr := dctx.Branch(nibbles.HexToCompact(prefix))
		if berr != nil {
			seedErr = berr
			return false
		}
		return len(b) > 0
	}

	fp.foldSem = semaphore.NewWeighted(int64(maxFoldConcurrency()))
	k := foldK(root.subtreeCount, fp.numWorkers)
	rootTask := deriveFoldFrontier(root, k, seedable)
	if seedErr != nil {
		return nil, seedErr
	}
	rootTask.base = base

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
	for nib := range 16 {
		if reusedPresent[nib] {
			cells[nib] = reusedCells[nib]
			present[nib] = true
		}
	}

	stitchSplitCells(base, &cells, &present)

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
	sr, deferred, err := fp.foldFreshStorage(ctx, t.node, t.prefix)
	if err != nil {
		return err
	}
	t.deferred = append(t.deferred, deferred...)
	setAccountStorageRoot(w, t.prefix, sr)
	return nil
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
