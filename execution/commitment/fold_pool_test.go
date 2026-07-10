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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

// The frontier pool's correctness under real tries is covered by TestFrontierParity (the streaming
// and streaming-scheduled modes now run through this dispatch). These tests pin the two properties
// that byte-parity alone does not prove: the dispatch is deadlock-free and concurrency-bounded even
// when ready tasks vastly outnumber workers, and a clean scheduler-folded top nibble is reused
// rather than silently re-folded.

// mkFoldTask builds a synthetic task wired under parent, for scheduling tests that never touch a
// real trie.
func mkFoldTask(kind foldTaskKind, parent *foldTask) *foldTask {
	t := &foldTask{kind: kind, parent: parent, nib: -1}
	if parent != nil {
		parent.children = append(parent.children, t)
	}
	return t
}

// setFoldPending sets each sub-task's pending counter the way the real derivation does: a merge
// waits on its children, an account leaf waits on its storage subtask, a plain leaf is ready.
func setFoldPending(subTasks []*foldTask) {
	for _, t := range subTasks {
		switch {
		case t.kind == foldMerge:
			t.pending.Store(int32(len(t.children)))
		case t.storage != nil:
			t.pending.Store(1)
		default:
			t.pending.Store(0)
		}
	}
}

// TestFoldPool_DispatchScheduling drives dispatchFoldTasks over a synthetic DAG with far more ready
// tasks than workers, asserting: every sub-task folds exactly once, no parent folds before its
// children (nor an account leaf before its storage subtask), concurrency never exceeds the worker
// count, and the whole thing completes — deadlock-free — rather than hanging.
func TestFoldPool_DispatchScheduling(t *testing.T) {
	t.Parallel()
	const numWorkers = 3

	root := mkFoldTask(foldMerge, nil)

	// Many independent ready leaves: the oversubscription that must stay bounded, not hang.
	for range 200 {
		mkFoldTask(foldLeaf, root)
	}

	// A multi-level merge chain: root -> m2 -> inner -> leaves, so a parent only becomes ready
	// once every descendant has folded.
	m2 := mkFoldTask(foldMerge, root)
	inner := mkFoldTask(foldMerge, m2)
	for range 3 {
		mkFoldTask(foldLeaf, inner)
	}
	// Give m2 a second, shallower child so it waits on two independent sub-chains.
	mkFoldTask(foldLeaf, m2)

	// An account leaf that depends on its storage-root subtask (the depth-64 seam edge): the
	// account must not fold until the storage subtask completes. The subtask hangs off the
	// account's storage edge, not its children, exactly as the derivation wires it.
	acct := mkFoldTask(foldLeaf, root)
	storage := &foldTask{kind: foldMerge, parent: acct, nib: -1}
	acct.storage = storage
	for range 2 {
		mkFoldTask(foldLeaf, storage)
	}

	subTasks := collectFoldTasks(root, nil)
	setFoldPending(subTasks)

	var (
		mu     sync.Mutex
		folded = make(map[*foldTask]bool, len(subTasks))
		cur    int
		maxCur int
		order  error
	)
	fold := func(_ context.Context, task *foldTask) error {
		mu.Lock()
		cur++
		if cur > maxCur {
			maxCur = cur
		}
		for _, c := range task.children {
			if !folded[c] && order == nil {
				order = errFoldOrder("parent folded before a child")
			}
		}
		if task.storage != nil && !folded[task.storage] && order == nil {
			order = errFoldOrder("account leaf folded before its storage subtask")
		}
		mu.Unlock()

		time.Sleep(time.Millisecond) // widen the window so real concurrency shows up in maxCur

		mu.Lock()
		folded[task] = true
		cur--
		mu.Unlock()
		return nil
	}

	done := make(chan error, 1)
	go func() { done <- dispatchFoldTasks(context.Background(), numWorkers, root, subTasks, fold) }()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("dispatchFoldTasks did not complete: deadlock")
	}

	require.NoError(t, order, "fold order violated a dependency edge")
	require.Len(t, folded, len(subTasks), "every sub-task must fold exactly once")
	require.LessOrEqual(t, maxCur, numWorkers, "concurrency must stay bounded by the worker count")
	require.Greater(t, maxCur, 1, "the pool must actually run tasks concurrently")
	require.Zero(t, cur, "no worker may remain in-flight after completion")
}

type foldOrderErr string

func (e foldOrderErr) Error() string { return string(e) }
func errFoldOrder(s string) error    { return foldOrderErr(s) }

// TestFoldPool_ReuseSchedulerCells proves the frontier dispatch consumes the eager scheduler's
// cached cells rather than orphaning them: a clean (folded, not re-dirtied) top nibble is pruned
// from the pool and its cell/deferred are lifted out for the finale stitch, while a re-dirtied
// nibble stays in the pool and its stale cached deferred are dropped.
func TestFoldPool_ReuseSchedulerCells(t *testing.T) {
	t.Parallel()

	keys := [][]byte{acctKeyNib(0x1, 0), acctKeyNib(0x2, 0), acctKeyNib(0x3, 0)}
	trie := buildDAGTrie(keys)
	rootTask := deriveFoldFrontier(trie.root, 1<<20, alwaysSeedable)
	require.Equal(t, foldMerge, rootTask.kind)
	require.Len(t, rootTask.children, 3)

	sc := NewStreamingCommitter(nil, length.Addr, DefaultTrieConfig())

	var arena byteArena
	cleanCell := cell{hashLen: 32}
	cleanUpd := getDeferredUpdate(&arena, []byte{0x1}, []byte{0, 0, 0, 0}, nil)
	sc.splits[0x1] = &splitState{prefix: []byte{0x1}, folded: true, dirty: false, cell: cleanCell, deferred: []*DeferredBranchUpdate{cleanUpd}}

	staleUpd := getDeferredUpdate(&arena, []byte{0x2}, []byte{0, 0, 0, 0}, nil)
	sc.splits[0x2] = &splitState{prefix: []byte{0x2}, folded: true, dirty: true, cell: cell{hashLen: 16}, deferred: []*DeferredBranchUpdate{staleUpd}}

	cells, present, deferred := sc.reuseSchedulerCells(rootTask)

	require.True(t, present[0x1], "the clean nibble must be marked present for the finale stitch")
	require.Equal(t, cleanCell, cells[0x1], "the reused cell must be the scheduler's folded cell")
	require.Equal(t, []*DeferredBranchUpdate{cleanUpd}, deferred, "the reused split's deferred must be lifted out")
	require.Nil(t, sc.splits[0x1].deferred, "a reused split hands off ownership of its deferred")

	require.False(t, present[0x2], "a re-dirtied nibble is not reused")
	require.False(t, present[0x3], "a nibble with no split is not reused")
	require.Nil(t, sc.splits[0x2].deferred, "a re-dirtied split's stale deferred are recycled")

	remaining := map[int]bool{}
	for _, top := range rootTask.children {
		remaining[top.nib] = true
	}
	require.NotContains(t, remaining, 0x1, "the clean nibble is pruned from the pool")
	require.Contains(t, remaining, 0x2, "the re-dirtied nibble folds fresh through the pool")
	require.Contains(t, remaining, 0x3, "the fresh nibble folds through the pool")
}
