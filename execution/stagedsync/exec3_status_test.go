package stagedsync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests pin the dispatch-flow bookkeeping in execStatusList in
// isolation. Until now this structure was exercised only through the full
// parallel-exec integration path; dependency-ordered validation changes how
// txs are selected for validation, so the existing contiguous behaviour needs
// a regression net and the new non-contiguous selection needs direct coverage.

func drainPending(m *execStatusList) []int {
	var out []int
	for {
		tx := m.takeNextPending()
		if tx < 0 {
			return out
		}
		out = append(out, tx)
	}
}

func TestExecStatusList_TakeNextPending_MinFirst(t *testing.T) {
	var m execStatusList
	for _, tx := range []int{4, 1, 3, 0, 2} {
		m.pushPending(tx)
	}
	require.Equal(t, 0, m.minPending())

	got := drainPending(&m)
	require.Equal(t, []int{0, 1, 2, 3, 4}, got, "takeNextPending must return pending in ascending order")
	require.Equal(t, -1, m.takeNextPending(), "empty pending returns -1")
	require.Equal(t, 5, m.inProgressCount(), "each taken tx becomes in-progress")
}

func TestExecStatusList_MaxCompleteContiguous(t *testing.T) {
	var m execStatusList
	for i := range 5 {
		m.pushPending(i)
	}
	drainPending(&m) // all in-progress

	require.Equal(t, -1, m.maxComplete(), "nothing complete yet")

	// Complete out of order: the contiguous prefix must not advance past a hole.
	m.markComplete(2)
	require.Equal(t, -1, m.maxComplete(), "tx0 still missing")
	m.markComplete(0)
	require.Equal(t, 0, m.maxComplete(), "prefix advances to 0, stops at hole (1)")
	m.markComplete(1)
	require.Equal(t, 2, m.maxComplete(), "0,1,2 now contiguous")
	m.markComplete(4)
	require.Equal(t, 2, m.maxComplete(), "still a hole at 3")
	m.markComplete(3)
	require.Equal(t, 4, m.maxComplete(), "3 fills the hole; prefix jumps to 4")
	require.Equal(t, 5, m.countComplete())
}

func TestExecStatusList_Dependency_Requeue(t *testing.T) {
	var m execStatusList
	for i := range 3 {
		m.pushPending(i)
	}
	drainPending(&m)

	// tx2 hit ErrDependency on tx1: it is pulled out of flight and blocked.
	m.clearInProgress(2)
	require.True(t, m.addDependency(1, 2), "recording a live blocker returns true")
	require.True(t, m.isBlocked(2))
	require.False(t, m.isBlocked(1))

	// tx1 completes → its dependents unblock and are re-queued for retry.
	m.markComplete(1)
	m.removeDependency(1)
	require.False(t, m.isBlocked(2), "tx2 no longer blocked")
	require.True(t, m.checkPending(2), "unblocked dependent is re-queued to pending")
}

func TestExecStatusList_AddDependency_Rejects(t *testing.T) {
	var m execStatusList
	require.False(t, m.addDependency(-1, 2), "negative blocker rejected")
	require.False(t, m.addDependency(3, 2), "blocker must precede dependent")
	require.False(t, m.addDependency(2, 2), "self-dependency rejected")
}

func TestExecStatusList_AddDependency_BlockerAlreadyComplete(t *testing.T) {
	var m execStatusList
	m.pushPending(0)
	m.pushPending(2)
	drainPending(&m)
	m.markComplete(0)

	// Blocker already complete → no live dependency recorded.
	require.False(t, m.addDependency(0, 2), "already-complete sole blocker leaves dependent runnable")
	require.False(t, m.isBlocked(2))
}

func TestExecStatusList_DrainDeferredIfReady(t *testing.T) {
	var m execStatusList
	m.pushDeferred(5)
	m.pushDeferred(7)

	m.drainDeferredIfReady(func(tx int) bool { return tx == 5 })
	require.True(t, m.checkPending(5), "ready deferred tx moved to pending")
	require.False(t, m.checkPending(7), "not-ready deferred tx stays deferred")

	m.drainDeferred()
	require.True(t, m.checkPending(7), "unconditional drain moves the rest")
}

func TestExecStatusList_RevalidationRange(t *testing.T) {
	var m execStatusList
	for i := range 5 {
		m.pushPending(i)
	}
	drainPending(&m)
	for i := range 5 {
		m.markComplete(i)
	}
	// tx3 re-dispatched (in-progress again) → excluded from the range.
	m.setInProgress(3)

	require.Equal(t, []int{2, 4}, m.getRevalidationRange(2), "range excludes in-progress tx3")

	m.pushPendingSet([]int{2, 4})
	require.False(t, m.checkComplete(2), "pushPendingSet clears complete")
	require.True(t, m.checkPending(2))
	require.True(t, m.checkPending(4))
}

// --- New non-contiguous selection API driving dependency-ordered validation ---

func TestExecStatusList_TakePendingWhere_SelectsNonContiguously(t *testing.T) {
	var m execStatusList
	for i := range 6 {
		m.pushPending(i)
	}

	// Select only the even txs — a non-contiguous subset. The odd txs must
	// remain pending and untouched.
	got := m.takePendingWhere(func(tx int) bool { return tx%2 == 0 })
	require.Equal(t, []int{0, 2, 4}, got, "returns matching pending txs in ascending order")

	require.Equal(t, 3, m.inProgressCount(), "selected txs become in-progress")
	for _, tx := range []int{0, 2, 4} {
		require.True(t, m.checkInProgress(tx))
		require.False(t, m.checkPending(tx), "selected txs removed from pending")
	}
	for _, tx := range []int{1, 3, 5} {
		require.True(t, m.checkPending(tx), "unselected txs stay pending")
		require.False(t, m.checkInProgress(tx))
	}
	require.Equal(t, 1, m.minPending(), "min pending is now the lowest unselected tx")
}

func TestExecStatusList_TakePendingWhere_Empty(t *testing.T) {
	var m execStatusList
	m.pushPending(0)
	m.pushPending(1)

	require.Nil(t, m.takePendingWhere(func(int) bool { return false }), "no match → nil, pending untouched")
	require.True(t, m.checkPending(0))
	require.True(t, m.checkPending(1))
	require.Equal(t, 0, m.inProgressCount())
}

func TestExecStatusList_TakePendingWhere_AllMatch_EqualsDrain(t *testing.T) {
	var m execStatusList
	for _, tx := range []int{3, 1, 4, 2, 0} {
		m.pushPending(tx)
	}
	got := m.takePendingWhere(func(int) bool { return true })
	require.Equal(t, []int{0, 1, 2, 3, 4}, got, "select-all matches contiguous drain order")
	require.Equal(t, -1, m.minPending())
	require.Equal(t, 5, m.inProgressCount())
}
