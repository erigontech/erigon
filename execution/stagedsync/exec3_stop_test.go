package stagedsync

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A stopped round shortens itself to the prefix that completed, and the lists have to come back with it.
//
// The block is complete when every task is complete, so a task left pending above the cut holds the block
// open forever — the exec loop waits on a result that nothing will produce. A dependency registered against
// one does the same thing a step removed: the task it blocks is released by a completion that never comes.
func TestExecStatusListTruncate(t *testing.T) {
	var m execStatusList
	for i := range 6 {
		m.pushPending(i)
	}
	require.True(t, m.addDependency(2, 5), "5 waits on 2 — a blocker that survives the cut")
	require.True(t, m.addDependency(3, 4), "4 waits on 3 — a blocker that does not")
	m.clearPending(4)
	m.clearPending(5)
	for i := range 3 {
		m.takeNextPending()
		m.markComplete(i)
	}
	m.takeNextPending() // 3 is in progress
	m.pushDeferred(4)

	m.truncate(3)

	require.Equal(t, 3, m.countComplete())
	require.Equal(t, 2, m.maxComplete())
	require.Equal(t, -1, m.minPending(), "a task above the cut was left pending")
	require.Equal(t, 0, m.inProgressCount(), "a task above the cut was left in progress")
	require.Empty(t, m.deferred, "a task above the cut was left deferred")
	for blocker, deps := range m.dependency {
		require.Less(t, blocker, 3, "a dependency survives on a task that was cut away")
		for dep := range deps {
			require.Less(t, dep, 3, "a dependent survives on a task that was cut away")
		}
	}
	for dependent, blockers := range m.blocker {
		require.Less(t, dependent, 3)
		for b := range blockers {
			require.Less(t, b, 3)
		}
	}
}
