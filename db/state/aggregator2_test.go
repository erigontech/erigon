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

package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// A background build and merge can overlap; editing must stay disabled until
// every lock is released, or Preset* could mutate worker config mid-merge.
func TestWorkersCfgEditingLockIsReentrant(t *testing.T) {
	t.Parallel()
	w := &workersCfg{merge: 1, collateAndBuild: 1}

	editable := func() bool {
		ran := false
		w.trySet(func() { ran = true })
		return ran
	}

	require.True(t, editable(), "editing enabled initially")

	w.lockEditing()
	require.False(t, editable(), "disabled while one op holds the lock")

	w.lockEditing()
	w.unlockEditing()
	require.False(t, editable(), "still disabled while a second op holds it")

	w.unlockEditing()
	require.True(t, editable(), "re-enabled after the last lock is released")

	w.unlockEditing()
	require.True(t, editable(), "extra unlock must not underflow and disable editing")
}

func TestWorkersCfgAppliesRequestDeferredWhilePinned(t *testing.T) {
	t.Parallel()
	w := &workersCfg{merge: 1, collateAndBuild: 1}
	compress := 0

	w.lockEditing()
	w.setMerge(2)
	w.setCollateAndBuild(4)
	w.trySet(func() { compress = 8 })
	require.Equal(t, 1, w.getMerge(), "must not change under a running build or merge")
	require.Equal(t, 1, w.getCollateAndBuild())
	require.Zero(t, compress)

	w.unlockEditing()
	require.Equal(t, 2, w.getMerge(), "held request applies on release")
	require.Equal(t, 4, w.getCollateAndBuild())
	require.Equal(t, 8, compress, "every queued request runs, not just the last")
}

func TestWorkersCfgHoldsRequestUntilLastPinReleases(t *testing.T) {
	t.Parallel()
	w := &workersCfg{merge: 1, collateAndBuild: 1}

	w.lockEditing()
	w.lockEditing()
	w.setMerge(2)
	w.setCollateAndBuild(4)

	w.unlockEditing()
	require.Equal(t, 1, w.getCollateAndBuild(), "one of two overlapping pins released is still pinned")

	w.unlockEditing()
	require.Equal(t, 4, w.getCollateAndBuild())
}

func TestWorkersCfgKeepsOnlyTheNewestRequest(t *testing.T) {
	t.Parallel()
	w := &workersCfg{merge: 1, collateAndBuild: 1}

	w.lockEditing()
	w.setMerge(2)
	w.setCollateAndBuild(4)
	w.setMerge(3)
	w.setCollateAndBuild(6)
	w.unlockEditing()
	require.Equal(t, 3, w.getMerge())
	require.Equal(t, 6, w.getCollateAndBuild())

	// A pin/release cycle with no request in between must not resurrect a stale value.
	w.lockEditing()
	w.unlockEditing()
	require.Equal(t, 6, w.getCollateAndBuild())
}
