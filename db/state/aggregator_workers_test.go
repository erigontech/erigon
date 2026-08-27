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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkersCfgAppliesRequestsDeferredWhilePinned(t *testing.T) {
	w := newWorkersCfg(1, 1)

	w.lockEditing()
	w.setCollateAndBuild(4)
	w.setMerge(2)
	compressed := 0
	w.trySet(func() { compressed = 8 })

	assert.Equal(t, 1, w.getCollateAndBuild(), "collate must not change while a background op pins the config")
	assert.Equal(t, 1, w.getMerge(), "merge must not change while pinned")
	assert.Equal(t, 0, compressed, "compressor write must not run while pinned")

	w.unlockEditing()

	assert.Equal(t, 4, w.getCollateAndBuild(), "collate requested while pinned must apply on release")
	assert.Equal(t, 2, w.getMerge(), "merge requested while pinned must apply on release")
	assert.Equal(t, 8, compressed, "compressor write requested while pinned must run on release")
}

func TestWorkersCfgHoldsRequestUntilLastPinReleases(t *testing.T) {
	w := newWorkersCfg(1, 1)

	w.lockEditing()
	w.lockEditing()
	w.setCollateAndBuild(4)

	w.unlockEditing()
	require.Equal(t, 1, w.getCollateAndBuild(), "one of two overlapping pins released is still pinned")

	w.unlockEditing()
	assert.Equal(t, 4, w.getCollateAndBuild(), "last pin released applies the held request")
}

func TestWorkersCfgUnpinnedSetsApplyImmediately(t *testing.T) {
	w := newWorkersCfg(1, 1)

	w.setCollateAndBuild(4)
	w.setMerge(2)
	assert.Equal(t, 4, w.getCollateAndBuild())
	assert.Equal(t, 2, w.getMerge())

	// A later pin/release cycle with no request in between must not resurrect a
	// stale value over what is already set.
	w.lockEditing()
	w.unlockEditing()
	assert.Equal(t, 4, w.getCollateAndBuild(), "release with no pending request keeps the current count")
	assert.Equal(t, 2, w.getMerge())
}

func TestWorkersCfgKeepsOnlyTheLatestRequestWhilePinned(t *testing.T) {
	w := newWorkersCfg(1, 1)

	w.lockEditing()
	w.setCollateAndBuild(4)
	w.setCollateAndBuild(6)
	w.unlockEditing()

	assert.Equal(t, 6, w.getCollateAndBuild(), "the last request wins, not the first")
}
