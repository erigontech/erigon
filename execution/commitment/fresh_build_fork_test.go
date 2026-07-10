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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// setForkWholeFresh flips the dev-only whole-fresh fork toggle for the duration of a test and
// restores it on cleanup, so a test can measure the frontier-serial baseline (toggle off).
func setForkWholeFresh(t *testing.T, on bool) {
	t.Helper()
	prev := forkWholeFresh.Load()
	forkWholeFresh.Store(on)
	t.Cleanup(func() { forkWholeFresh.Store(prev) })
}

// runWholeFreshBatch folds one fresh batch (empty on-disk state) through the sequential oracle and
// the candidate engine, returning both roots and the number of Process calls that routed through the
// whole-fresh account-plane fork. It also asserts stored-branch byte parity between the two stores.
func runWholeFreshBatch(t *testing.T, mode runMode, workers int, keys [][]byte, upds []Update) (candRoot, seqRoot []byte, routes int64) {
	t.Helper()
	seqMs := NewMockState(t)
	candMs := NewMockState(t)
	candMs.SetConcurrentCommitment(true)

	seqRoot, _ = processModeBatchState(t, seqMs, modeSeq, 0, keys, upds, nil)

	before := wholeFreshFolds.Load()
	candRoot, _ = processModeBatchState(t, candMs, mode, workers, keys, upds, nil)
	routes = wholeFreshFolds.Load() - before

	requireBranchParity(t, seqMs, candMs)
	return candRoot, seqRoot, routes
}

// TestWholeFreshBuild_Detector pins the empty-state detector in isolation: only a real root branch
// with no on-disk state (rootSeedable and sawSeedable both false) and no root extension is whole-fresh,
// and the dev-only toggle gates the whole predicate.
func TestWholeFreshBuild_Detector(t *testing.T) {
	branchRoot := &prefixNode{bitmap: 0b11} // real root branch, no extension
	extRoot := &prefixNode{bitmap: 0b11, ext: []byte{1, 2}}
	loneLeaf := &prefixNode{bitmap: 0} // no root branch (single account / propagate-folded root)

	t.Run("toggle_on", func(t *testing.T) {
		setForkWholeFresh(t, true)
		require.True(t, wholeFreshBuild(branchRoot, false, false), "empty state + root branch is whole-fresh")
		require.False(t, wholeFreshBuild(branchRoot, true, false), "present root branch is not fresh")
		require.False(t, wholeFreshBuild(branchRoot, false, true), "a seedable deeper branch is not fresh")
		require.False(t, wholeFreshBuild(extRoot, false, false), "root extension routes to frontier")
		require.False(t, wholeFreshBuild(loneLeaf, false, false), "no root branch routes to frontier")
	})
	t.Run("toggle_off", func(t *testing.T) {
		setForkWholeFresh(t, false)
		require.False(t, wholeFreshBuild(branchRoot, false, false), "toggle off never routes to whole-fresh")
	})
}

// TestFreshBuildFork_FreshRoutesToWholeFresh proves a whole-fresh build routes through the new
// whole-fresh entry (counter fires) while staying root- and branch-identical to the sequential
// oracle. Task 1's entry delegates to the frontier fold, so parity is expected; the counter is the
// evidence the new path executed.
func TestFreshBuildFork_FreshRoutesToWholeFresh(t *testing.T) {
	b0 := balancedBatches()[0]
	for _, mode := range parityModes {
		for _, w := range []int{1, 4} {
			t.Run(fmt.Sprintf("%s/w%d", mode, w), func(t *testing.T) {
				cand, seq, routes := runWholeFreshBatch(t, mode, w, b0.keys, b0.upds)
				require.Equal(t, seq, cand, "whole-fresh route must match the sequential root")
				require.Positive(t, routes, "a fresh build must route through the whole-fresh fork")
			})
		}
	}
}

// TestFreshBuildFork_IncrementalStaysFrontier is the guard test: once batch 1 has built on-disk
// state, batch 2's incremental commit must NOT route through the whole-fresh fork (the on-disk root
// branch makes it unfit) and must stay root- and branch-identical to sequential. This proves the
// slice is additive — non-empty state keeps the frontier path.
func TestFreshBuildFork_IncrementalStaysFrontier(t *testing.T) {
	batches := balancedBatches()
	for _, mode := range parityModes {
		for _, w := range []int{1, 4} {
			t.Run(fmt.Sprintf("%s/w%d", mode, w), func(t *testing.T) {
				seqMs := NewMockState(t)
				candMs := NewMockState(t)
				candMs.SetConcurrentCommitment(true)

				var seqBlob, candBlob []byte
				_, seqBlob = processModeBatchState(t, seqMs, modeSeq, 0, batches[0].keys, batches[0].upds, seqBlob)
				_, candBlob = processModeBatchState(t, candMs, mode, w, batches[0].keys, batches[0].upds, candBlob)

				before := wholeFreshFolds.Load()
				seqRoot, _ := processModeBatchState(t, seqMs, modeSeq, 0, batches[1].keys, batches[1].upds, seqBlob)
				candRoot, _ := processModeBatchState(t, candMs, mode, w, batches[1].keys, batches[1].upds, candBlob)
				routes := wholeFreshFolds.Load() - before

				require.Zero(t, routes, "an incremental build must NOT route through the whole-fresh fork")
				require.Equal(t, seqRoot, candRoot, "incremental root must match the sequential root")
				requireBranchParity(t, seqMs, candMs)
			})
		}
	}
}

// TestFreshBuildFork_ToggleOffIsFrontier pins that the dev-only toggle disables the whole-fresh
// route entirely: a fresh build with the toggle off never routes through the fork and stays identical
// to the sequential (frontier-serial baseline) root.
func TestFreshBuildFork_ToggleOffIsFrontier(t *testing.T) {
	b0 := balancedBatches()[0]
	setForkWholeFresh(t, false)
	for _, mode := range parityModes {
		for _, w := range []int{1, 4} {
			t.Run(fmt.Sprintf("%s/w%d", mode, w), func(t *testing.T) {
				cand, seq, routes := runWholeFreshBatch(t, mode, w, b0.keys, b0.upds)
				require.Zero(t, routes, "toggle-off must not route through the whole-fresh fork")
				require.Equal(t, seq, cand, "toggle-off must match the sequential root (== frontier)")
			})
		}
	}
}
