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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// freshAccountPlaneCorpus builds a whole-fresh corpus whose every top nibble is a branch (so the
// account-plane fork-join fires rather than the frontier fallback): perNibble tail accounts under
// each of the 16 nibbles, some carrying multi-nibble storage (depth-64 seam branches), plus one whale
// sharing top nibble 0xd whose large fresh storage forces the fork-join to fan out toward the seam.
func freshAccountPlaneCorpus(seed int64, perNibble, whaleSlots int) ([][]byte, []Update) {
	rnd := rand.New(rand.NewSource(seed))
	ub := NewUpdateBuilder()
	base := int(seed) * 100_000
	for nib := range 16 {
		for s := range perNibble {
			a := addrHex(findAddressForNibble(nib, base+nib*1_000+s))
			ub.Balance(a, rnd.Uint64()+1)
			if s%5 == 0 {
				for range 6 {
					addRandomSlot(ub, rnd, a)
				}
			}
		}
	}
	whale := addrHex(findAddressForNibble(0xd, base+999_999))
	ub.Balance(whale, rnd.Uint64()+1)
	for range whaleSlots {
		addRandomSlot(ub, rnd, whale)
	}
	return ub.Build()
}

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
	branchRoot := &prefixNode{bitmap: 0b11} // real multi-way root branch, no extension
	extRoot := &prefixNode{bitmap: 0b11, ext: []byte{1, 2}}
	singleChild := &prefixNode{bitmap: 0b100} // one first nibble: a propagate/common-prefix root
	loneLeaf := &prefixNode{bitmap: 0}        // no root branch (single account / propagate-folded root)

	t.Run("toggle_on", func(t *testing.T) {
		setForkWholeFresh(t, true)
		require.True(t, wholeFreshBuild(branchRoot, false, false), "empty state + multi-way root branch is whole-fresh")
		require.False(t, wholeFreshBuild(branchRoot, true, false), "present root branch is not fresh")
		require.False(t, wholeFreshBuild(branchRoot, false, true), "a seedable deeper branch is not fresh")
		require.False(t, wholeFreshBuild(extRoot, false, false), "root extension routes to frontier")
		require.False(t, wholeFreshBuild(singleChild, false, false), "single-first-nibble propagate root routes to frontier")
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

// TestFreshBuild_AccountPlane is the Task 2 parity gate: a whole-fresh corpus (tail accounts across
// every top nibble + multi-nibble storage seams + a whale) folds through the account-plane fork-join
// byte-for-byte identically to the sequential oracle (root + stored branches), the fork-join route
// actually fired (not the frontier fallback), and it fanned out below the top nibble. The same corpus
// with the toggle off (frontier-serial baseline) also matches sequential, proving the slice additive.
func TestFreshBuild_AccountPlane(t *testing.T) {
	cfgs := []struct {
		seed       int64
		workers    int
		perNibble  int
		whaleSlots int
	}{
		{1, 4, 24, 5_000},
		{2, 8, 28, 9_000},
		{3, 16, 20, 7_000},
	}
	for _, mode := range parityModes {
		for _, c := range cfgs {
			t.Run(fmt.Sprintf("%s/seed%d/w%d", mode, c.seed, c.workers), func(t *testing.T) {
				keys, upds := freshAccountPlaneCorpus(c.seed, c.perNibble, c.whaleSlots)

				seqMs := NewMockState(t)
				seqRoot, _ := processModeBatchState(t, seqMs, modeSeq, 0, keys, upds, nil)

				forkBefore := wholeFreshForkJoins.Load()
				forkFoldMaxDepth.Store(0)
				onMs := NewMockState(t)
				onMs.SetConcurrentCommitment(true)
				onRoot, _ := processModeBatchState(t, onMs, mode, c.workers, keys, upds, nil)

				require.Equal(t, seqRoot, onRoot, "flag-on whole-fresh root != sequential")
				requireBranchParity(t, seqMs, onMs)
				require.Greater(t, wholeFreshForkJoins.Load(), forkBefore,
					"whole-fresh account-plane fork-join did not fire (fell back to frontier)")
				// The eager scheduler may pre-fold (and thus reuse) the whale's top nibble, so the
				// fan-out counter is only reliable on the non-scheduled engines.
				if mode != modeStreamingScheduled {
					require.Positive(t, forkFoldMaxDepth.Load(),
						"account-plane fork-join did not fan out below the top nibble")
				}

				setForkWholeFresh(t, false)
				offMs := NewMockState(t)
				offMs.SetConcurrentCommitment(true)
				offRoot, _ := processModeBatchState(t, offMs, mode, c.workers, keys, upds, nil)
				require.Equal(t, seqRoot, offRoot, "flag-off (frontier) whole-fresh root != sequential")
				requireBranchParity(t, seqMs, offMs)
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
