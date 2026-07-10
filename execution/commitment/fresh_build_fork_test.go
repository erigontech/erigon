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
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"
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

// freshWhaleAccountPlaneCorpus builds a whole-fresh corpus with a multi-way root branch (spread
// accounts under every top nibble) and one whale under nibble 0xd whose deeply-nested fresh storage
// spans the depth-64 seam and branches several levels below it — so a low-K fork-join splits inside the
// whale's storage plane, not only above the seam.
func freshWhaleAccountPlaneCorpus(seed int64, spread, whaleSlots int) ([][]byte, []Update) {
	rnd := rand.New(rand.NewSource(seed))
	ub := NewUpdateBuilder()
	base := int(seed) * 500_000
	for nib := range 16 {
		for s := range spread {
			a := addrHex(findAddressForNibble(nib, base+nib*1_000+s))
			ub.Balance(a, rnd.Uint64()+1)
		}
	}
	whale := addrHex(findAddressForNibble(0xd, base+424_242))
	ub.Balance(whale, rnd.Uint64()+1)
	for range whaleSlots {
		addRandomSlot(ub, rnd, whale)
	}
	return ub.Build()
}

// freshSeamSingleNibbleCorpus builds a whole-fresh corpus where, under every top nibble, one account
// carries storage confined to a single first storage nibble — the depth-64 ext seam (several slots
// under one first nibble, a single-nibble sub-branch) or inline seam (exactly one slot, held inline) —
// beside a balance-only sibling that keeps the top nibble a pure branch. The account-plane fork must
// reproduce both single-survivor seam encodings the sequential engine emits.
func freshSeamSingleNibbleCorpus(seed int64) ([][]byte, []Update) {
	rnd := rand.New(rand.NewSource(seed))
	ub := NewUpdateBuilder()
	base := int(seed) * 300_000
	for nib := range 16 {
		seamAddr := findAddressForNibble(nib, base+nib*7+1)
		seamHex := addrHex(seamAddr)
		ub.Balance(seamHex, rnd.Uint64()+1)
		if nib%2 == 0 {
			storNib := byte((nib + 3) & 0xf)
			for range 4 {
				l, v := genStorageSlotFirstNibble(rnd, seamAddr, storNib)
				ub.Storage(seamHex, l, v)
			}
		} else {
			addRandomSlot(ub, rnd, seamHex)
		}
		ub.Balance(addrHex(findAddressForNibble(nib, base+nib*7+2)), rnd.Uint64()+1)
	}
	return ub.Build()
}

// freshSeamSharedTopNibbleCorpus builds a whole-fresh corpus with perNibble accounts under every top
// nibble (deep shared-prefix account-plane branches), a mix of balance-only and small-storage accounts,
// so the account-plane fork folds real multi-level account branches that cross the seam inline.
func freshSeamSharedTopNibbleCorpus(seed int64, perNibble int) ([][]byte, []Update) {
	rnd := rand.New(rand.NewSource(seed))
	ub := NewUpdateBuilder()
	base := int(seed) * 400_000
	for nib := range 16 {
		for s := range perNibble {
			a := addrHex(findAddressForNibble(nib, base+nib*100+s))
			ub.Balance(a, rnd.Uint64()+1)
			if s%3 == 0 {
				for range 3 {
					addRandomSlot(ub, rnd, a)
				}
			}
		}
	}
	return ub.Build()
}

// runSeamParity folds a whole-fresh seam corpus through the sequential oracle and both the fork-on and
// fork-off (frontier-serial) engine, asserting flag-on == flag-off == sequential (root + stored
// branches) and that the account-plane fork-join fired.
func runSeamParity(t *testing.T, mode runMode, workers int, keys [][]byte, upds []Update) {
	t.Helper()
	seqMs := NewMockState(t)
	seqRoot, _ := processModeBatchState(t, seqMs, modeSeq, 0, keys, upds, nil)

	forkBefore := wholeFreshForkJoins.Load()
	onMs := NewMockState(t)
	onMs.SetConcurrentCommitment(true)
	onRoot, _ := processModeBatchState(t, onMs, mode, workers, keys, upds, nil)

	require.Equal(t, seqRoot, onRoot, "flag-on seam root != sequential")
	requireBranchParity(t, seqMs, onMs)
	require.Greater(t, wholeFreshForkJoins.Load(), forkBefore,
		"account-plane fork-join did not fire on the seam corpus (fell back to frontier)")

	setForkWholeFresh(t, false)
	offMs := NewMockState(t)
	offMs.SetConcurrentCommitment(true)
	offRoot, _ := processModeBatchState(t, offMs, mode, workers, keys, upds, nil)
	require.Equal(t, seqRoot, offRoot, "flag-off (frontier) seam root != sequential")
	requireBranchParity(t, seqMs, offMs)
}

// TestFreshBuild_AccountForkJoinSeamParity is the deterministic seam-crossing gate: a fresh account
// trie (multi-way account plane + a whale with deeply-nested storage) folds through the account-plane
// fork-join (ff.fold at depth 0) with an unbounded semaphore and a low K, so the fork provably splits
// below the depth-64 seam into the whale's fresh storage. It must reproduce the serial account fold and
// the sequential oracle byte-for-byte (state root + every stored branch across all planes) — proving
// each account's storage folds via the same recursion and its storage root is wired into the account
// cell before the branch keccak, rather than the seam degenerating to a plain nibble fork.
func TestFreshBuild_AccountForkJoinSeamParity(t *testing.T) {
	ctx := context.Background()
	pk, upds := freshWhaleAccountPlaneCorpus(3_003, 8, 50_000)
	root := freshAccountTrie(pk, upds)
	require.Empty(t, root.ext, "a multi-way fresh corpus root branches at depth 0")

	oracle := oracleRoot(t, pk, upds)
	oracleMs := seqFreshBranchOracle(t, pk, upds)
	requireSpansAllPlanes(t, oracleMs)

	srSerial, defSerial, err := foldFreshAccountRootDeferred(root)
	require.NoError(t, err)

	forkFoldMaxDepth.Store(0)
	fc := newFoldCtx(true)
	ff := &forkFolder{sem: semaphore.NewWeighted(1 << 20), k: 64}
	srPar, err := ff.fold(ctx, fc, root, nil, 0)
	require.NoError(t, err)
	defPar := fc.deferred
	fc.hph.Release()

	require.Equal(t, srSerial, srPar, "account-plane fork-join root != serial account fold")
	require.Equal(t, oracle, srPar[:], "account-plane fork-join state root != sequential oracle")
	require.Greater(t, forkFoldMaxDepth.Load(), int64(64),
		"low-K account-plane fork-join must split below the depth-64 seam into the whale's fresh storage")

	msSerial := NewMockState(t)
	_, err = ApplyDeferredBranchUpdates(defSerial, 1, msSerial.PutBranch)
	require.NoError(t, err)
	requireBranchParity(t, oracleMs, msSerial)

	msPar := NewMockState(t)
	_, err = ApplyDeferredBranchUpdates(defPar, 1, msPar.PutBranch)
	require.NoError(t, err)
	requireBranchParity(t, oracleMs, msPar)
}

// TestFreshBuild_Seam is the Task 3 seam-correctness gate through the real engine: each of the three
// seam shapes — an account whose fresh storage forks across the depth-64 seam, an account whose storage
// is confined to a single first nibble (ext / inline seam), and accounts sharing deep top-nibble
// prefixes — folds through the whole-fresh account-plane fork-join byte-for-byte identically to the
// sequential oracle (root + stored branches), fork-on == fork-off, and the fork-join fires.
func TestFreshBuild_Seam(t *testing.T) {
	scenarios := []struct {
		name    string
		workers int
		build   func() ([][]byte, []Update)
	}{
		{"storage_across_seam", 8, func() ([][]byte, []Update) { return freshWhaleAccountPlaneCorpus(51, 8, 12_000) }},
		{"single_first_nibble", 4, func() ([][]byte, []Update) { return freshSeamSingleNibbleCorpus(52) }},
		{"shared_top_nibble", 8, func() ([][]byte, []Update) { return freshSeamSharedTopNibbleCorpus(53, 12) }},
	}
	for _, sc := range scenarios {
		for _, mode := range parityModes {
			t.Run(fmt.Sprintf("%s/%s", sc.name, mode), func(t *testing.T) {
				keys, upds := sc.build()
				runSeamParity(t, mode, sc.workers, keys, upds)
			})
		}
	}
}
