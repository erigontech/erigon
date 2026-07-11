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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/common/length"
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
// Callers must not use t.Parallel: the toggle is process-global.
func setForkWholeFresh(t *testing.T, on bool) {
	t.Helper()
	prev := forkWholeFresh.Load()
	forkWholeFresh.Store(on)
	t.Cleanup(func() { forkWholeFresh.Store(prev) })
}

// runWholeFreshBatch folds one fresh batch (empty on-disk state) through the sequential oracle and
// the candidate engine, returning both roots and the number of Process calls that routed through the
// whole-fresh account-plane fork. It also asserts stored-branch byte parity between the two stores.
// routes is a delta on a process-global counter: callers asserting on it must not use t.Parallel.
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
// over a provably empty root wall (no seedable branch probed, no cell carried on the wall) and no
// root extension is whole-fresh, and the dev-only toggle gates the whole predicate.
func TestWholeFreshBuild_Detector(t *testing.T) {
	branchRoot := &prefixNode{bitmap: 0b11} // real multi-way root branch, no extension
	extRoot := &prefixNode{bitmap: 0b11, ext: []byte{1, 2}}
	singleChild := &prefixNode{bitmap: 0b100} // one first nibble: a propagate/common-prefix root
	loneLeaf := &prefixNode{bitmap: 0}        // no root branch (single account / propagate-folded root)

	emptyWall := func() *HexPatriciaHashed {
		base := NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig())
		seedRootBase(base)
		return base
	}
	leafWall := emptyWall() // a collapsed root: the wall carries a leaf cell, no branch record
	leafWall.afterMap[0] = 1 << 7

	t.Run("toggle_on", func(t *testing.T) {
		setForkWholeFresh(t, true)
		require.True(t, wholeFreshBuild(emptyWall(), branchRoot, false), "empty state + multi-way root branch is whole-fresh")
		require.False(t, wholeFreshBuild(emptyWall(), branchRoot, true), "a seedable branch is not fresh")
		require.False(t, wholeFreshBuild(leafWall, branchRoot, false), "a cell carried on the wall is not fresh")
		require.False(t, wholeFreshBuild(nil, branchRoot, false), "no wall proves nothing")
		require.False(t, wholeFreshBuild(NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig()), branchRoot, false),
			"an unformed wall (activeRows 0) proves nothing")
		require.False(t, wholeFreshBuild(emptyWall(), extRoot, false), "root extension routes to frontier")
		require.False(t, wholeFreshBuild(emptyWall(), singleChild, false), "single-first-nibble propagate root routes to frontier")
		require.False(t, wholeFreshBuild(emptyWall(), loneLeaf, false), "no root branch routes to frontier")
	})
	t.Run("toggle_off", func(t *testing.T) {
		setForkWholeFresh(t, false)
		require.False(t, wholeFreshBuild(emptyWall(), branchRoot, false), "toggle off never routes to whole-fresh")
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
// slice is additive — non-empty state keeps the frontier path. No t.Parallel: the route assertion
// is a zero delta on a process-global counter.
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

// collapsedRootThenFreshFanoutBatches: batch 1 is a single account (top nibble 7), so the on-disk
// root collapses to a leaf with no branch record at the root prefix — the same probe signature as
// empty state. Batch 2 leaves it untouched and adds fresh accounts under every top nibble: a gate
// that infers emptiness from probe absence misroutes to the whole-fresh fork and drops the carried
// leaf. Batch 3 re-touches the solo account (the harness asserts parity after every batch).
func collapsedRootThenFreshFanoutBatches() []engineBatch {
	solo := addrHex(findAddressForNibble(7, 31_000))
	ub := NewUpdateBuilder()
	ub.Balance(solo, 1_000_001)
	k1, u1 := ub.Build()

	ub2 := NewUpdateBuilder()
	for nib := range 16 {
		for s := range 2 {
			ub2.Balance(addrHex(findAddressForNibble(nib, 32_000+nib*8+s)), uint64(500+nib*16+s))
		}
	}
	k2, u2 := ub2.Build()

	ub3 := NewUpdateBuilder()
	ub3.Balance(solo, 1_000_002)
	k3, u3 := ub3.Build()
	return []engineBatch{{k1, u1}, {k2, u2}, {k3, u3}}
}

// TestFreshBuildFork_CollapsedRootNotWholeFresh pins that a root collapsed to a single leaf (no
// root branch record) is NOT treated as empty state: the fan-out batch must fold the carried leaf
// into the new root, byte-identical to the sequential oracle.
func TestFreshBuildFork_CollapsedRootNotWholeFresh(t *testing.T) {
	runParityOverModes(t, collapsedRootThenFreshFanoutBatches(), []int{1, 4})
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

// TestFreshBuild_WhaleStorageForksAtGrain is the Task 5 gate: at the *production* account-plane grain
// (foldK(root.subtreeCount, numWorkers)) a whale's fresh storage still forks below the depth-64 seam.
// On a whale-dominated build the account-plane grain is far coarser than the whale's per-first-nibble
// storage count, so the pre-Task-5 fork (which reused the account grain across the seam) folded the
// whale serially at numWorkers=NumCPU. The corpus is tuned so the whale's per-nibble storage count sits
// *below* the account grain (no fork on the old grain) but *above* the storage-local grain
// foldK(whaleStorage, numWorkers) (fork on the new grain), so forkFoldMaxDepth reaching the storage
// plane (>= 64) proves the per-subtree grain engaged — while the root stays byte-identical to serial.
func TestFreshBuild_WhaleStorageForksAtGrain(t *testing.T) {
	ctx := context.Background()
	const workers = 24
	pk, upds := freshWhaleAccountPlaneCorpus(5_005, 1_500, 32_000)
	root := freshAccountTrie(pk, upds)
	require.Empty(t, root.ext, "a multi-way fresh corpus root branches at depth 0")

	k := foldK(root.subtreeCount, workers)
	require.Greater(t, k, uint32(32_000/16),
		"corpus mis-tuned: account grain must exceed the whale's per-nibble storage count (no fork on the old grain)")
	require.Less(t, foldK(32_000, workers), uint32(32_000/16),
		"corpus mis-tuned: storage-local grain must fall below the whale's per-nibble storage count (fork on the new grain)")

	oracle := oracleRoot(t, pk, upds)
	srSerial, _, err := foldFreshAccountRootDeferred(root)
	require.NoError(t, err)

	forkFoldMaxDepth.Store(0)
	fc := newFoldCtx(true)
	ff := &forkFolder{sem: semaphore.NewWeighted(1 << 20), k: k, numWorkers: workers}
	srPar, err := ff.fold(ctx, fc, root, nil, 0)
	require.NoError(t, err)
	fc.hph.Release()

	require.Equal(t, srSerial, srPar, "per-subtree-grain fork-join root != serial account fold")
	require.Equal(t, oracle, srPar[:], "per-subtree-grain fork-join root != sequential oracle")
	require.GreaterOrEqual(t, forkFoldMaxDepth.Load(), int64(64),
		"whale storage did not fork below the depth-64 seam at the production account grain (per-subtree grain not engaged)")
}

// TestFreshBuild_ForkFloorParity pins the interior fork-floor gate: with k just below the corpus
// (so no child clears the k gate) a sub-k forkFloor is the only source of forks, and the fold
// must stay byte-identical to the serial fold and the sequential oracle (root + stored branches).
// The gate-at-k control (forkFloor pinned to k) proves the floor is what created the forks.
func TestFreshBuild_ForkFloorParity(t *testing.T) {
	ctx := context.Background()
	pk, upds := freshWhaleAccountPlaneCorpus(3_003, 8, 50_000)
	root := freshAccountTrie(pk, upds)
	require.Empty(t, root.ext, "a multi-way fresh corpus root branches at depth 0")
	k := root.subtreeCount - 1

	oracle := oracleRoot(t, pk, upds)
	oracleMs := seqFreshBranchOracle(t, pk, upds)
	srSerial, defSerial, err := foldFreshAccountRootDeferred(root)
	require.NoError(t, err)
	msSerial := NewMockState(t)
	_, err = ApplyDeferredBranchUpdates(defSerial, 1, msSerial.PutBranch)
	require.NoError(t, err)
	requireBranchParity(t, oracleMs, msSerial)

	forkFoldMaxDepth.Store(-1)
	fc0 := newFoldCtx(true)
	ff0 := &forkFolder{sem: semaphore.NewWeighted(1 << 20), k: k, forkFloor: k}
	srCtl, err := ff0.fold(ctx, fc0, root, nil, 0)
	require.NoError(t, err)
	putDeferredUpdates(fc0.deferred)
	fc0.hph.Release()
	require.Equal(t, srSerial, srCtl, "gate-at-k control root != serial account fold")
	require.Equal(t, int64(-1), forkFoldMaxDepth.Load(),
		"test setup: gate-at-k control must not fork (children must sit below k)")

	forkFoldMaxDepth.Store(-1)
	fc := newFoldCtx(true)
	ff := &forkFolder{sem: semaphore.NewWeighted(1 << 20), k: k, forkFloor: 256}
	srPar, err := ff.fold(ctx, fc, root, nil, 0)
	require.NoError(t, err)
	defPar := fc.deferred
	fc.hph.Release()
	require.GreaterOrEqual(t, forkFoldMaxDepth.Load(), int64(0),
		"fork floor did not create forks (gate not engaged)")

	require.Equal(t, srSerial, srPar, "fork-floor root != serial account fold")
	require.Equal(t, oracle, srPar[:], "fork-floor state root != sequential oracle")
	msPar := NewMockState(t)
	_, err = ApplyDeferredBranchUpdates(defPar, 1, msPar.PutBranch)
	require.NoError(t, err)
	requireBranchParity(t, oracleMs, msPar)
}

// nibFoldRendezvous pins top-nibble fold overlap deterministically: each fold entry blocks until a
// second entrant arrives, so a serial dispatch (one nibble in flight at a time) strands the first
// entrant until the timeout, while a concurrent dispatch releases as soon as two folds run
// together. The timeout arm records failure and returns, so Process always completes and the test
// asserts — it never hangs or skips.
type nibFoldRendezvous struct {
	arrived     atomic.Int32
	release     chan struct{}
	releaseOnce sync.Once
	timedOut    atomic.Bool
	timeout     time.Duration
}

func newNibFoldRendezvous(timeout time.Duration) *nibFoldRendezvous {
	return &nibFoldRendezvous{release: make(chan struct{}), timeout: timeout}
}

func (r *nibFoldRendezvous) enter(int) {
	if r.arrived.Add(1) >= 2 {
		r.releaseOnce.Do(func() { close(r.release) })
	}
	select {
	case <-r.release:
	case <-time.After(r.timeout):
		// both arms ready must count as a release, not a timeout
		select {
		case <-r.release:
		default:
			r.timedOut.Store(true)
		}
	}
}

// TestFreshBuild_TopNibbleFoldsOverlap is the top-level dispatch concurrency gate: at least two
// top-nibble folds must execute concurrently — the rendezvous releases only once a second entrant
// arrives, so a serial dispatch deterministically times out. Also asserts the fork-join
// route fired, so a corpus that slips to the frontier fallback reads as a setup failure rather
// than a false verdict. No t.Parallel: the hook and route counters are process-global.
func TestFreshBuild_TopNibbleFoldsOverlap(t *testing.T) {
	keys, upds := freshAccountPlaneCorpus(7, 6, 400)

	r := newNibFoldRendezvous(5 * time.Second)
	onNibFoldStart = r.enter
	t.Cleanup(func() { onNibFoldStart = nil })

	forkBefore := wholeFreshForkJoins.Load()
	ms := NewMockState(t)
	ms.SetConcurrentCommitment(true)
	processModeBatchState(t, ms, modeStreaming, 4, keys, upds, nil)

	require.Greater(t, wholeFreshForkJoins.Load(), forkBefore,
		"test setup: whole-fresh fork-join did not fire (fell back to frontier)")
	require.GreaterOrEqual(t, r.arrived.Load(), int32(2),
		"test setup: fewer than two top-nibble folds executed")
	require.False(t, r.timedOut.Load(),
		"top-nibble folds never overlapped: whole-fresh dispatch is serial")
}

// TestFreshBuild_TopNibbleDispatchParity pins byte-identity for the corpus the overlap gate
// drives: fork-on and fork-off both fold root- and stored-branch-identical to the sequential
// oracle, and the fork-join fires, across engines and worker counts.
func TestFreshBuild_TopNibbleDispatchParity(t *testing.T) {
	for _, mode := range parityModes {
		for _, w := range []int{1, 4} {
			t.Run(fmt.Sprintf("%s/w%d", mode, w), func(t *testing.T) {
				keys, upds := freshAccountPlaneCorpus(7, 6, 400)
				runSeamParity(t, mode, w, keys, upds)
			})
		}
	}
}

// TestFreshBuild_TopNibbleDispatchDeadlockFree pins the TryAcquire-then-inline contract of the
// top-nibble dispatch under full semaphore saturation: at foldSem sizes 1 and 2 a forked nibble
// holds the only slot(s) while the rest must fall back to inline folds, so a blocking acquire
// anywhere in the dispatch would wedge exactly this configuration. Each run must complete and
// reproduce the serial per-subtree mount-wall cells.
func TestFreshBuild_TopNibbleDispatchDeadlockFree(t *testing.T) {
	ctx := context.Background()
	keys, upds := freshAccountPlaneCorpus(11, 40, 4_000)
	root := freshAccountTrie(keys, upds)
	require.Empty(t, root.ext, "a multi-way fresh corpus root branches at depth 0")
	rootTask := deriveFoldFrontier(root, foldK(root.subtreeCount, 8), func([]byte) bool { return false })
	require.Len(t, rootTask.children, 16, "corpus must touch all 16 top nibbles")

	var expected [16]cell
	for _, top := range rootTask.children {
		c, d, err := foldFreshAccountSubtreeCellDeferred(top.node, rootTask.prefix, top.nib)
		require.NoError(t, err)
		putDeferredUpdates(d)
		expected[top.nib] = c
	}

	for _, slots := range []int64{1, 2} {
		t.Run(fmt.Sprintf("sem%d", slots), func(t *testing.T) {
			ff := &forkFolder{sem: semaphore.NewWeighted(slots), k: 64, numWorkers: int(slots)}
			type dispatchResult struct {
				cells    [16]cell
				present  [16]bool
				deferred []*DeferredBranchUpdate
				err      error
			}
			done := make(chan dispatchResult, 1)
			go func() {
				var r dispatchResult
				r.cells, r.present, r.deferred, r.err = foldTopNibblesForkJoin(ctx, ff, rootTask.children, rootTask.prefix)
				done <- r
			}()
			select {
			case r := <-done:
				require.NoError(t, r.err)
				putDeferredUpdates(r.deferred)
				for _, top := range rootTask.children {
					require.True(t, r.present[top.nib], "nibble %x missing from the dispatch result", top.nib)
					require.Equal(t, expected[top.nib], r.cells[top.nib], "nibble %x cell != serial per-subtree fold", top.nib)
				}
			case <-time.After(2 * time.Minute):
				t.Fatal("top-nibble dispatch wedged under foldSem saturation")
			}
		})
	}
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
