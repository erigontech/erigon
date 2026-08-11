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
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/cache"
)

// generateStandaloneIIFile writes files with a hardcoded step size of 10.
const alignStepSize = 10

func generateStateFiles(t *testing.T, dirs datadir.Dirs, ranges []testFileRange) {
	t.Helper()
	generateAccountsFile(t, dirs, ranges)
	generateStorageFile(t, dirs, ranges)
	generateCodeFile(t, dirs, ranges)
}

func generateStandaloneIIFiles(t *testing.T, dirs datadir.Dirs, ranges []testFileRange) {
	t.Helper()
	for _, name := range []kv.InvertedIdx{kv.LogAddrIdx, kv.LogTopicIdx, kv.TracesFromIdx, kv.TracesToIdx} {
		generateStandaloneIIFile(t, name, dirs, ranges)
	}
}

func requireVisibleEnd(t *testing.T, agg *Aggregator, end uint64) {
	t.Helper()
	at := agg.BeginFilesRo()
	defer at.Close()
	for _, d := range kv.StateDomains {
		require.EqualValues(t, end, at.d[d].files.EndTxNum(), "domain %s", d)
	}
	for _, ii := range at.standaloneIIs() {
		require.EqualValues(t, end, ii.files.EndTxNum(), "index %s", ii.name)
	}
}

type cacheAggregatorHolder struct{ agg *Aggregator }

func (h cacheAggregatorHolder) Agg() any { return h.agg }

func bindTestStateCache(t *testing.T, agg *Aggregator) {
	t.Helper()
	stateCache := cache.NewStateCache(1<<20, 1<<20, 1<<20, 1<<20)
	t.Cleanup(stateCache.Close)
	agg.BindStateCache(stateCache)
}

// state visible past commitment's files = state no commitment covers
func TestVisibleFilesAligned_LaggingCommitmentClampsEveryone(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, alignStepSize)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateStandaloneIIFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}})
	require.NoError(t, agg.OpenFolder())

	requireVisibleEnd(t, agg, alignStepSize)
}

// domains visible past the log/trace indexes answer eth_getLogs from blocks no index saw
func TestVisibleFilesAligned_LaggingStandaloneIdxClampsEveryone(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, alignStepSize)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	for _, name := range []kv.InvertedIdx{kv.LogTopicIdx, kv.TracesFromIdx, kv.TracesToIdx} {
		generateStandaloneIIFile(t, name, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	}
	generateStandaloneIIFile(t, kv.LogAddrIdx, agg.Dirs(), []testFileRange{{0, 1}})
	require.NoError(t, agg.OpenFolder())

	requireVisibleEnd(t, agg, alignStepSize)
}

// no files at all = nothing to be misaligned with, so the domains stay visible
func TestVisibleFilesAligned_EntityWithoutFilesDoesNotClamp(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, alignStepSize)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	require.NoError(t, agg.OpenFolder())

	at := agg.BeginFilesRo()
	defer at.Close()
	for _, d := range kv.StateDomains {
		require.EqualValues(t, 2*alignStepSize, at.d[d].files.EndTxNum(), "domain %s", d)
	}
}

// stage_custom_trace re-executes blocks to rebuild receipts, and reads the state while
// doing it
func TestUnalign_KeepsStateVisibleWhileReceiptsLag(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, alignStepSize)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateStandaloneIIFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateDomainFiles(t, "receipt", agg.Dirs(), []testFileRange{{0, 1}})
	require.NoError(t, agg.OpenFolder())

	requireVisibleEnd(t, agg, alignStepSize)

	realign := agg.Unalign(kv.ReceiptDomain)
	at := agg.BeginFilesRo()
	for _, d := range kv.StateDomains {
		require.EqualValues(t, 2*alignStepSize, at.d[d].files.EndTxNum(), "domain %s", d)
	}
	at.Close()

	realign()
	requireVisibleEnd(t, agg, alignStepSize)
}

// commitment rebuild reads accounts and storage while commitment covers only what it
// rebuilt so far
func TestUnalign_KeepsStateVisibleWhileCommitmentLags(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, alignStepSize)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateStandaloneIIFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}})
	require.NoError(t, agg.OpenFolder())

	agg.DisableAllDependencies() // what the rebuild does about file ranges
	realign := agg.Unalign(kv.CommitmentDomain)

	at := agg.BeginFilesRo()
	for _, d := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
		require.EqualValues(t, 2*alignStepSize, at.d[d].files.EndTxNum(), "domain %s", d)
	}
	// it may lag what it is rebuilt from, never lead it
	require.EqualValues(t, alignStepSize, at.d[kv.CommitmentDomain].files.EndTxNum())
	at.Close()

	realign()
	requireVisibleEnd(t, agg, alignStepSize)
}

// the state domains can not lag: every rebuild reads them
func TestUnalign_RejectsStateDomain(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, alignStepSize)

	for _, d := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
		require.Panics(t, func() { agg.Unalign(d) }, "domain %s", d)
	}
}

// Fill admission relies on view frontiers never decreasing. Raising
// visibility (unaligning a lagging entity) is allowed even on a forbidden
// aggregator; the transition that lowers a cached state domain's visible end
// (here: realigning while receipt still lags, which drops the shared ceiling)
// must panic, whichever entry point caused it.
func TestVisibilityLowering_StateCachePanicsOnStateLoweringOnly(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, alignStepSize)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateStandaloneIIFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateDomainFiles(t, "receipt", agg.Dirs(), []testFileRange{{0, 1}})
	require.NoError(t, agg.OpenFolder())

	bindTestStateCache(t, agg)
	realign := agg.Unalign(kv.ReceiptDomain) // raises the ceiling: allowed
	require.Panics(t, func() { realign() }, "realigning a still-lagging receipt lowers the state domains' ends")
}

func TestCloseDirtyFilesNoReopenRestoresVisibilityLoweringGuard(t *testing.T) {
	t.Parallel()

	for _, initiallyForbidden := range []bool{false, true} {
		t.Run(fmt.Sprintf("initially_forbidden_%t", initiallyForbidden), func(t *testing.T) {
			_, agg := testDbAndAggregatorv3(t, alignStepSize)
			if initiallyForbidden {
				bindTestStateCache(t, agg)
			}

			agg.closeDirtyFilesNoReopen()

			require.Equal(t, initiallyForbidden, agg.visibilityLoweringForbidden.Load())
		})
	}
}

// craftedClampedVisible replaces the current visible bundle with one where
// every state domain's values files end one segment below its history-II end
// — the divergence a dependency checker produces when a dependent file is
// missing.
func craftedClampedVisible(t *testing.T, agg *Aggregator) {
	t.Helper()
	agg.dirtyFilesLock.Lock()
	defer agg.dirtyFilesLock.Unlock()
	v := agg.visible.Load()
	crafted := &aggregatorVisible{minimaxTxNum: v.minimaxTxNum}
	crafted.d, crafted.dh, crafted.dhii, crafted.iis = v.d, v.dh, v.dhii, v.iis
	for _, dom := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain} {
		files := v.d[dom].files
		require.GreaterOrEqual(t, len(files), 2)
		crafted.d[dom] = newDomainVisible(dom, files[:len(files)-1])
	}
	v.next = crafted
	agg.visible.Store(crafted)
}

// A dependency-clamped values view has no exact frontier: reads mix fresh
// DB-resident keys with older file values for gap keys, and raising the
// dependent file's visibility later reveals state without any cache apply —
// nothing would invalidate a fill made during the clamp. DomainVisibleEnd
// must report ok=false so such views never fill.
func TestDomainVisibleEnd_ClampedViewHasNoExactFrontier(t *testing.T) {
	t.Parallel()
	db, agg := testDbAndAggregatorv3(t, alignStepSize)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateStandaloneIIFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	require.NoError(t, agg.OpenFolder())

	craftedClampedVisible(t, agg)

	at := agg.BeginFilesRo()
	defer at.Close()
	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	_, ok := at.DomainVisibleEnd(kv.AccountsDomain, tx)
	require.False(t, ok, "a dependency-clamped values view has no exact frontier")
	require.False(t, at.HasExactDomainVisibleEnd(kv.AccountsDomain))
}

// The forbid assert must also watch the history-II ends: they are the base of
// what DomainVisibleEnd reports, and with values dependency-clamped below the
// ceiling they can lower while every values end stays put.
func TestVisibilityLowering_StateCacheGuardsHistoryIIEnd(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, alignStepSize)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateStandaloneIIFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	require.NoError(t, agg.OpenFolder())

	craftedClampedVisible(t, agg)
	bindTestStateCache(t, agg)

	// Drop the accounts history-II {1,2} segment in memory rather than from
	// disk (Windows forbids removing a mapped file): the recalculation lowers
	// the ii end while every values end stays put.
	agg.dirtyFilesLock.Lock()
	defer agg.dirtyFilesLock.Unlock()
	dropped := 0
	agg.d[kv.AccountsDomain].History.InvertedIndex.dirtyFiles.CloseIf(func(item *FilesItem) bool {
		if item.endTxNum == 2*alignStepSize {
			dropped++
			return true
		}
		return false
	})
	require.Equal(t, 1, dropped)

	require.Panics(t, func() { agg.recalcVisibleFiles(nil) },
		"lowering a history-II end while values ends stay put must trip the forbid assert")
}

func TestVisibilityLowering_StateCacheGuardAllowsCommitmentDomain(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, alignStepSize)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateStandaloneIIFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	require.NoError(t, agg.OpenFolder())

	agg.DisableAllDependencies()
	agg.Unalign(kv.CommitmentDomain)
	bindTestStateCache(t, agg)
	branchCache := agg.d[kv.CommitmentDomain].branchCache
	require.NotNil(t, branchCache)
	publisher := branchCache.Publisher()
	publisher.Initialize(cache.BranchGeneration(1, 2*alignStepSize))
	key := []byte{0x01}
	view := branchCache.View(cache.BranchGeneration(1, 2*alignStepSize))
	view.Fill(key, []byte{0xbb}, 1)
	_, _, ok := view.Get(key)
	require.True(t, ok)

	agg.dirtyFilesLock.Lock()
	defer agg.dirtyFilesLock.Unlock()
	dropped := 0
	agg.d[kv.CommitmentDomain].dirtyFiles.CloseIf(func(item *FilesItem) bool {
		if item.endTxNum == 2*alignStepSize {
			dropped++
			return true
		}
		return false
	})
	require.Equal(t, 1, dropped)

	require.NotPanics(t, func() { agg.recalcVisibleFiles(nil) },
		"the StateCache guard must not reject a safe BranchCache reset")
	_, _, ok = view.Get(key)
	require.False(t, ok, "the old commitment-files generation must be revoked")
	_, _, ok = branchCache.View(cache.BranchGeneration(1, alignStepSize)).Get(key)
	require.False(t, ok, "branches from the removed commitment file must be cleared")
}

func TestFilePublicationRevokesCacheGenerations(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, alignStepSize)

	stateCache := cache.NewStateCache(1<<20, 1<<20, 1<<20, 1<<20)
	t.Cleanup(stateCache.Close)
	statePublisher := stateCache.Publisher()
	statePublisher.Initialize(cache.StateGeneration(1, 0, 0, 0))
	execctx.BindStateCacheToAggregator(cacheAggregatorHolder{agg}, stateCache)

	accountKey := make([]byte, 20)
	accountKey[0] = 1
	stateView := stateCache.View(cache.StateGeneration(1, 0, 0, 0))
	stateView.Fill(kv.AccountsDomain, accountKey, []byte{1}, 1)
	_, ok := stateView.Get(kv.AccountsDomain, accountKey)
	require.True(t, ok)

	branchCache := agg.d[kv.CommitmentDomain].branchCache
	require.NotNil(t, branchCache)
	branchPublisher := branchCache.Publisher()
	branchPublisher.Initialize(cache.BranchGeneration(1, 0))
	branchKey := []byte{0x01}
	branchView := branchCache.View(cache.BranchGeneration(1, 0))
	branchView.Fill(branchKey, []byte{0xbb}, 1)
	_, _, ok = branchView.Get(branchKey)
	require.True(t, ok)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateStandaloneIIFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	require.NoError(t, agg.OpenFolder())

	_, ok = stateView.Get(kv.AccountsDomain, accountKey)
	require.False(t, ok, "file publication must revoke pre-publication state views")
	stateView.Fill(kv.AccountsDomain, accountKey, []byte{1}, 1)
	statePublisher.Initialize(cache.StateGeneration(1, 2*alignStepSize, 2*alignStepSize, 2*alignStepSize))
	_, ok = stateCache.View(cache.StateGeneration(1, 2*alignStepSize, 2*alignStepSize, 2*alignStepSize)).Get(kv.AccountsDomain, accountKey)
	require.False(t, ok, "a revoked state view must not refill after file publication")

	_, _, ok = branchView.Get(branchKey)
	require.False(t, ok, "file publication must revoke pre-publication branch views")
	branchView.Fill(branchKey, []byte{0xbb}, 1)
	branchPublisher.Initialize(cache.BranchGeneration(1, 2*alignStepSize))
	_, _, ok = branchCache.View(cache.BranchGeneration(1, 2*alignStepSize)).Get(branchKey)
	require.False(t, ok, "a revoked branch view must not refill after file publication")
}

func TestCacheBindingAbsorbsExistingFileVisibility(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, alignStepSize)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateStandaloneIIFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	require.NoError(t, agg.OpenFolder())

	stateCache := cache.NewStateCache(1<<20, 1<<20, 1<<20, 1<<20)
	t.Cleanup(stateCache.Close)
	statePublisher := stateCache.Publisher()
	statePublisher.Initialize(cache.StateGeneration(1, 0, 0, 0))
	accountKey := make([]byte, 20)
	accountKey[0] = 1
	oldView := stateCache.View(cache.StateGeneration(1, 0, 0, 0))
	oldView.Fill(kv.AccountsDomain, accountKey, []byte{1}, 1)
	_, ok := oldView.Get(kv.AccountsDomain, accountKey)
	require.True(t, ok)

	execctx.BindStateCacheToAggregator(cacheAggregatorHolder{agg}, stateCache)

	_, ok = oldView.Get(kv.AccountsDomain, accountKey)
	require.False(t, ok, "binding must revoke entries created before the visible files were absorbed")
	statePublisher.Initialize(cache.StateGeneration(1, 2*alignStepSize, 2*alignStepSize, 2*alignStepSize))
	_, ok = stateCache.View(cache.StateGeneration(1, 2*alignStepSize, 2*alignStepSize, 2*alignStepSize)).Get(kv.AccountsDomain, accountKey)
	require.False(t, ok)
}
