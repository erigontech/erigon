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
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/state/statecfg"
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

// state visible past commitment's files = state no commitment covers
func TestVisibleFilesAligned_LaggingCommitmentClampsEveryone(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg := testDbAndAggregatorv3(t, alignStepSize)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateStandaloneIIFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}})
	require.NoError(t, agg.OpenFolder(db))

	requireVisibleEnd(t, agg, alignStepSize)
}

// a gap in one domain's files lowers every list's visible end
func TestVisibleFilesAligned_GapInOneListLowersEveryList(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg := testDbAndAggregatorv3(t, alignStepSize)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateStandaloneIIFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	require.NoError(t, dir.RemoveFile(filepath.Join(agg.Dirs().SnapDomain, "v1.0-accounts.1-2.bt")))
	require.NoError(t, agg.OpenFolder(db))

	requireVisibleEnd(t, agg, alignStepSize)
}

// a gap in the middle of one domain's files lowers every list's visible end to the gap
func TestVisibleFilesAligned_MiddleGapInOneListLowersEveryList(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg := testDbAndAggregatorv3(t, alignStepSize)

	ranges := []testFileRange{{0, 1}, {1, 2}, {2, 3}}
	generateStateFiles(t, agg.Dirs(), ranges)
	generateStandaloneIIFiles(t, agg.Dirs(), ranges)
	generateCommitmentFile(t, agg.Dirs(), ranges)
	require.NoError(t, dir.RemoveFile(filepath.Join(agg.Dirs().SnapDomain, "v1.0-accounts.1-2.bt")))
	require.NoError(t, agg.OpenFolder(db))

	requireVisibleEnd(t, agg, alignStepSize)
}

// a values list whose first file is unreadable lowers every list to that file's start
func TestVisibleFilesAligned_LeadingGapInOneListLowersEveryList(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg := testDbAndAggregatorv3(t, alignStepSize)

	ranges := []testFileRange{{0, 1}, {1, 2}, {2, 3}}
	generateStateFiles(t, agg.Dirs(), ranges)
	generateStandaloneIIFiles(t, agg.Dirs(), ranges)
	generateCommitmentFile(t, agg.Dirs(), ranges)
	require.NoError(t, dir.RemoveFile(filepath.Join(agg.Dirs().SnapDomain, "v1.0-accounts.0-1.bt")))
	require.NoError(t, agg.OpenFolder(db))

	requireVisibleEnd(t, agg, 0)
}

// domains visible past the log/trace indexes answer eth_getLogs from blocks no index saw
func TestVisibleFilesAligned_LaggingStandaloneIdxClampsEveryone(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg := testDbAndAggregatorv3(t, alignStepSize)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	for _, name := range []kv.InvertedIdx{kv.LogTopicIdx, kv.TracesFromIdx, kv.TracesToIdx} {
		generateStandaloneIIFile(t, name, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	}
	generateStandaloneIIFile(t, kv.LogAddrIdx, agg.Dirs(), []testFileRange{{0, 1}})
	require.NoError(t, agg.OpenFolder(db))

	requireVisibleEnd(t, agg, alignStepSize)
}

// no files at all = nothing to be misaligned with, so the domains stay visible
func TestVisibleFilesAligned_EntityWithoutFilesDoesNotClamp(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg := testDbAndAggregatorv3(t, alignStepSize)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	require.NoError(t, agg.OpenFolder(db))

	at := agg.BeginFilesRo()
	defer at.Close()
	for _, d := range kv.StateDomains {
		require.EqualValues(t, 2*alignStepSize, at.d[d].files.EndTxNum(), "domain %s", d)
	}
}

// stage_custom_trace re-executes blocks to rebuild receipts, and reads the state while
// doing it
func TestUnalign_KeepsStateVisibleWhileReceiptsLag(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg := testDbAndAggregatorv3(t, alignStepSize)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateStandaloneIIFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateDomainFiles(t, "receipt", agg.Dirs(), []testFileRange{{0, 1}})
	require.NoError(t, agg.OpenFolder(db))

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
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg := testDbAndAggregatorv3(t, alignStepSize)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateStandaloneIIFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}})
	require.NoError(t, agg.OpenFolder(db))

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
func TestVisibilityLowering_ForbiddenAggregatorPanicsOnLoweringOnly(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg := testDbAndAggregatorv3(t, alignStepSize)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateStandaloneIIFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateDomainFiles(t, "receipt", agg.Dirs(), []testFileRange{{0, 1}})
	require.NoError(t, agg.OpenFolder(db))

	agg.ForbidVisibilityLowering()
	realign := agg.Unalign(kv.ReceiptDomain) // raises the ceiling: allowed
	require.Panics(t, func() { realign() }, "realigning a still-lagging receipt lowers the state domains' ends")
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
		crafted.d[dom] = newDomainVisible(dom, files[:len(files)-1], true)
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
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg := testDbAndAggregatorv3(t, alignStepSize)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateStandaloneIIFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	require.NoError(t, agg.OpenFolder(db))

	craftedClampedVisible(t, agg)

	at := agg.BeginFilesRo()
	defer at.Close()
	tx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	_, ok := at.DomainVisibleEnd(kv.AccountsDomain, tx)
	require.False(t, ok, "a dependency-clamped values view has no exact frontier")
}

// The forbid assert must also watch the history-II ends: they are the base of
// what DomainVisibleEnd reports, and with values dependency-clamped below the
// ceiling they can lower while every values end stays put.
func TestVisibilityLowering_GuardsHistoryIIEnd(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg := testDbAndAggregatorv3(t, alignStepSize)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateStandaloneIIFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	require.NoError(t, agg.OpenFolder(db))

	craftedClampedVisible(t, agg)
	agg.ForbidVisibilityLowering()

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

func TestVisibleFilesAligned_HistoryGapLowersEveryList(t *testing.T) {
	for _, path := range []string{"accessor/v2.0-accounts.1-2.vi", "accessor/v1.0-accounts.1-2.efi"} {
		t.Run(path, func(t *testing.T) {
			t.Cleanup(statecfg.SetCommitmentV3(false))
			db, agg := testDbAndAggregatorv3(t, alignStepSize)
			ranges := []testFileRange{{0, 1}, {1, 2}, {2, 3}}
			generateStateFiles(t, agg.Dirs(), ranges)
			generateCommitmentFile(t, agg.Dirs(), ranges)
			generateStandaloneIIFiles(t, agg.Dirs(), ranges)
			require.NoError(t, dir.RemoveFile(filepath.Join(agg.Dirs().Snap, filepath.FromSlash(path))))
			require.NoError(t, agg.OpenFolder(db))

			requireVisibleEnd(t, agg, alignStepSize)
		})
	}
}

func TestVisibleFilesAligned_CutThroughAMergedFileRepeats(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg := testDbAndAggregatorv3(t, alignStepSize)

	merged := []testFileRange{{0, 2}, {2, 3}}
	generateStateFiles(t, agg.Dirs(), merged)
	generateCommitmentFile(t, agg.Dirs(), merged)
	generateStandaloneIIFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}, {2, 3}})
	require.NoError(t, dir.RemoveFile(filepath.Join(agg.Dirs().SnapAccessors, "v1.0-tracesto.1-2.efi")))
	require.NoError(t, agg.OpenFolder(db))

	requireVisibleEnd(t, agg, 0)
}

func TestOpenFolderLetsMissedAccessorsBuildBeforeGapCheck(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg := testDbAndAggregatorv3(t, alignStepSize)

	ranges := []testFileRange{{0, 1}, {1, 2}}
	generateStateFiles(t, agg.Dirs(), ranges)
	generateCommitmentFile(t, agg.Dirs(), ranges)
	generateStandaloneIIFiles(t, agg.Dirs(), ranges)
	require.NoError(t, dir.RemoveFile(filepath.Join(agg.Dirs().SnapDomain, "v1.0-accounts.1-2.bt")))
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return SavePruneValProgress(tx, agg.d[kv.AccountsDomain].ValuesTable, &prune.Stat{TxTo: 2 * alignStepSize})
	}))

	agg.buildsMissedAccessorsAfterOpen = true
	require.NoError(t, agg.OpenFolder(db))
	require.NoError(t, agg.BuildMissedAccessors(t.Context(), db, 1))
	requireVisibleEnd(t, agg, 2*alignStepSize)
}

func TestHasAccessorsToBuildSkipsCoveredFiles(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	_, agg := testDbAndAggregatorv3(t, alignStepSize)

	ranges := []testFileRange{{0, 1}, {1, 2}, {0, 2}}
	generateStateFiles(t, agg.Dirs(), ranges)
	generateCommitmentFile(t, agg.Dirs(), ranges)
	generateStandaloneIIFiles(t, agg.Dirs(), ranges)
	require.NoError(t, dir.RemoveFile(filepath.Join(agg.Dirs().SnapDomain, "v1.0-accounts.0-2.bt")))
	require.NoError(t, agg.OpenFolder(nil))

	require.False(t, agg.hasAccessorsToBuild(), "startup builds no accessor for a file covered by indexed files")
}

// exitedInChildProcess re-runs the test in a child process, because the gap check exits the process.
// The child must also print each of wantOutput before the gap check exits it.
func exitedInChildProcess(t *testing.T, wantOutput ...string) bool {
	t.Helper()
	if os.Getenv("ERIGON_GAP_CHECK_CHILD") != "" {
		return false
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$")
	tmp := t.TempDir() // the child exits before its own cleanup
	cmd.Env = append(os.Environ(), "ERIGON_GAP_CHECK_CHILD=1", "TMPDIR="+tmp, "TMP="+tmp, "TEMP="+tmp)
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "the gap check must exit the process:\n%s", out)
	require.Equal(t, 1, exitErr.ExitCode(), "the gap check exits with code 1:\n%s", out)
	require.NotContains(t, string(out), "--- FAIL", "the child must exit in the gap check, not fail a check")
	for _, want := range append(wantOutput, "gap between snapshot files and DB") {
		require.Contains(t, string(out), want)
	}
	return true
}

func TestOpenFolderChecksGapWhenNoAccessorBuildFollows(t *testing.T) {
	if exitedInChildProcess(t) {
		return
	}
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg := testDbAndAggregatorv3(t, alignStepSize)
	ranges := []testFileRange{{0, 1}, {1, 2}}
	generateStateFiles(t, agg.Dirs(), ranges)
	generateCommitmentFile(t, agg.Dirs(), ranges)
	generateStandaloneIIFiles(t, agg.Dirs(), ranges)
	require.NoError(t, dir.RemoveFile(filepath.Join(agg.Dirs().SnapDomain, "v1.0-accounts.1-2.bt")))
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return SavePruneValProgress(tx, agg.d[kv.AccountsDomain].ValuesTable, &prune.Stat{TxTo: 2 * alignStepSize})
	}))
	require.NoError(t, agg.OpenFolder(db))
}

func TestBuildMissedAccessorsChecksGapWhenAnAccessorCannotBeBuilt(t *testing.T) {
	const opened = "OpenFolder deferred the gap check"
	if exitedInChildProcess(t, opened) {
		return
	}
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg := testDbAndAggregatorv3(t, alignStepSize)
	ranges := []testFileRange{{0, 1}, {1, 2}, {2, 3}}
	generateStateFiles(t, agg.Dirs(), ranges)
	generateCommitmentFile(t, agg.Dirs(), ranges)
	for _, path := range []string{"idx/v1.0-accounts.1-2.ef", "accessor/v2.0-accounts.1-2.vi"} {
		require.NoError(t, dir.RemoveFile(filepath.Join(agg.Dirs().Snap, filepath.FromSlash(path))))
	}
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return SavePruneValProgress(tx, agg.d[kv.AccountsDomain].History.ValuesTable, &prune.Stat{TxTo: 3 * alignStepSize})
	}))
	agg.buildsMissedAccessorsAfterOpen = true
	require.NoError(t, agg.OpenFolder(db))
	fmt.Println(opened)
	require.NoError(t, agg.BuildMissedAccessors(t.Context(), db, 1))
}

// files left by a history that is switched off do not cut the lists that are read
func TestVisibleFilesAligned_DisabledHistoryLeftoversDoNotCut(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg := testDbAndAggregatorv3(t, alignStepSize)

	ranges := []testFileRange{{0, 1}, {1, 2}}
	generateStateFiles(t, agg.Dirs(), ranges)
	generateCommitmentFile(t, agg.Dirs(), ranges)
	generateStandaloneIIFiles(t, agg.Dirs(), ranges)
	generateCommitmentHistoryAndIndexFiles(t, agg.Dirs(), []testFileRange{{0, 1}})
	require.NoError(t, agg.OpenFolder(db))

	requireVisibleEnd(t, agg, 2*alignStepSize)
}

func TestUnalignIdx_KeepsStateVisibleWhileAnIndexLags(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg := testDbAndAggregatorv3(t, alignStepSize)

	ranges := []testFileRange{{0, 1}, {1, 2}}
	generateStateFiles(t, agg.Dirs(), ranges)
	generateCommitmentFile(t, agg.Dirs(), ranges)
	for _, name := range []kv.InvertedIdx{kv.LogTopicIdx, kv.TracesFromIdx, kv.TracesToIdx} {
		generateStandaloneIIFile(t, name, agg.Dirs(), ranges)
	}
	generateStandaloneIIFile(t, kv.LogAddrIdx, agg.Dirs(), []testFileRange{{0, 1}})
	require.NoError(t, agg.OpenFolder(db))

	requireVisibleEnd(t, agg, alignStepSize)

	realign := agg.UnalignIdx(kv.LogAddrIdx)
	at := agg.BeginFilesRo()
	for _, d := range kv.StateDomains {
		require.EqualValues(t, 2*alignStepSize, at.d[d].files.EndTxNum(), "domain %s", d)
	}
	at.Close()

	realign()
	requireVisibleEnd(t, agg, alignStepSize)
}

func TestVisibleFilesAligned_HistoryGapLowersEveryListWithoutDependencies(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	db, agg := testDbAndAggregatorv3(t, alignStepSize)
	ranges := []testFileRange{{0, 1}, {1, 2}, {2, 3}}
	generateStateFiles(t, agg.Dirs(), ranges)
	generateCommitmentFile(t, agg.Dirs(), ranges)
	generateStandaloneIIFiles(t, agg.Dirs(), ranges)
	require.NoError(t, dir.RemoveFile(filepath.Join(agg.Dirs().SnapAccessors, "v2.0-accounts.1-2.vi")))
	agg.DisableAllDependencies()
	require.NoError(t, agg.OpenFolder(db))

	requireVisibleEnd(t, agg, alignStepSize)
}
