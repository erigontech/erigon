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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
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
func TestVisibilityLowering_ForbiddenAggregatorPanicsOnLoweringOnly(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, alignStepSize)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateStandaloneIIFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateDomainFiles(t, "receipt", agg.Dirs(), []testFileRange{{0, 1}})
	require.NoError(t, agg.OpenFolder())

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
		crafted.d[dom] = newDomainVisible(dom, files[:len(files)-1])
	}
	v.next = crafted
	agg.visible.Store(crafted)
}

// A view's frontier must not overstate what its values view can serve: with
// domain values clamped below the history-II end (dependency checker), reads
// above the values end fall back to older file values, so DomainVisibleEnd
// must report the values end, not the II end.
func TestDomainVisibleEnd_ClampedToValuesCoverage(t *testing.T) {
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

	end, ok := at.DomainVisibleEnd(kv.AccountsDomain, tx)
	require.True(t, ok)
	require.Equal(t, uint64(1*alignStepSize), end,
		"the frontier must not overstate the values coverage")
}

// The forbid assert must also watch the history-II ends: they are the base of
// what DomainVisibleEnd reports, and with values dependency-clamped below the
// ceiling they can lower while every values end stays put.
func TestVisibilityLowering_GuardsHistoryIIEnd(t *testing.T) {
	t.Parallel()
	_, agg := testDbAndAggregatorv3(t, alignStepSize)

	generateStateFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	generateStandaloneIIFiles(t, agg.Dirs(), []testFileRange{{0, 1}, {1, 2}})
	require.NoError(t, agg.OpenFolder())

	craftedClampedVisible(t, agg)
	agg.ForbidVisibilityLowering()

	for _, pattern := range []string{
		filepath.Join(agg.Dirs().SnapIdx, "*accounts.1-2.ef"),
		filepath.Join(agg.Dirs().SnapAccessors, "*accounts.1-2.efi"),
	} {
		matches, err := filepath.Glob(pattern)
		require.NoError(t, err)
		require.NotEmpty(t, matches, pattern)
		for _, m := range matches {
			require.NoError(t, dir.RemoveFile(m))
		}
	}

	require.Panics(t, func() { _ = agg.ReloadFiles() },
		"lowering a history-II end while values ends stay put must trip the forbid assert")
}
