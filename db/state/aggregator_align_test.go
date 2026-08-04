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

	"github.com/erigontech/erigon/common/dbg"

	"github.com/stretchr/testify/require"

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

// Fill admission relies on view frontiers never decreasing, which holds only
// while no process both fills a StateCache and lowers visible file ends —
// Unalign must refuse to run beside a wired cache.
func TestUnalign_PanicsWithWiredStateCache(t *testing.T) {
	dbg.SetStateCacheWired(true)
	t.Cleanup(func() { dbg.SetStateCacheWired(false) })

	_, agg := testDbAndAggregatorv3(t, alignStepSize)
	require.Panics(t, func() { agg.Unalign(kv.ReceiptDomain) })
	require.Panics(t, func() { agg.UnalignIdx(kv.LogAddrIdx) })
}
