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

package state_test

import (
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func TestConvertCommitmentFiles_SqueezedLegacyToV3Folds(t *testing.T) {
	t.Cleanup(statecfg.SetCommitmentV3(false))
	legacySchema := statecfg.Schema
	db, agg, roots := testDbAggregatorWithCommitmentHistory(t, 10, 32)
	require.NoError(t, agg.BuildFiles2(t.Context(), db, 31, 32, unboundedFinalityCtx, false))
	agg.WaitForFiles()
	rootTxNums := slices.Sorted(maps.Keys(roots))
	legacyFiles, err := filepath.Glob(filepath.Join(agg.Dirs().SnapDomain, "*-commitment.*.kv"))
	require.NoError(t, err)
	require.NotEmpty(t, legacyFiles)
	var latestFileTo uint64
	for _, path := range legacyFiles {
		_, to := commitmentFileRange(t, path)
		latestFileTo = max(latestFileTo, to)
	}
	latestCoveredTxNum := latestFileTo*10 - 1
	require.Equal(t, rootTxNums[len(rootTxNums)-1], latestCoveredTxNum)
	latestIndex := slices.IndexFunc(rootTxNums, func(txNum uint64) bool { return txNum > latestCoveredTxNum })
	if latestIndex < 0 {
		latestIndex = len(rootTxNums)
	}
	latestIndex--
	require.GreaterOrEqual(t, latestIndex, 1)
	latestTxNum := rootTxNums[latestIndex]
	earlierTxNum := rootTxNums[latestIndex-1]
	runOrchestrator(t, db, state.ConvertOpts{TargetSqueeze: true})
	domainFiles, err := filepath.Glob(filepath.Join(agg.Dirs().SnapDomain, "*-commitment.*.kv"))
	require.NoError(t, err)
	require.NotEmpty(t, domainFiles)
	squeezed := 0
	for _, path := range domainFiles {
		from, to := commitmentFileRange(t, path)
		prefix := "v2.1-"
		if to-from < 2 {
			prefix = "v2.2-"
		}
		require.Truef(t, strings.HasPrefix(filepath.Base(path), prefix), "%s is not %s", filepath.Base(path), prefix)
		if prefix == "v2.1-" {
			squeezed++
		}
	}
	require.Positive(t, squeezed)
	require.NoError(t, dir.RemoveAll(filepath.Join(agg.Dirs().Snap, "backup")))

	db, agg = reopenForConvert(t, db, agg, &legacySchema)
	oldBatch := state.SetCommitmentV3BatchForTest(16)
	t.Cleanup(func() { state.SetCommitmentV3BatchForTest(oldBatch) })
	runOrchestrator(t, db, state.ConvertOpts{TargetV3: true})

	statecfg.Schema = legacySchema
	t.Cleanup(statecfg.SetCommitmentV3(true))
	statecfg.EnableHistoricalCommitment()
	db, agg = reopenAggregator(t, db, agg, 10)
	require.NoError(t, agg.OpenFolder(db))
	for _, txNum := range []uint64{latestTxNum, earlierTxNum} {
		check, err := state.FoldCommitmentV3(t.Context(), agg, txNum+1)
		require.NoErrorf(t, err, "records as of txNum %d", txNum+1)
		require.Equalf(t, roots[txNum], check.Root, "root as of txNum %d", txNum+1)
	}
}
