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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
)

func TestStepAlignedEndRefusesListStartingOffBoundary(t *testing.T) {
	t.Parallel()
	db, agg := testDbAndAggregatorv3(t, 10)
	dirs := agg.Dirs()
	generateAccountsFile(t, dirs, []testFileRange{{0, 20}})
	generateStorageFile(t, dirs, []testFileRange{{0, 20}})
	generateCommitmentFile(t, dirs, []testFileRange{{0, 20}})
	generateCodeFile(t, dirs, []testFileRange{{4, 20}})
	require.NoError(t, agg.OpenFolder(db))
	at := agg.BeginFilesRo()
	require.Equal(t, uint64(40), at.d[kv.CodeDomain].files.StartTxNum())
	at.Close()
	_, err := agg.StepAlignedEnd(100)
	require.ErrorContains(t, err, "starts at txNum 40")
}

func TestStepAlignedEndRefusesHiddenFiles(t *testing.T) {
	t.Parallel()
	db, agg := testDbAndAggregatorv3(t, 10)
	dirs := agg.Dirs()
	split := []testFileRange{{0, 4}, {4, 6}, {6, 8}, {8, 12}}
	generateAccountsFile(t, dirs, split)
	generateStorageFile(t, dirs, split)
	generatePlainCommitmentFile(t, dirs, []testFileRange{{0, 4}, {4, 8}, {8, 12}})
	require.NoError(t, agg.OpenFolder(db))
	_, err := agg.StepAlignedEnd(30)
	require.ErrorContains(t, err, "no visible file for txNum 40-80")
}

func TestAlignFilesToStepSizeRefusesCommitmentRangeMismatch(t *testing.T) {
	t.Parallel()
	db, agg := testDbAndAggregatorv3(t, 10)
	dirs := agg.Dirs()
	split := []testFileRange{{0, 4}, {4, 6}, {6, 8}, {8, 12}}
	generateAccountsFile(t, dirs, split)
	generateStorageFile(t, dirs, split)
	generatePlainCommitmentFile(t, dirs, []testFileRange{{0, 4}, {4, 8}, {8, 12}})
	agg.DisableInterDomainDependencies()
	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, false)
	require.NoError(t, agg.OpenFolder(db))
	_, err := agg.AlignFilesToStepSize(t.Context(), 30)
	require.ErrorContains(t, err, "commitment merge 0-120 needs accounts merged over the same range")
}

func generatePlainCommitmentFile(t *testing.T, dirs datadir.Dirs, ranges []testFileRange) {
	t.Helper()
	generateCommitmentFile(t, dirs, ranges)
	paths, err := filepath.Glob(filepath.Join(dirs.SnapDomain, "v1.0-commitment.*"))
	require.NoError(t, err)
	require.NotEmpty(t, paths)
	for _, path := range paths {
		require.NoError(t, os.Rename(path, filepath.Join(dirs.SnapDomain, "v2.2-"+strings.TrimPrefix(filepath.Base(path), "v1.0-"))))
	}
}
