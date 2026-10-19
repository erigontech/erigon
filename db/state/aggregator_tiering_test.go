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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/version"
)

func TestCommitmentHotMaxMergeSteps(t *testing.T) {
	const stepSize = 10
	ranges := []testFileRange{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}, {6, 7}, {7, 8}}
	span := func(r MergeRange) [2]uint64 {
		if !r.needMerge {
			return [2]uint64{}
		}
		return [2]uint64{r.from / stepSize, r.to / stepSize}
	}
	for _, tc := range []struct {
		hotSteps   uint64
		commitment [2]uint64
	}{
		{hotSteps: 0, commitment: [2]uint64{0, 8}},
		{hotSteps: 1, commitment: [2]uint64{}},
	} {
		db, agg := testDbAndAggregatorv3(t, stepSize)
		agg.commitmentHotMaxMergeSteps = tc.hotSteps
		dirs := agg.Dirs()
		generateAccountsFile(t, dirs, ranges)
		generateStorageFile(t, dirs, ranges)
		generateCodeFile(t, dirs, ranges)
		generateCommitmentFile(t, dirs, ranges)
		require.NoError(t, agg.OpenFolder(db))

		aggTx := agg.BeginFilesRo()
		r := aggTx.findMergeRange(8*stepSize, stepSize, 32)
		aggTx.Close()

		require.Equal(t, [2]uint64{0, 8}, span(r.domain[kv.AccountsDomain].values), "hot=%d", tc.hotSteps)
		require.Equal(t, [2]uint64{0, 8}, span(r.domain[kv.StorageDomain].values), "hot=%d", tc.hotSteps)
		require.Equal(t, tc.commitment, span(r.domain[kv.CommitmentDomain].values), "hot=%d", tc.hotSteps)
	}
}

func TestCommitmentHotMaxMergeStepsRefusesReferencesFlag(t *testing.T) {
	for _, hotSteps := range []uint64{0, 1} {
		db, agg := testDbAndAggregatorv3(t, 10)
		agg.commitmentHotMaxMergeSteps = hotSteps
		agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, true)
		err := agg.OpenFolder(db)
		if hotSteps == 0 {
			require.NoError(t, err)
		} else {
			require.ErrorContains(t, err, "commitment references")
		}
	}
}

func TestCommitmentHotMaxMergeStepsRefusesReferencedFiles(t *testing.T) {
	db, agg := testDbAndAggregatorv3(t, 10)
	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, false)
	generateCommitmentFile(t, agg.Dirs(), []testFileRange{{0, 2}})
	require.NoError(t, agg.OpenFolder(db))
	agg.d[kv.CommitmentDomain].dirtyFiles.Scan(func(it *FilesItem) bool {
		it.version = version.V2_1
		return true
	})
	require.NoError(t, agg.checkCommitmentTiering())
	agg.commitmentHotMaxMergeSteps = 1
	require.ErrorContains(t, agg.checkCommitmentTiering(), "commitment references")
}

func TestCommitmentHotMaxMergeStepsRefusesReloadToReferences(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	writeRefsToml(t, dirs, true)
	agg := NewTest(dirs).MustOpen(t.Context())
	t.Cleanup(agg.Close)
	require.NoError(t, agg.ReloadErigonDBSettings(true))
	agg.commitmentHotMaxMergeSteps = 1
	require.ErrorContains(t, agg.ReloadErigonDBSettings(true), "commitment references")
}

func TestDemotionRefusesReferencesFlag(t *testing.T) {
	db, agg := testDbAndAggregatorv3(t, 10)
	agg.demoteColdDir = t.TempDir()
	agg.ForTestReferencesInCommitmentBranches(kv.CommitmentDomain, true)
	require.ErrorContains(t, agg.OpenFolder(db), "commitment references")
}

func TestOpenWithDemoteColdDirRecoversLeftovers(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	link := filepath.Join(dirs.SnapDomain, "v1.0-commitment.0-1.kv.demote.tmp")
	require.NoError(t, os.Symlink(filepath.Join(t.TempDir(), "v1.0-commitment.0-1.kv"), link))
	agg := NewTest(dirs).DemoteColdDir(t.TempDir()).MustOpen(t.Context())
	t.Cleanup(agg.Close)
	_, err := os.Lstat(link)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestOpenRefusesTieringWithReferences(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	writeRefsToml(t, dirs, true)
	settings, err := ResolveErigonDBSettings(dirs, log.New(), true)
	require.NoError(t, err)
	opts := NewTest(dirs).WithErigonDBSettings(settings)

	agg, err := opts.Open(t.Context())
	require.NoError(t, err)
	agg.Close()
	_, err = opts.DemoteColdDir(t.TempDir()).Open(t.Context())
	require.ErrorContains(t, err, "commitment references")
	_, err = opts.CommitmentHotMaxMergeSteps(1).Open(t.Context())
	require.ErrorContains(t, err, "commitment references")
}
