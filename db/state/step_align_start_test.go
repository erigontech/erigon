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
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
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
	_, _, err := agg.StepAlignedEnd(100)
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
	_, _, err := agg.StepAlignedEnd(30)
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
	_, _, err := agg.AlignFilesToStepSize(t.Context(), 30)
	require.ErrorContains(t, err, "accounts and commitment files differ below txNum 120")
}

func TestStepAlignedEndRefusesInterruptedCommitmentMerge(t *testing.T) {
	t.Parallel()
	db, agg := testDbAndAggregatorv3(t, 10)
	dirs := agg.Dirs()
	split := []testFileRange{{0, 4}, {4, 6}, {6, 7}, {7, 8}}
	generateAccountsFile(t, dirs, split)
	generateStorageFile(t, dirs, split)
	generatePlainCommitmentFile(t, dirs, append(split, testFileRange{4, 8}))
	require.NoError(t, agg.OpenFolder(db))
	_, _, err := agg.StepAlignedEnd(20)
	require.ErrorContains(t, err, "accounts and commitment files differ below txNum 80")
}

func TestAlignFilesToStepSizeMergesStoppedShadowToItsOwnEnd(t *testing.T) {
	db, agg := pbinDualAggregator(t)
	live := []testFileRange{{0, 1}, {1, 2}, {2, 3}, {3, 4}}
	for _, d := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain, kv.CodeDomain, kv.CommitmentDomain} {
		writeStepAlignTestFiles(t, agg, d, live)
	}
	writeStepAlignTestFiles(t, agg, kv.CommitmentBinDomain, []testFileRange{{0, 1}, {1, 2}, {2, 3}})
	require.NoError(t, agg.OpenFolder(db))
	require.NoError(t, agg.BuildMissedAccessors(t.Context(), db, 1))
	agg.StopCommitmentDomain(kv.CommitmentBinDomain)

	end, stoppedEnd, err := agg.AlignFilesToStepSize(t.Context(), 2*alignStepSize)
	require.NoError(t, err)
	require.Equal(t, uint64(4*alignStepSize), end)
	require.Equal(t, uint64(2*alignStepSize), stoppedEnd)
	require.True(t, agg.CommitmentDomainStopped(kv.CommitmentBinDomain))
	at := agg.BeginFilesRo()
	defer at.Close()
	require.Equal(t, [][2]uint64{{0, 20}, {20, 40}}, fileRangesUpTo(at.d[kv.CommitmentDomain].files, end))
	require.Equal(t, [][2]uint64{{0, 20}, {20, 40}}, fileRangesUpTo(at.d[kv.AccountsDomain].files, end))
	require.Equal(t, [][2]uint64{{0, 20}, {20, 30}}, fileRangesUpTo(at.d[kv.CommitmentBinDomain].files, end))
}

func writeStepAlignTestFiles(t *testing.T, agg *Aggregator, domain kv.Domain, ranges []testFileRange) {
	t.Helper()
	agg.d[domain].Compression = seg.CompressNone
	cell := append([]byte{18, 0}, bytes.Repeat([]byte{7}, 32)...)
	for _, r := range ranges {
		path := filepath.Join(agg.Dirs().SnapDomain, fmt.Sprintf("v1.0-%s.%d-%d.kv", domain, r.fromStep, r.toStep))
		c, err := seg.NewCompressor(t.Context(), t.Name(), path, agg.Dirs().Tmp, seg.DefaultCfg, log.LvlDebug, log.New())
		require.NoError(t, err)
		require.NoError(t, c.AddUncompressedWord([]byte{byte(r.fromStep)}))
		require.NoError(t, c.AddUncompressedWord(append(bytes.Clone(cell), cell...)))
		require.NoError(t, c.Compress())
		c.Close()
	}
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
