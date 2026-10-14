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
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

func testAggWithThreeSteps(t *testing.T) (kv.RwDB, *Aggregator) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("inspects /proc/self/maps; Linux only")
	}
	db, agg := testDbAndAggregatorv3(t, 10)
	ranges := []testFileRange{{0, 1}, {1, 2}, {2, 3}}
	dirs := agg.Dirs()
	generateAccountsFile(t, dirs, ranges)
	generateStorageFile(t, dirs, ranges)
	generateCodeFile(t, dirs, ranges)
	generateCommitmentFile(t, dirs, ranges)
	require.NoError(t, agg.OpenFolder(db))
	return db, agg
}

func moveToColdByHand(t *testing.T, ssdPath, coldDir string) string {
	t.Helper()
	coldPath := filepath.Join(coldDir, filepath.Base(ssdPath))
	src, err := os.Open(ssdPath)
	require.NoError(t, err)
	defer src.Close()
	dst, err := os.Create(coldPath)
	require.NoError(t, err)
	_, err = io.Copy(dst, src)
	require.NoError(t, err)
	require.NoError(t, dst.Close())
	require.NoError(t, os.Symlink(coldPath, ssdPath+".tmp-link"))
	require.NoError(t, os.Rename(ssdPath+".tmp-link", ssdPath))
	return coldPath
}

func procSelfMapsHas(t *testing.T, s string) bool {
	t.Helper()
	maps, err := os.ReadFile("/proc/self/maps")
	require.NoError(t, err)
	return strings.Contains(string(maps), s)
}

func TestAggregatorOpenFolderAfterSymlinkSwapKeepsSSDInodeMapped(t *testing.T) {
	t.Parallel()
	db, agg := testAggWithThreeSteps(t)
	at := agg.BeginFilesRo()
	_, _, _, _, err := at.DebugGetLatestFromFiles(kv.CommitmentDomain, []byte("key"), 0)
	require.NoError(t, err)
	at.Close()

	ssdPath := domainFileBySuffix(t, agg.Dirs().SnapDomain, "commitment.1-2.kv")
	coldPath := moveToColdByHand(t, ssdPath, t.TempDir())
	require.NoError(t, agg.OpenFolder(db))

	require.True(t, procSelfMapsHas(t, ssdPath+" (deleted)"))
	require.False(t, procSelfMapsHas(t, coldPath))
}

func TestAggregatorDemoteFileSwapsToColdAndReleasesSSDInode(t *testing.T) {
	t.Parallel()
	_, agg := testAggWithThreeSteps(t)
	snapDomain := agg.Dirs().SnapDomain
	ssdPath := domainFileBySuffix(t, snapDomain, "commitment.1-2.kv")
	kviPath := strings.TrimSuffix(ssdPath, ".kv") + ".kvi"
	coldDir := t.TempDir()
	coldPath := filepath.Join(coldDir, filepath.Base(ssdPath))

	oldTx := agg.BeginFilesRo()
	defer oldTx.Close()
	require.NoError(t, agg.DemoteFile(t.Context(), kv.CommitmentDomain, 1, 2, coldDir))

	fi, err := os.Lstat(ssdPath)
	require.NoError(t, err)
	require.Equal(t, os.ModeSymlink, fi.Mode()&os.ModeSymlink)
	target, err := os.Readlink(ssdPath)
	require.NoError(t, err)
	require.Equal(t, coldPath, target)
	fi, err = os.Lstat(kviPath)
	require.NoError(t, err)
	require.True(t, fi.Mode().IsRegular())

	_, _, _, _, err = oldTx.DebugGetLatestFromFiles(kv.CommitmentDomain, []byte("key"), 0)
	require.NoError(t, err)
	newTx := agg.BeginFilesRo()
	_, _, _, _, err = newTx.DebugGetLatestFromFiles(kv.CommitmentDomain, []byte("key"), 0)
	require.NoError(t, err)
	newTx.Close()
	require.True(t, procSelfMapsHas(t, ssdPath+" (deleted)"))

	oldTx.Close()
	require.False(t, procSelfMapsHas(t, ssdPath+" (deleted)"))
	require.True(t, procSelfMapsHas(t, coldPath))
	require.False(t, procSelfFdHas(t, ssdPath+" (deleted)"))
	require.True(t, procSelfFdHas(t, coldPath))
}

func procSelfFdHas(t *testing.T, s string) bool {
	t.Helper()
	const fdDir = "/proc/self/fd"
	fds, err := os.ReadDir(fdDir)
	require.NoError(t, err)
	for _, fd := range fds {
		if target, err := os.Readlink(filepath.Join(fdDir, fd.Name())); err == nil && target == s {
			return true
		}
	}
	return false
}

func TestAggregatorReclaimAfterDemoteIsCloseOnly(t *testing.T) {
	t.Parallel()
	_, agg := testAggWithThreeSteps(t)
	ssdPath := domainFileBySuffix(t, agg.Dirs().SnapDomain, "commitment.1-2.kv")
	kviPath := strings.TrimSuffix(ssdPath, ".kv") + ".kvi"
	coldDir := t.TempDir()

	oldTx := agg.BeginFilesRo()
	require.NoError(t, agg.DemoteFile(t.Context(), kv.CommitmentDomain, 1, 2, coldDir))
	oldTx.Close()

	for _, p := range []string{ssdPath, kviPath, filepath.Join(coldDir, filepath.Base(ssdPath))} {
		_, err := os.Stat(p)
		require.NoError(t, err, p)
	}
}

func TestAggregatorDemoteFileSkipsCopyOfSameSizeColdTarget(t *testing.T) {
	t.Parallel()
	_, agg := testAggWithThreeSteps(t)
	ssdPath := domainFileBySuffix(t, agg.Dirs().SnapDomain, "commitment.1-2.kv")
	coldDir := t.TempDir()
	coldPath := filepath.Join(coldDir, filepath.Base(ssdPath))
	data, err := os.ReadFile(ssdPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(coldPath, data, 0o644))
	past := time.Now().Add(-time.Hour).Truncate(time.Second)
	require.NoError(t, os.Chtimes(coldPath, past, past))

	require.NoError(t, agg.DemoteFile(t.Context(), kv.CommitmentDomain, 1, 2, coldDir))

	fi, err := os.Stat(coldPath)
	require.NoError(t, err)
	require.Equal(t, past, fi.ModTime())
}

func TestAggregatorDemoteFileReplacesColdTargetOfOtherSize(t *testing.T) {
	t.Parallel()
	_, agg := testAggWithThreeSteps(t)
	ssdPath := domainFileBySuffix(t, agg.Dirs().SnapDomain, "commitment.1-2.kv")
	ssdInfo, err := os.Stat(ssdPath)
	require.NoError(t, err)
	coldDir := t.TempDir()
	coldPath := filepath.Join(coldDir, filepath.Base(ssdPath))
	require.NoError(t, os.WriteFile(coldPath, []byte("partial"), 0o644))

	require.NoError(t, agg.DemoteFile(t.Context(), kv.CommitmentDomain, 1, 2, coldDir))

	fi, err := os.Stat(coldPath)
	require.NoError(t, err)
	require.Equal(t, ssdInfo.Size(), fi.Size())
	_, err = os.Stat(coldPath + ".tmp")
	require.ErrorIs(t, err, fs.ErrNotExist)
}
