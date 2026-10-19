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
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/mvcc"
)

func testAggWithThreeSteps(t *testing.T) (kv.RwDB, *Aggregator) {
	t.Helper()
	return testAggWithRanges(t, []testFileRange{{0, 1}, {1, 2}, {2, 3}})
}

func testAggWithRanges(t *testing.T, ranges []testFileRange) (kv.RwDB, *Aggregator) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("inspects /proc/self/maps; Linux only")
	}
	db, agg := testDbAndAggregatorv3(t, 10)
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

func retireDirtyCommitmentFile(t *testing.T, agg *Aggregator, reason mvcc.RetireReason, from, to kv.Step) {
	t.Helper()
	d := agg.d[kv.CommitmentDomain]
	agg.dirtyFilesLock.Lock()
	defer agg.dirtyFilesLock.Unlock()
	item, ok := d.dirtyFiles.Get(newFilesItem(uint64(from)*d.stepSize, uint64(to)*d.stepSize))
	require.True(t, ok)
	retire(reason, d.dirtyFiles, []*FilesItem{item}, d.FilenameBase, d.logger)
	agg.recalcVisibleFiles(retiredFiles{item})
}

func TestAggregatorDeletingDemotedFileRemovesColdTarget(t *testing.T) {
	t.Parallel()
	_, agg := testAggWithThreeSteps(t)
	ssdPath := domainFileBySuffix(t, agg.Dirs().SnapDomain, "commitment.1-2.kv")
	coldDir := t.TempDir()
	require.NoError(t, agg.DemoteFile(t.Context(), kv.CommitmentDomain, 1, 2, coldDir))

	retireDirtyCommitmentFile(t, agg, mvcc.RetireReasonMerged, 1, 2)

	_, err := os.Lstat(ssdPath)
	require.ErrorIs(t, err, fs.ErrNotExist)
	_, err = os.Lstat(filepath.Join(coldDir, filepath.Base(ssdPath)))
	require.ErrorIs(t, err, fs.ErrNotExist)
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

func isSymlink(t *testing.T, path string) bool {
	t.Helper()
	fi, err := os.Lstat(path)
	require.NoError(t, err)
	return fi.Mode()&os.ModeSymlink != 0
}

func waitDemotionIdle(t *testing.T, agg *Aggregator) {
	t.Helper()
	require.Eventually(t, func() bool { return !agg.demoting.Load() }, 10*time.Second, 5*time.Millisecond)
}

func TestAggregatorDemoteInBackgroundOldestFullyColdFirst(t *testing.T) {
	t.Parallel()
	_, agg := testAggWithRanges(t, []testFileRange{{0, 4}, {4, 5}, {5, 6}, {6, 8}})
	snapDomain := agg.Dirs().SnapDomain
	paths := []string{
		domainFileBySuffix(t, snapDomain, "commitment.0-4.kv"),
		domainFileBySuffix(t, snapDomain, "commitment.4-5.kv"),
		domainFileBySuffix(t, snapDomain, "commitment.5-6.kv"),
	}
	straddling := domainFileBySuffix(t, snapDomain, "commitment.6-8.kv")
	coldDir := t.TempDir()
	boundary := 7 * agg.StepSize()

	for i := range paths {
		require.True(t, agg.DemoteInBackground(boundary, coldDir))
		waitDemotionIdle(t, agg)
		for j, p := range paths {
			require.Equal(t, j <= i, isSymlink(t, p), "after run %d: %s", i, p)
		}
	}
	require.False(t, agg.DemoteInBackground(boundary, coldDir))
	require.False(t, isSymlink(t, straddling))
}

func TestAggregatorDemoteInBackgroundIsSingleFlight(t *testing.T) {
	t.Parallel()
	_, agg := testAggWithRanges(t, []testFileRange{{0, 1}, {1, 2}, {2, 3}})
	snapDomain := agg.Dirs().SnapDomain
	coldDir := t.TempDir()
	entered, release := make(chan struct{}), make(chan struct{})
	agg.onDemoteCopy = func(context.Context) {
		entered <- struct{}{}
		<-release
	}

	require.True(t, agg.DemoteInBackground(2*agg.StepSize(), coldDir))
	<-entered
	require.False(t, agg.DemoteInBackground(2*agg.StepSize(), coldDir))
	close(release)
	waitDemotionIdle(t, agg)

	require.True(t, isSymlink(t, domainFileBySuffix(t, snapDomain, "commitment.0-1.kv")))
	require.False(t, isSymlink(t, domainFileBySuffix(t, snapDomain, "commitment.1-2.kv")))
}

func TestAggregatorCloseCancelsDemotionCopy(t *testing.T) {
	t.Parallel()
	_, agg := testAggWithRanges(t, []testFileRange{{0, 1}, {1, 2}})
	ssdPath := domainFileBySuffix(t, agg.Dirs().SnapDomain, "commitment.0-1.kv")
	coldDir := t.TempDir()
	entered := make(chan struct{})
	agg.onDemoteCopy = func(ctx context.Context) {
		close(entered)
		<-ctx.Done()
	}

	require.True(t, agg.DemoteInBackground(agg.StepSize(), coldDir))
	<-entered
	agg.Close()

	require.False(t, agg.demoting.Load())
	require.False(t, isSymlink(t, ssdPath))
	coldFiles, err := os.ReadDir(coldDir)
	require.NoError(t, err)
	require.Empty(t, coldFiles)
}

func TestAggregatorDemoteFileMetrics(t *testing.T) {
	t.Parallel()
	_, agg := testAggWithThreeSteps(t)
	ssdPath := domainFileBySuffix(t, agg.Dirs().SnapDomain, "commitment.1-2.kv")
	fi, err := os.Stat(ssdPath)
	require.NoError(t, err)
	demotions := mxDemotions.WithLabelValues("commitment")
	demotedBytes := mxDemotedBytes.WithLabelValues("commitment")
	demotionsBefore, bytesBefore := demotions.GetValue(), demotedBytes.GetValue()

	require.NoError(t, agg.DemoteFile(t.Context(), kv.CommitmentDomain, 1, 2, t.TempDir()))

	require.GreaterOrEqual(t, demotions.GetValue()-demotionsBefore, 1.0)
	require.GreaterOrEqual(t, demotedBytes.GetValue()-bytesBefore, float64(fi.Size()))
	require.Positive(t, mxDemotionSeconds.GetValue())
	require.Positive(t, mxDemotionCopyBytesPerSec.GetValue())
}

func TestAggregatorOpenFolderFailsOnDanglingSymlink(t *testing.T) {
	t.Parallel()
	db, agg := testAggWithThreeSteps(t)
	ssdPath := domainFileBySuffix(t, agg.Dirs().SnapDomain, "commitment.1-2.kv")
	coldPath := filepath.Join(t.TempDir(), filepath.Base(ssdPath))
	require.NoError(t, dir.RemoveFile(ssdPath))
	require.NoError(t, os.Symlink(coldPath, ssdPath))

	err := agg.OpenFolder(db)
	require.ErrorContains(t, err, "dangling symlink")
	require.ErrorContains(t, err, coldPath)
}

func TestAggregatorRecoverDemotionRemovesOwnLeftoversOnly(t *testing.T) {
	t.Parallel()
	_, agg := testAggWithThreeSteps(t)
	snapDomain := agg.Dirs().SnapDomain
	ssdPath := domainFileBySuffix(t, snapDomain, "commitment.1-2.kv")
	name := filepath.Base(ssdPath)
	coldDir := t.TempDir()
	ownTmp := filepath.Join(coldDir, name+".tmp")
	otherTmp := filepath.Join(coldDir, "v1.0-commitment.7-8.kv.tmp")
	linkTmp := ssdPath + ".demote.tmp"
	for _, p := range []string{ownTmp, otherTmp} {
		require.NoError(t, os.WriteFile(p, []byte("partial"), 0o644))
	}
	require.NoError(t, os.Symlink(filepath.Join(coldDir, name), linkTmp))

	require.NoError(t, recoverDemotion(snapDomain, coldDir, agg.logger))

	for _, p := range []string{ownTmp, linkTmp} {
		_, err := os.Lstat(p)
		require.ErrorIs(t, err, fs.ErrNotExist, p)
	}
	_, err := os.Stat(otherTmp)
	require.NoError(t, err)
	require.False(t, isSymlink(t, ssdPath))
}

func TestAggregatorOpenFolderIgnoresDanglingDemoteTmpLink(t *testing.T) {
	t.Parallel()
	db, agg := testAggWithThreeSteps(t)
	ssdPath := domainFileBySuffix(t, agg.Dirs().SnapDomain, "commitment.1-2.kv")
	require.NoError(t, os.Symlink(filepath.Join(t.TempDir(), "gone.kv"), ssdPath+".demote.tmp"))
	require.NoError(t, agg.OpenFolder(db))
}

func TestAggregatorDemoteInBackgroundSkipsOnlyColdResidentFiles(t *testing.T) {
	t.Parallel()
	db, agg := testAggWithRanges(t, []testFileRange{{0, 1}, {1, 2}, {2, 3}})
	snapDomain := agg.Dirs().SnapDomain
	onOtherSSDDir := domainFileBySuffix(t, snapDomain, "commitment.0-1.kv")
	alreadyCold := domainFileBySuffix(t, snapDomain, "commitment.1-2.kv")
	notLinked := domainFileBySuffix(t, snapDomain, "commitment.2-3.kv")
	coldDir := t.TempDir()
	moveToColdByHand(t, onOtherSSDDir, t.TempDir())
	moveToColdByHand(t, alreadyCold, coldDir)
	require.NoError(t, agg.OpenFolder(db))

	require.True(t, agg.DemoteInBackground(3*agg.StepSize(), coldDir))
	waitDemotionIdle(t, agg)
	target, err := os.Readlink(onOtherSSDDir)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(coldDir, filepath.Base(onOtherSSDDir)), target)

	require.True(t, agg.DemoteInBackground(3*agg.StepSize(), coldDir))
	waitDemotionIdle(t, agg)
	require.True(t, isSymlink(t, notLinked))
	require.False(t, agg.DemoteInBackground(3*agg.StepSize(), coldDir))
}
